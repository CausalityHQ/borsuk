//! Generation-bound lookup from original source IDs to source-plane ordinals.

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::native_source_tier::{
    NativeSourceTier, SourceCandidate, SourceTierError, decoded_sha256,
};

const MAGIC: [u8; 8] = *b"BORSMAP1";
const VERSION: u32 = 1;
const HEADER_BYTES: u64 = 96;
const ENTRY_BYTES: u64 = 16;

/// An invalid map fails before returning a candidate roster.
#[derive(Debug, Error)]
pub enum SourceIdMapError {
    /// An underlying local filesystem operation failed.
    #[error("source ID map I/O failed: {0}")]
    Io(#[from] io::Error),
    /// A source-tier identity could not be decoded.
    #[error("source ID map identity failed: {0}")]
    Source(#[from] SourceTierError),
    /// The map's content or generation binding is invalid.
    #[error("source ID map contract differs: {0}")]
    Invalid(&'static str),
}

/// An authenticated, resident ID-to-source-ordinal table for one generation.
#[derive(Debug)]
pub struct NativeSourceIdMap {
    rows: u64,
    generation: u64,
    source_sha256: [u8; 32],
    source_artifact_sha256: [u8; 32],
    entries: Vec<(u64, u64)>,
}

fn expected_len(rows: u64) -> Result<u64, SourceIdMapError> {
    if rows == 0 {
        return Err(SourceIdMapError::Invalid("zero source rows"));
    }
    rows.checked_mul(ENTRY_BYTES)
        .and_then(|body| body.checked_add(HEADER_BYTES))
        .ok_or(SourceIdMapError::Invalid("map byte length overflow"))
}

fn header(
    rows: u64,
    generation: u64,
    source_sha256: [u8; 32],
    source_artifact_sha256: [u8; 32],
) -> Result<[u8; 96], SourceIdMapError> {
    expected_len(rows)?;
    if generation == 0 {
        return Err(SourceIdMapError::Invalid("zero generation"));
    }
    let mut bytes = [0_u8; 96];
    bytes[..8].copy_from_slice(&MAGIC);
    bytes[8..12].copy_from_slice(&VERSION.to_le_bytes());
    bytes[16..24].copy_from_slice(&rows.to_le_bytes());
    bytes[24..32].copy_from_slice(&generation.to_le_bytes());
    bytes[32..64].copy_from_slice(&source_sha256);
    bytes[64..96].copy_from_slice(&source_artifact_sha256);
    Ok(bytes)
}

/// Write a map from source IDs in source-plane ordinal order.
///
/// The caller must derive `source_ids` from the same authenticated input as
/// the source plane. Every resolved row is checked again by the source scorer.
pub fn write_source_id_map<I>(
    path: &Path,
    rows: u64,
    generation: u64,
    source_sha256: &str,
    source_artifact_sha256: &str,
    mut source_ids: I,
) -> Result<String, SourceIdMapError>
where
    I: Iterator<Item = u64>,
{
    let source = decoded_sha256(source_sha256)?;
    let artifact = decoded_sha256(source_artifact_sha256)?;
    let head = header(rows, generation, source, artifact)?;
    let capacity = usize::try_from(rows)
        .map_err(|_| SourceIdMapError::Invalid("map rows exceed address space"))?;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(capacity)
        .map_err(|_| SourceIdMapError::Invalid("map build allocation"))?;
    for ordinal in 0..rows {
        let id = source_ids
            .next()
            .ok_or(SourceIdMapError::Invalid("too few source IDs"))?;
        entries.push((id, ordinal));
    }
    if source_ids.next().is_some() {
        return Err(SourceIdMapError::Invalid("too many source IDs"));
    }
    entries.sort_unstable_by_key(|(id, _)| *id);
    if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(SourceIdMapError::Invalid("duplicate source ID"));
    }
    let parent = path
        .parent()
        .ok_or(SourceIdMapError::Invalid("map path has no parent"))?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    let mut digest = Sha256::new();
    {
        let mut writer = BufWriter::new(temporary.as_file_mut());
        writer.write_all(&head)?;
        digest.update(head);
        for (id, ordinal) in entries {
            let id_bytes = id.to_le_bytes();
            let ordinal_bytes = ordinal.to_le_bytes();
            writer.write_all(&id_bytes)?;
            writer.write_all(&ordinal_bytes)?;
            digest.update(id_bytes);
            digest.update(ordinal_bytes);
        }
        writer.flush()?;
    }
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| SourceIdMapError::Io(error.error))?;
    File::open(parent)?.sync_all()?;
    Ok(format!("{:x}", digest.finalize()))
}

impl NativeSourceIdMap {
    /// Whether this authenticated map belongs to the opened source plane.
    #[must_use]
    pub fn binds_to(&self, source: &NativeSourceTier) -> bool {
        source.rows() == self.rows
            && source.generation() == self.generation
            && source.source_sha256() == self.source_sha256
            && source.artifact_sha256() == self.source_artifact_sha256
    }

    /// Authenticate the entire immutable map before retaining its entries.
    pub fn open_authenticated(
        path: &Path,
        artifact_sha256: &str,
        source_sha256: &str,
        source_artifact_sha256: &str,
        rows: u64,
        generation: u64,
    ) -> Result<Self, SourceIdMapError> {
        let expected_artifact = decoded_sha256(artifact_sha256)?;
        let source = decoded_sha256(source_sha256)?;
        let source_artifact = decoded_sha256(source_artifact_sha256)?;
        let head = header(rows, generation, source, source_artifact)?;
        let file = File::open(path)?;
        if file.metadata()?.len() != expected_len(rows)? {
            return Err(SourceIdMapError::Invalid("map byte length"));
        }
        let capacity = usize::try_from(rows)
            .map_err(|_| SourceIdMapError::Invalid("map rows exceed address space"))?;
        let bit_bytes = rows
            .div_ceil(8)
            .try_into()
            .map_err(|_| SourceIdMapError::Invalid("map bitset exceeds address space"))?;
        let mut visited = Vec::new();
        visited
            .try_reserve_exact(bit_bytes)
            .map_err(|_| SourceIdMapError::Invalid("map bitset allocation"))?;
        visited.resize(bit_bytes, 0_u8);
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(capacity)
            .map_err(|_| SourceIdMapError::Invalid("map resident allocation"))?;
        let mut reader = BufReader::new(file);
        let mut actual_head = [0_u8; 96];
        reader.read_exact(&mut actual_head)?;
        if actual_head != head {
            return Err(SourceIdMapError::Invalid("map format or generation"));
        }
        let mut digest = Sha256::new();
        digest.update(actual_head);
        let mut previous_id = None;
        for _ in 0..rows {
            let mut raw = [0_u8; 16];
            reader.read_exact(&mut raw)?;
            digest.update(raw);
            let id = u64::from_le_bytes(raw[..8].try_into().unwrap());
            let ordinal = u64::from_le_bytes(raw[8..].try_into().unwrap());
            if previous_id.is_some_and(|previous| id <= previous) || ordinal >= rows {
                return Err(SourceIdMapError::Invalid("map ID order or ordinal"));
            }
            let byte = usize::try_from(ordinal / 8)
                .map_err(|_| SourceIdMapError::Invalid("map ordinal exceeds address space"))?;
            let mask = 1_u8 << (ordinal % 8);
            if visited[byte] & mask != 0 {
                return Err(SourceIdMapError::Invalid("duplicate source ordinal"));
            }
            visited[byte] |= mask;
            entries.push((id, ordinal));
            previous_id = Some(id);
        }
        let computed: [u8; 32] = digest.finalize().into();
        if computed != expected_artifact {
            return Err(SourceIdMapError::Invalid("map artifact SHA-256"));
        }
        Ok(Self {
            rows,
            generation,
            source_sha256: source,
            source_artifact_sha256: source_artifact,
            entries,
        })
    }

    /// Resident pair payload, excluding Vec allocation overhead.
    #[must_use]
    pub fn resident_entry_bytes(&self) -> usize {
        self.entries.len() * std::mem::size_of::<(u64, u64)>()
    }

    /// Deduplicate two fixed candidate-ID rosters and resolve every ID.
    pub fn resolve_union(
        &self,
        source: &NativeSourceTier,
        router_ids: &[u64],
        expansion_ids: &[u64],
    ) -> Result<Vec<SourceCandidate>, SourceIdMapError> {
        if !self.binds_to(source) {
            return Err(SourceIdMapError::Invalid("source generation binding"));
        }
        let total = router_ids
            .len()
            .checked_add(expansion_ids.len())
            .ok_or(SourceIdMapError::Invalid("candidate count overflow"))?;
        if total == 0 {
            return Err(SourceIdMapError::Invalid("empty candidate union"));
        }
        let mut ids = Vec::new();
        ids.try_reserve_exact(total)
            .map_err(|_| SourceIdMapError::Invalid("candidate union allocation"))?;
        ids.extend_from_slice(router_ids);
        ids.extend_from_slice(expansion_ids);
        ids.sort_unstable();
        ids.dedup();
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(ids.len())
            .map_err(|_| SourceIdMapError::Invalid("candidate roster allocation"))?;
        for id in ids {
            let index = self
                .entries
                .binary_search_by_key(&id, |(source_id, _)| *source_id)
                .map_err(|_| SourceIdMapError::Invalid("unknown source ID"))?;
            candidates.push(SourceCandidate {
                ordinal: self.entries[index].1,
                source_id: id,
            });
        }
        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_source_tier::{NativeSourceTier, SourceCandidate, write_source_tier};
    use sha2::Digest;

    const SOURCE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn resolves_router_and_expansion_ids_once_from_bound_generation() {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source.bin");
        let source_artifact = write_source_tier(
            &source_path,
            3,
            2,
            19,
            SOURCE_SHA,
            vec![
                (900, vec![1.0, 0.0]),
                (100, vec![0.0, 1.0]),
                (7, vec![1.0, 0.0]),
            ]
            .into_iter(),
        )
        .unwrap();
        let source = NativeSourceTier::open_authenticated(
            &source_path,
            &source_artifact,
            SOURCE_SHA,
            3,
            2,
            19,
            4096,
        )
        .unwrap();
        let map_path = directory.path().join("source-ids.bin");
        let map_sha = write_source_id_map(
            &map_path,
            3,
            19,
            SOURCE_SHA,
            &source_artifact,
            [900, 100, 7].into_iter(),
        )
        .unwrap();
        let map = NativeSourceIdMap::open_authenticated(
            &map_path,
            &map_sha,
            SOURCE_SHA,
            &source_artifact,
            3,
            19,
        )
        .unwrap();
        let union = map.resolve_union(&source, &[900, 100], &[100, 7]).unwrap();
        assert_eq!(
            union,
            vec![
                SourceCandidate {
                    ordinal: 2,
                    source_id: 7,
                },
                SourceCandidate {
                    ordinal: 1,
                    source_id: 100,
                },
                SourceCandidate {
                    ordinal: 0,
                    source_id: 900,
                },
            ]
        );
        let (ranked, cost) = source
            .rank_exact_with_stats(&[1.0, 0.0], &union, 2)
            .unwrap();
        assert_eq!(
            ranked.iter().map(|row| row.source_id).collect::<Vec<_>>(),
            vec![7, 900]
        );
        assert_eq!(cost.source_rows, 3);
        assert_eq!(map.resident_entry_bytes(), 48);
        assert!(map.resolve_union(&source, &[901], &[7]).is_err());
        assert!(map.resolve_union(&source, &[], &[]).is_err());
    }

    #[test]
    fn rejects_duplicate_missing_extra_and_corrupted_entries() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("ids.bin");
        assert!(
            write_source_id_map(&path, 2, 19, SOURCE_SHA, SOURCE_SHA, [7, 7].into_iter()).is_err()
        );
        assert!(
            write_source_id_map(&path, 2, 19, SOURCE_SHA, SOURCE_SHA, [7].into_iter()).is_err()
        );
        assert!(
            write_source_id_map(&path, 2, 19, SOURCE_SHA, SOURCE_SHA, [7, 8, 9].into_iter())
                .is_err()
        );
        let sha =
            write_source_id_map(&path, 2, 19, SOURCE_SHA, SOURCE_SHA, [7, 8].into_iter()).unwrap();
        assert!(
            NativeSourceIdMap::open_authenticated(&path, &sha, SOURCE_SHA, SOURCE_SHA, 2, 20)
                .is_err()
        );
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[96 + 8..96 + 16].copy_from_slice(&1_u64.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let changed_sha = format!("{:x}", sha2::Sha256::digest(&bytes));
        assert!(
            NativeSourceIdMap::open_authenticated(
                &path,
                &changed_sha,
                SOURCE_SHA,
                SOURCE_SHA,
                2,
                19,
            )
            .is_err()
        );
    }
}
