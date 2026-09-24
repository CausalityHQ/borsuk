//! Authenticated exact-source fallback for a generation-pinned candidate set.
//!
//! This module owns a versioned local float32 plane. S3 hydration, a faster
//! first-pass plane, routing and expansion are separate layers. Search is
//! exact cosine within the supplied candidates and never changes candidate
//! membership to fit a memory or vector-count threshold.

use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    os::unix::fs::FileExt,
    path::Path,
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

const MAGIC: [u8; 8] = *b"BORSST02";
const VERSION: u32 = 2;
const HEADER_BYTES: u64 = 64;

/// An exact-source tier error. Invalid or unauthenticated artifacts fail closed.
#[derive(Debug, Error)]
pub enum SourceTierError {
    /// An underlying local filesystem operation failed.
    #[error("source tier I/O failed: {0}")]
    Io(#[from] io::Error),
    /// The source artifact, geometry, query or candidate roster is invalid.
    #[error("source tier contract differs: {0}")]
    Invalid(&'static str),
}

/// A physical/source mapping entry supplied by an authenticated generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceCandidate {
    /// Ordinal in the source plane.
    pub ordinal: u64,
    /// Public source identifier used for deterministic tie breaks.
    pub source_id: u64,
}

/// One exact cosine result within a supplied candidate roster.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScoredSourceCandidate {
    /// Public source identifier.
    pub source_id: u64,
    /// Cosine score, descending in search results.
    pub score: f64,
}

/// A pinned, authenticated local source plane.
#[derive(Debug)]
pub struct NativeSourceTier {
    file: File,
    rows: u64,
    dimensions: usize,
    generation: u64,
    source_sha256: [u8; 32],
    verification_block_bytes: usize,
    block_digests: Vec<[u8; 32]>,
}

pub(crate) fn decoded_sha256(value: &str) -> Result<[u8; 32], SourceTierError> {
    if value.len() != 64 {
        return Err(SourceTierError::Invalid("SHA-256 length"));
    }
    let mut bytes = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text =
            std::str::from_utf8(chunk).map_err(|_| SourceTierError::Invalid("SHA-256 encoding"))?;
        bytes[index] =
            u8::from_str_radix(text, 16).map_err(|_| SourceTierError::Invalid("SHA-256 hex"))?;
    }
    Ok(bytes)
}

fn row_bytes(dimensions: usize) -> Result<u64, SourceTierError> {
    if dimensions == 0 {
        return Err(SourceTierError::Invalid("zero dimensions"));
    }
    u64::try_from(dimensions)
        .ok()
        .and_then(|value| value.checked_mul(4))
        .and_then(|coordinates| coordinates.checked_add(8))
        .ok_or(SourceTierError::Invalid("row width overflow"))
}

pub(crate) fn expected_len(rows: u64, dimensions: usize) -> Result<u64, SourceTierError> {
    rows.checked_mul(row_bytes(dimensions)?)
        .and_then(|body| body.checked_add(HEADER_BYTES))
        .ok_or(SourceTierError::Invalid("source length overflow"))
}

fn header(
    rows: u64,
    dimensions: usize,
    generation: u64,
    source: [u8; 32],
) -> Result<[u8; 64], SourceTierError> {
    if rows == 0 || dimensions == 0 || generation == 0 {
        return Err(SourceTierError::Invalid("source geometry or generation"));
    }
    let dimensions = u32::try_from(dimensions)
        .map_err(|_| SourceTierError::Invalid("dimensions exceed format"))?;
    expected_len(rows, dimensions as usize)?;
    let mut bytes = [0_u8; 64];
    bytes[..8].copy_from_slice(&MAGIC);
    bytes[8..12].copy_from_slice(&VERSION.to_le_bytes());
    bytes[12..16].copy_from_slice(&dimensions.to_le_bytes());
    bytes[16..24].copy_from_slice(&rows.to_le_bytes());
    bytes[24..32].copy_from_slice(&generation.to_le_bytes());
    bytes[32..64].copy_from_slice(&source);
    Ok(bytes)
}

/// Stream one generation's float32 source plane into a new immutable file.
///
/// `vectors` must contain exactly `rows` unique source-ID/vector pairs. The caller
/// supplies the authenticated source-object SHA-256 and publishes this file
/// only after its returned artifact SHA-256 is bound in a generation manifest.
/// A temporary file is synced and atomically installed without replacing an
/// existing artifact.
pub fn write_source_tier<I>(
    path: &Path,
    rows: u64,
    dimensions: usize,
    generation: u64,
    source_sha256: &str,
    mut vectors: I,
) -> Result<String, SourceTierError>
where
    I: Iterator<Item = (u64, Vec<f32>)>,
{
    let source = decoded_sha256(source_sha256)?;
    let head = header(rows, dimensions, generation, source)?;
    let parent = path
        .parent()
        .ok_or(SourceTierError::Invalid("source path has no parent"))?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    let mut digest = Sha256::new();
    let mut source_ids = HashSet::new();
    {
        let mut writer = BufWriter::new(temporary.as_file_mut());
        writer.write_all(&head)?;
        digest.update(head);
        for _ in 0..rows {
            let (source_id, vector) = vectors
                .next()
                .ok_or(SourceTierError::Invalid("too few source rows"))?;
            if !source_ids.insert(source_id) {
                return Err(SourceTierError::Invalid("duplicate source ID"));
            }
            if vector.len() != dimensions || vector.iter().any(|value| !value.is_finite()) {
                return Err(SourceTierError::Invalid("source vector geometry"));
            }
            let norm_squared = vector.iter().fold(0.0_f64, |acc, &value| {
                acc + f64::from(value) * f64::from(value)
            });
            if !norm_squared.is_finite() || norm_squared <= 0.0 {
                return Err(SourceTierError::Invalid("source vector norm"));
            }
            let id_bytes = source_id.to_le_bytes();
            writer.write_all(&id_bytes)?;
            digest.update(id_bytes);
            for value in vector {
                let bytes = value.to_le_bytes();
                writer.write_all(&bytes)?;
                digest.update(bytes);
            }
        }
        if vectors.next().is_some() {
            return Err(SourceTierError::Invalid("too many source rows"));
        }
        writer.flush()?;
    }
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| SourceTierError::Io(error.error))?;
    Ok(format!("{:x}", digest.finalize()))
}

impl NativeSourceTier {
    /// Open one local generation only after checking full-file SHA-256,
    /// version, source identity, exact geometry and byte length. Block digests
    /// derived in that scan authenticate every later local candidate read.
    /// `verification_block_bytes` is an explicit memory/I/O capacity choice;
    /// it must be a power of two from 4 KiB through 1 MiB.
    pub fn open_authenticated(
        path: &Path,
        artifact_sha256: &str,
        source_sha256: &str,
        rows: u64,
        dimensions: usize,
        generation: u64,
        verification_block_bytes: usize,
    ) -> Result<Self, SourceTierError> {
        let expected_artifact = decoded_sha256(artifact_sha256)?;
        let source = decoded_sha256(source_sha256)?;
        if !verification_block_bytes.is_power_of_two()
            || !(4096..=1024 * 1024).contains(&verification_block_bytes)
        {
            return Err(SourceTierError::Invalid("verification block size"));
        }
        let length = expected_len(rows, dimensions)?;
        let file = File::open(path)?;
        let mut head = [0_u8; 64];
        read_exact_at(&file, &mut head, 0)?;
        if head[..8] != MAGIC || head[8..12] != VERSION.to_le_bytes() {
            return Err(SourceTierError::Invalid(
                "unsupported source format version",
            ));
        }
        if file.metadata()?.len() != length {
            return Err(SourceTierError::Invalid("source byte length"));
        }
        let block_width = verification_block_bytes as u64;
        let block_count = usize::try_from(length.div_ceil(block_width))
            .map_err(|_| SourceTierError::Invalid("digest count exceeds address space"))?;
        block_count
            .checked_mul(32)
            .ok_or(SourceTierError::Invalid("digest table size overflow"))?;
        let mut block_digests = Vec::new();
        block_digests
            .try_reserve_exact(block_count)
            .map_err(|_| SourceTierError::Invalid("digest table allocation"))?;
        let mut actual = Sha256::new();
        let mut reader = BufReader::new(&file);
        let mut block = vec![0_u8; verification_block_bytes];
        let mut remaining = length;
        while remaining > 0 {
            let count = remaining.min(block_width) as usize;
            reader.read_exact(&mut block[..count])?;
            actual.update(&block[..count]);
            block_digests.push(Sha256::digest(&block[..count]).into());
            remaining -= count as u64;
        }
        let computed: [u8; 32] = actual.finalize().into();
        if computed != expected_artifact {
            return Err(SourceTierError::Invalid("source artifact SHA-256"));
        }
        read_exact_at(&file, &mut head, 0)?;
        if head != header(rows, dimensions, generation, source)? {
            return Err(SourceTierError::Invalid("source format or generation"));
        }
        Ok(Self {
            file,
            rows,
            dimensions,
            generation,
            source_sha256: source,
            verification_block_bytes,
            block_digests,
        })
    }

    /// Number of source ordinals in the plane.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Vector dimensionality.
    #[must_use]
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Authenticated generation identifier.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// SHA-256 identity of the source object from which this plane was built.
    #[must_use]
    pub fn source_sha256(&self) -> [u8; 32] {
        self.source_sha256
    }

    /// Configured authenticated local read granularity.
    #[must_use]
    pub fn verification_block_bytes(&self) -> usize {
        self.verification_block_bytes
    }

    /// Resident bytes in the per-block SHA-256 table, excluding Vec overhead.
    #[must_use]
    pub fn resident_digest_bytes(&self) -> usize {
        self.block_digests.len() * 32
    }

    fn read_verified_row(
        &self,
        output: &mut [u8],
        scratch: &mut [u8],
        offset: u64,
    ) -> Result<(), SourceTierError> {
        let end = offset
            .checked_add(output.len() as u64)
            .ok_or(SourceTierError::Invalid("source row end overflow"))?;
        let block_width = self.verification_block_bytes as u64;
        let length = expected_len(self.rows, self.dimensions)?;
        if end > length || scratch.len() != self.verification_block_bytes {
            return Err(SourceTierError::Invalid("source row or scratch geometry"));
        }
        for block_number in offset / block_width..=(end - 1) / block_width {
            let block_start = block_number
                .checked_mul(block_width)
                .ok_or(SourceTierError::Invalid("source block offset overflow"))?;
            let count = (length - block_start).min(block_width) as usize;
            read_exact_at(&self.file, &mut scratch[..count], block_start)?;
            let digest: [u8; 32] = Sha256::digest(&scratch[..count]).into();
            let index = usize::try_from(block_number)
                .map_err(|_| SourceTierError::Invalid("digest index exceeds address space"))?;
            if self.block_digests.get(index) != Some(&digest) {
                return Err(SourceTierError::Invalid("source block SHA-256"));
            }
            let copy_start = offset.max(block_start);
            let copy_end = end.min(block_start + count as u64);
            let from = (copy_start - block_start) as usize;
            let to = (copy_start - offset) as usize;
            let copy_len = (copy_end - copy_start) as usize;
            output[to..to + copy_len].copy_from_slice(&scratch[from..from + copy_len]);
        }
        Ok(())
    }

    /// Exact cosine top-k within a previously fixed candidate union.
    ///
    /// It reads one float32 row per candidate from the authenticated local
    /// plane. Every local read is therefore part of the serving cost, even
    /// though this method issues no S3 GET itself.
    pub fn rank_exact(
        &self,
        query: &[f32],
        candidates: &[SourceCandidate],
        top_k: usize,
    ) -> Result<Vec<ScoredSourceCandidate>, SourceTierError> {
        if query.len() != self.dimensions
            || top_k == 0
            || top_k > candidates.len()
            || query.iter().any(|value| !value.is_finite())
        {
            return Err(SourceTierError::Invalid("query or top-k geometry"));
        }
        let query_norm = query
            .iter()
            .fold(0.0_f64, |acc, &value| {
                acc + f64::from(value) * f64::from(value)
            })
            .sqrt();
        if !query_norm.is_finite() || query_norm <= 0.0 {
            return Err(SourceTierError::Invalid("query norm"));
        }
        let query_unit = query
            .iter()
            .map(|&value| f64::from(value) / query_norm)
            .collect::<Vec<_>>();
        let mut ids = HashSet::with_capacity(candidates.len());
        let mut ordinals = HashSet::with_capacity(candidates.len());
        let mut scored = Vec::with_capacity(candidates.len());
        let width = usize::try_from(row_bytes(self.dimensions)?)
            .map_err(|_| SourceTierError::Invalid("row width exceeds address space"))?;
        let mut bytes = vec![0_u8; width];
        let mut scratch = vec![0_u8; self.verification_block_bytes];
        for candidate in candidates {
            if candidate.ordinal >= self.rows
                || !ids.insert(candidate.source_id)
                || !ordinals.insert(candidate.ordinal)
            {
                return Err(SourceTierError::Invalid("candidate identity"));
            }
            let offset = candidate
                .ordinal
                .checked_mul(width as u64)
                .and_then(|body_offset| body_offset.checked_add(HEADER_BYTES))
                .ok_or(SourceTierError::Invalid("source row offset overflow"))?;
            self.read_verified_row(&mut bytes, &mut scratch, offset)?;
            let authenticated_id = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            if authenticated_id != candidate.source_id {
                return Err(SourceTierError::Invalid("candidate/source ID mismatch"));
            }
            let mut norm_squared = 0.0_f64;
            let mut dot = 0.0_f64;
            for (coordinate, encoded) in bytes[8..].chunks_exact(4).enumerate() {
                let value = f64::from(f32::from_le_bytes(encoded.try_into().unwrap()));
                if !value.is_finite() {
                    return Err(SourceTierError::Invalid("source vector nonfinite"));
                }
                norm_squared += value * value;
                dot += value * query_unit[coordinate];
            }
            let norm = norm_squared.sqrt();
            if !norm.is_finite() || norm <= 0.0 {
                return Err(SourceTierError::Invalid("source vector norm"));
            }
            let score = dot / norm;
            if !score.is_finite() {
                return Err(SourceTierError::Invalid("source score nonfinite"));
            }
            scored.push(ScoredSourceCandidate {
                source_id: candidate.source_id,
                score,
            });
        }
        scored.sort_unstable_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.source_id.cmp(&right.source_id))
        });
        scored.truncate(top_k);
        Ok(scored)
    }
}

fn read_exact_at(
    file: &File,
    mut output: &mut [u8],
    mut offset: u64,
) -> Result<(), SourceTierError> {
    while !output.is_empty() {
        let count = file.read_at(output, offset)?;
        if count == 0 {
            return Err(SourceTierError::Invalid("source row truncated"));
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or(SourceTierError::Invalid("source read offset overflow"))?;
        output = &mut output[count..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn authenticated_plane_ranks_nonmonotone_ids_and_stable_ties() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.bin");
        let sha = write_source_tier(
            &path,
            3,
            2,
            7,
            SOURCE_SHA,
            vec![
                (900, vec![1.0, 0.0]),
                (500, vec![0.0, 1.0]),
                (100, vec![1.0, 0.0]),
            ]
            .into_iter(),
        )
        .unwrap();
        let tier =
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 3, 2, 7, 4096).unwrap();
        let result = tier
            .rank_exact(
                &[1.0, 0.0],
                &[
                    SourceCandidate {
                        ordinal: 0,
                        source_id: 900,
                    },
                    SourceCandidate {
                        ordinal: 1,
                        source_id: 500,
                    },
                    SourceCandidate {
                        ordinal: 2,
                        source_id: 100,
                    },
                ],
                2,
            )
            .unwrap();
        assert_eq!(
            result.iter().map(|row| row.source_id).collect::<Vec<_>>(),
            vec![100, 900]
        );
        assert_eq!(result[0].score, 1.0);
    }

    #[test]
    fn authentication_rejects_tampering_and_wrong_generation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.bin");
        let sha = write_source_tier(
            &path,
            1,
            2,
            7,
            SOURCE_SHA,
            vec![(3, vec![1.0, 0.0])].into_iter(),
        )
        .unwrap();
        assert!(
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 1, 2, 8, 4096).is_err()
        );
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&[0_u8; 4], HEADER_BYTES).unwrap();
        assert!(
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 1, 2, 7, 4096).is_err()
        );
    }

    #[test]
    fn read_rejects_valid_vector_mutation_after_open() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.bin");
        let sha = write_source_tier(
            &path,
            1,
            2,
            7,
            SOURCE_SHA,
            vec![(3, vec![1.0, 0.0])].into_iter(),
        )
        .unwrap();
        let tier =
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 1, 2, 7, 4096).unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&0.0_f32.to_le_bytes(), HEADER_BYTES + 8)
            .unwrap();
        file.write_all_at(&1.0_f32.to_le_bytes(), HEADER_BYTES + 12)
            .unwrap();
        assert!(
            tier.rank_exact(
                &[1.0, 0.0],
                &[SourceCandidate {
                    ordinal: 0,
                    source_id: 3
                }],
                1
            )
            .is_err()
        );
    }

    #[test]
    fn verifies_rows_spanning_blocks_and_reports_digest_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.bin");
        let mut first = vec![0.0_f32; 1024];
        first[0] = 1.0;
        let mut second = vec![0.0_f32; 1024];
        second[1023] = 1.0;
        let sha = write_source_tier(
            &path,
            2,
            1024,
            7,
            SOURCE_SHA,
            vec![(1, first), (2, second)].into_iter(),
        )
        .unwrap();
        assert!(
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 2, 1024, 7, 3000)
                .is_err()
        );
        let tier = NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 2, 1024, 7, 4096)
            .unwrap();
        assert_eq!(tier.verification_block_bytes(), 4096);
        assert_eq!(tier.resident_digest_bytes(), 96);
        let mut query = vec![0.0_f32; 1024];
        query[0] = 1.0;
        let candidates = [
            SourceCandidate {
                ordinal: 1,
                source_id: 2,
            },
            SourceCandidate {
                ordinal: 0,
                source_id: 1,
            },
        ];
        let result = tier.rank_exact(&query, &candidates, 2).unwrap();
        assert_eq!(result[0].source_id, 1);
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&2.0_f32.to_le_bytes(), HEADER_BYTES + 2 * (8 + 4096) - 4)
            .unwrap();
        assert!(tier.rank_exact(&query, &candidates, 2).is_err());
    }

    #[test]
    fn rejects_invalid_rows_and_duplicate_candidates() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.bin");
        assert!(
            write_source_tier(
                &path,
                1,
                2,
                1,
                SOURCE_SHA,
                vec![(42, vec![f32::NAN, 0.0])].into_iter()
            )
            .is_err()
        );
        assert!(
            write_source_tier(
                &path,
                2,
                2,
                1,
                SOURCE_SHA,
                vec![(42, vec![1.0, 0.0]), (42, vec![0.0, 1.0])].into_iter(),
            )
            .is_err()
        );
        let sha = write_source_tier(
            &path,
            2,
            2,
            1,
            SOURCE_SHA,
            vec![(42, vec![1.0, 0.0]), (43, vec![0.0, 1.0])].into_iter(),
        )
        .unwrap();
        let tier =
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 2, 2, 1, 4096).unwrap();
        assert!(
            tier.rank_exact(
                &[1.0, 0.0],
                &[
                    SourceCandidate {
                        ordinal: 0,
                        source_id: 42
                    },
                    SourceCandidate {
                        ordinal: 1,
                        source_id: 42
                    },
                ],
                1
            )
            .is_err()
        );
    }

    #[test]
    fn cosine_normalizes_nonunit_vectors_and_rejects_wrong_identity() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.bin");
        let sha = write_source_tier(
            &path,
            2,
            2,
            3,
            SOURCE_SHA,
            vec![(10, vec![100.0, 0.0]), (11, vec![1.0, 1.0])].into_iter(),
        )
        .unwrap();
        let tier =
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 2, 2, 3, 4096).unwrap();
        let result = tier
            .rank_exact(
                &[2.0, 0.0],
                &[
                    SourceCandidate {
                        ordinal: 1,
                        source_id: 11,
                    },
                    SourceCandidate {
                        ordinal: 0,
                        source_id: 10,
                    },
                ],
                2,
            )
            .unwrap();
        assert_eq!(result[0].source_id, 10);
        assert_eq!(result[0].score, 1.0);
        assert!(
            NativeSourceTier::open_authenticated(&path, &sha, &"2".repeat(64), 2, 2, 3, 4096)
                .is_err()
        );
        assert!(
            write_source_tier(
                &path,
                1,
                2,
                3,
                SOURCE_SHA,
                vec![(10, vec![1.0, 0.0])].into_iter(),
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unbound_candidate_source_id() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.bin");
        let sha = write_source_tier(
            &path,
            1,
            2,
            7,
            SOURCE_SHA,
            vec![(42, vec![1.0, 0.0])].into_iter(),
        )
        .unwrap();
        let tier =
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 1, 2, 7, 4096).unwrap();
        assert!(
            tier.rank_exact(
                &[1.0, 0.0],
                &[SourceCandidate {
                    ordinal: 0,
                    source_id: 999
                }],
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn old_format_marker_is_rejected_clearly() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.bin");
        write_source_tier(
            &path,
            1,
            2,
            7,
            SOURCE_SHA,
            vec![(42, vec![1.0, 0.0])].into_iter(),
        )
        .unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(b"BORSST01", 0).unwrap();
        let sha = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
        assert!(matches!(
            NativeSourceTier::open_authenticated(&path, &sha, SOURCE_SHA, 1, 2, 7, 4096),
            Err(SourceTierError::Invalid(
                "unsupported source format version"
            ))
        ));
    }
}
