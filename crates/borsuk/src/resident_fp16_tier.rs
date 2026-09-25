//! Authenticated generation-pinned FP16 rerank plane with explicit RAM admission.

use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use half::f16;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::native_source_tier::SourceCandidate;

const MAGIC: [u8; 8] = *b"BORSF160";
const VERSION: u32 = 1;
const HEADER_BYTES: u64 = 64;

/// An invalid source/plane identity fails closed before the tier can serve.
#[derive(Debug, Error)]
pub enum ResidentFp16Error {
    /// Local artifact I/O failed.
    #[error("resident FP16 artifact I/O failed: {0}")]
    Io(#[from] io::Error),
    /// Artifact geometry, source binding or query roster differs.
    #[error("resident FP16 contract differs: {0}")]
    Invalid(&'static str),
}

/// A fully resident immutable precision plane for one generation.
pub struct ResidentFp16Tier {
    ids: Vec<u64>,
    coordinates: Vec<u16>,
    dimensions: usize,
    generation: u64,
    artifact_sha256: [u8; 32],
    source_sha256: [u8; 32],
}

fn decode_digest(value: &str) -> Result<[u8; 32], ResidentFp16Error> {
    if value.len() != 64 {
        return Err(ResidentFp16Error::Invalid("SHA-256 length"));
    }
    let mut result = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let hex = std::str::from_utf8(chunk)
            .map_err(|_| ResidentFp16Error::Invalid("SHA-256 encoding"))?;
        result[index] =
            u8::from_str_radix(hex, 16).map_err(|_| ResidentFp16Error::Invalid("SHA-256 hex"))?;
    }
    Ok(result)
}

fn geometry(rows: u64, dimensions: usize) -> Result<(usize, usize, u64), ResidentFp16Error> {
    if rows == 0 || dimensions == 0 || dimensions > u32::MAX as usize {
        return Err(ResidentFp16Error::Invalid("plane geometry"));
    }
    let rows_usize = usize::try_from(rows)
        .map_err(|_| ResidentFp16Error::Invalid("row count exceeds address space"))?;
    let coordinates = rows_usize
        .checked_mul(dimensions)
        .ok_or(ResidentFp16Error::Invalid("coordinate count overflow"))?;
    let resident_bytes = rows_usize
        .checked_mul(8)
        .and_then(|ids| coordinates.checked_mul(2)?.checked_add(ids))
        .ok_or(ResidentFp16Error::Invalid("resident byte count overflow"))?;
    let body_bytes = u64::try_from(resident_bytes)
        .map_err(|_| ResidentFp16Error::Invalid("artifact length overflow"))?;
    let artifact_bytes = HEADER_BYTES
        .checked_add(body_bytes)
        .ok_or(ResidentFp16Error::Invalid("artifact length overflow"))?;
    Ok((coordinates, resident_bytes, artifact_bytes))
}

fn header(
    rows: u64,
    dimensions: usize,
    generation: u64,
    source: [u8; 32],
) -> Result<[u8; 64], ResidentFp16Error> {
    geometry(rows, dimensions)?;
    if generation == 0 {
        return Err(ResidentFp16Error::Invalid("zero generation"));
    }
    let mut result = [0_u8; 64];
    result[..8].copy_from_slice(&MAGIC);
    result[8..12].copy_from_slice(&VERSION.to_le_bytes());
    result[12..16].copy_from_slice(&(dimensions as u32).to_le_bytes());
    result[16..24].copy_from_slice(&rows.to_le_bytes());
    result[24..32].copy_from_slice(&generation.to_le_bytes());
    result[32..64].copy_from_slice(&source);
    Ok(result)
}

/// Exact row bytes admitted for a resident FP16 generation, excluding
/// allocator, query workspace, router, deltas and concurrent generations.
pub fn resident_plane_bytes(rows: u64, dimensions: usize) -> Result<usize, ResidentFp16Error> {
    Ok(geometry(rows, dimensions)?.1)
}

/// Write an immutable FP16 plane in the same physical row order as the SQ8
/// generation. The caller publishes its returned hash in that generation's
/// authenticated manifest only after the file is fully synced.
pub fn write_resident_fp16_tier<I>(
    path: &Path,
    rows: u64,
    dimensions: usize,
    generation: u64,
    source_sha256: &str,
    mut vectors: I,
) -> Result<String, ResidentFp16Error>
where
    I: Iterator<Item = (u64, Vec<f32>)>,
{
    let source = decode_digest(source_sha256)?;
    let head = header(rows, dimensions, generation, source)?;
    let parent = path
        .parent()
        .ok_or(ResidentFp16Error::Invalid("artifact parent"))?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    let mut digest = Sha256::new();
    let mut ids = HashSet::new();
    let row_bytes = dimensions
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(8))
        .ok_or(ResidentFp16Error::Invalid("row byte count overflow"))?;
    let mut row = vec![0_u8; row_bytes];
    {
        let mut writer = BufWriter::new(temporary.as_file_mut());
        writer.write_all(&head)?;
        digest.update(head);
        for _ in 0..rows {
            let (id, vector) = vectors
                .next()
                .ok_or(ResidentFp16Error::Invalid("too few source rows"))?;
            if !ids.insert(id) || vector.len() != dimensions {
                return Err(ResidentFp16Error::Invalid("source row identity or width"));
            }
            row[..8].copy_from_slice(&id.to_le_bytes());
            let mut norm = 0.0_f64;
            for (index, coordinate) in vector.into_iter().enumerate() {
                if !coordinate.is_finite() {
                    return Err(ResidentFp16Error::Invalid("source nonfinite coordinate"));
                }
                norm += f64::from(coordinate) * f64::from(coordinate);
                let encoded = f16::from_f32(coordinate);
                if !encoded.is_finite() {
                    return Err(ResidentFp16Error::Invalid("FP16 coordinate overflow"));
                }
                row[8 + 2 * index..10 + 2 * index]
                    .copy_from_slice(&encoded.to_bits().to_le_bytes());
            }
            if !norm.is_finite() || norm <= 0.0 {
                return Err(ResidentFp16Error::Invalid("source zero/nonfinite norm"));
            }
            writer.write_all(&row)?;
            digest.update(&row);
        }
        if vectors.next().is_some() {
            return Err(ResidentFp16Error::Invalid("too many source rows"));
        }
        writer.flush()?;
    }
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| ResidentFp16Error::Io(error.error))?;
    Ok(format!("{:x}", digest.finalize()))
}

impl ResidentFp16Tier {
    /// Source ID at an authenticated physical row. Graph edges use these row
    /// ordinals and retain the plane identity across generation swaps.
    pub(crate) fn source_id(&self, ordinal: usize) -> Result<u64, ResidentFp16Error> {
        self.ids
            .get(ordinal)
            .copied()
            .ok_or(ResidentFp16Error::Invalid("candidate ordinal"))
    }

    /// Cosine score against a query normalized once by the caller. This
    /// scores one graph visit without allocating a shortlist or recomputing
    /// the query norm. The graph validates query geometry and finiteness.
    pub(crate) fn cosine_similarity_unit_query(
        &self,
        normalized: &[f64],
        ordinal: usize,
    ) -> Result<f64, ResidentFp16Error> {
        if normalized.len() != self.dimensions || ordinal >= self.ids.len() {
            return Err(ResidentFp16Error::Invalid("graph score geometry"));
        }
        let offset = ordinal
            .checked_mul(self.dimensions)
            .ok_or(ResidentFp16Error::Invalid("candidate offset"))?;
        let mut dot = 0.0_f64;
        let mut norm_squared = 0.0_f64;
        for (index, &bits) in self.coordinates[offset..offset + self.dimensions]
            .iter()
            .enumerate()
        {
            let value = f64::from(f16::from_bits(bits).to_f32());
            dot += value * normalized[index];
            norm_squared += value * value;
        }
        if !norm_squared.is_finite() || norm_squared <= 0.0 {
            return Err(ResidentFp16Error::Invalid("candidate FP16 norm"));
        }
        Ok(dot / norm_squared.sqrt())
    }

    /// Open and fully authenticate an immutable plane. The budget check
    /// precedes its large allocation; this object itself owns the charged
    /// ID and coordinate arrays until the pinned generation is released.
    pub fn open_authenticated(
        path: &Path,
        artifact_sha256: &str,
        source_sha256: &str,
        rows: u64,
        dimensions: usize,
        generation: u64,
        resident_budget_bytes: usize,
    ) -> Result<Self, ResidentFp16Error> {
        let expected_artifact = decode_digest(artifact_sha256)?;
        let expected_source = decode_digest(source_sha256)?;
        let (coordinates, resident_bytes, artifact_bytes) = geometry(rows, dimensions)?;
        if resident_bytes > resident_budget_bytes {
            return Err(ResidentFp16Error::Invalid("resident budget"));
        }
        let file = File::open(path)?;
        if file.metadata()?.len() != artifact_bytes {
            return Err(ResidentFp16Error::Invalid("artifact byte length"));
        }
        let mut reader = BufReader::new(file);
        let mut head = [0_u8; 64];
        reader.read_exact(&mut head)?;
        if head != header(rows, dimensions, generation, expected_source)? {
            return Err(ResidentFp16Error::Invalid(
                "format/source/generation header",
            ));
        }
        let mut digest = Sha256::new();
        digest.update(head);
        let rows_usize = usize::try_from(rows)
            .map_err(|_| ResidentFp16Error::Invalid("row count exceeds address space"))?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(rows_usize)
            .map_err(|_| ResidentFp16Error::Invalid("resident ID allocation"))?;
        let mut data = Vec::new();
        data.try_reserve_exact(coordinates)
            .map_err(|_| ResidentFp16Error::Invalid("resident coordinate allocation"))?;
        let row_bytes = dimensions
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(8))
            .ok_or(ResidentFp16Error::Invalid("row byte count overflow"))?;
        let mut row = vec![0_u8; row_bytes];
        for _ in 0..rows {
            reader.read_exact(&mut row)?;
            digest.update(&row);
            ids.push(u64::from_le_bytes(row[..8].try_into().unwrap()));
            for coordinate in row[8..].chunks_exact(2) {
                let bits = u16::from_le_bytes(coordinate.try_into().unwrap());
                if !f16::from_bits(bits).is_finite() {
                    return Err(ResidentFp16Error::Invalid("nonfinite FP16 artifact"));
                }
                data.push(bits);
            }
        }
        let computed: [u8; 32] = digest.finalize().into();
        if computed != expected_artifact {
            return Err(ResidentFp16Error::Invalid("artifact SHA-256"));
        }
        Ok(Self {
            ids,
            coordinates: data,
            dimensions,
            generation,
            artifact_sha256: expected_artifact,
            source_sha256: expected_source,
        })
    }

    /// Charged ID and FP16 payload bytes owned by this generation.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        self.ids.capacity() * 8 + self.coordinates.capacity() * 2
    }

    /// Pinned generation number.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Number of authenticated physical rows in this plane.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.ids.len()
    }

    /// Coordinate width of each physical row.
    #[must_use]
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Authenticated source identity.
    #[must_use]
    pub fn source_sha256(&self) -> [u8; 32] {
        self.source_sha256
    }

    /// Authenticated complete artifact identity.
    #[must_use]
    pub fn artifact_sha256(&self) -> [u8; 32] {
        self.artifact_sha256
    }

    /// Score a complete authenticated physical shortlist without an SQ8
    /// body read. The plane supplies the public ID for each ordinal; callers
    /// must first bind this tier and the ordinal mapping to one generation.
    pub fn rank_ordinals_cosine(
        &self,
        query: &[f32],
        ordinals: &[usize],
        top_k: usize,
    ) -> Result<Vec<u64>, ResidentFp16Error> {
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(ordinals.len())
            .map_err(|_| ResidentFp16Error::Invalid("candidate allocation"))?;
        for &ordinal in ordinals {
            let &source_id = self
                .ids
                .get(ordinal)
                .ok_or(ResidentFp16Error::Invalid("candidate ordinal"))?;
            candidates.push(SourceCandidate {
                ordinal: ordinal as u64,
                source_id,
            });
        }
        self.rank_cosine(query, &candidates, top_k)
    }

    /// Deterministic cosine top-k within a fixed SQ8 physical shortlist.
    /// A candidate ID/ordinal mismatch fails closed instead of silently
    /// scoring a row from another generation.
    pub fn rank_cosine(
        &self,
        query: &[f32],
        candidates: &[SourceCandidate],
        top_k: usize,
    ) -> Result<Vec<u64>, ResidentFp16Error> {
        if (query.len() != self.dimensions
            || query.iter().any(|x| !x.is_finite())
            || top_k == 0
            || top_k > candidates.len())
        {
            return Err(ResidentFp16Error::Invalid("query/top-k geometry"));
        }
        let query_norm = query
            .iter()
            .fold(0.0_f64, |sum, &x| sum + f64::from(x) * f64::from(x))
            .sqrt();
        if !query_norm.is_finite() || query_norm <= 0.0 {
            return Err(ResidentFp16Error::Invalid("query norm"));
        }
        let normalized = query
            .iter()
            .map(|&x| f64::from(x) / query_norm)
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let mut scores = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let ordinal = usize::try_from(candidate.ordinal)
                .map_err(|_| ResidentFp16Error::Invalid("candidate ordinal"))?;
            if ordinal >= self.ids.len()
                || !seen.insert(ordinal)
                || self.ids[ordinal] != candidate.source_id
            {
                return Err(ResidentFp16Error::Invalid("candidate identity/generation"));
            }
            let offset = ordinal
                .checked_mul(self.dimensions)
                .ok_or(ResidentFp16Error::Invalid("candidate offset"))?;
            let mut dot = 0.0_f64;
            let mut norm_squared = 0.0_f64;
            for (index, &bits) in self.coordinates[offset..offset + self.dimensions]
                .iter()
                .enumerate()
            {
                let value = f64::from(f16::from_bits(bits).to_f32());
                dot += value * normalized[index];
                norm_squared += value * value;
            }
            if !norm_squared.is_finite() || norm_squared <= 0.0 {
                return Err(ResidentFp16Error::Invalid("candidate FP16 norm"));
            }
            scores.push((candidate.source_id, dot / norm_squared.sqrt()));
        }
        scores.sort_unstable_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(scores.into_iter().take(top_k).map(|(id, _)| id).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn authenticated_generation_budget_and_cosine_rerank() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plane.bin");
        let digest = write_resident_fp16_tier(
            &path,
            3,
            2,
            7,
            SOURCE,
            vec![
                (42, vec![1.0, 0.0]),
                (7, vec![0.0, 1.0]),
                (19, vec![0.5, 0.5]),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(resident_plane_bytes(3, 2).unwrap(), 36);
        assert!(ResidentFp16Tier::open_authenticated(&path, &digest, SOURCE, 3, 2, 7, 35).is_err());
        let tier =
            ResidentFp16Tier::open_authenticated(&path, &digest, SOURCE, 3, 2, 7, 36).unwrap();
        assert_eq!(tier.resident_bytes(), 36);
        assert_eq!(tier.generation(), 7);
        assert_eq!(tier.rows(), 3);
        assert_eq!(tier.dimensions(), 2);
        let roster = [
            SourceCandidate {
                ordinal: 1,
                source_id: 7,
            },
            SourceCandidate {
                ordinal: 0,
                source_id: 42,
            },
            SourceCandidate {
                ordinal: 2,
                source_id: 19,
            },
        ];
        assert_eq!(
            tier.rank_cosine(&[1.0, 0.0], &roster, 2).unwrap(),
            vec![42, 19]
        );
        assert_eq!(
            tier.rank_ordinals_cosine(&[1.0, 0.0], &[1, 0, 2], 2)
                .unwrap(),
            vec![42, 19]
        );
        assert!(tier.rank_ordinals_cosine(&[1.0, 0.0], &[1, 1], 1).is_err());
        assert!(tier.rank_ordinals_cosine(&[1.0, 0.0], &[3], 1).is_err());
        assert!(
            tier.rank_cosine(
                &[1.0, 0.0],
                &[SourceCandidate {
                    ordinal: 0,
                    source_id: 7
                }],
                1
            )
            .is_err()
        );
        assert!(ResidentFp16Tier::open_authenticated(&path, &digest, SOURCE, 3, 2, 8, 36).is_err());
    }

    #[test]
    fn corrupt_artifact_and_invalid_source_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plane.bin");
        let digest = write_resident_fp16_tier(
            &path,
            1,
            2,
            1,
            SOURCE,
            vec![(3, vec![1.0, 0.0])].into_iter(),
        )
        .unwrap();
        assert!(
            write_resident_fp16_tier(
                &path,
                1,
                2,
                1,
                SOURCE,
                vec![(3, vec![1.0, 0.0])].into_iter(),
            )
            .is_err()
        );
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[65] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(ResidentFp16Tier::open_authenticated(&path, &digest, SOURCE, 1, 2, 1, 12).is_err());
        let invalid = directory.path().join("invalid.bin");
        assert!(
            write_resident_fp16_tier(
                &invalid,
                1,
                2,
                1,
                SOURCE,
                vec![(3, vec![f32::NAN, 0.0])].into_iter(),
            )
            .is_err()
        );
    }
}
