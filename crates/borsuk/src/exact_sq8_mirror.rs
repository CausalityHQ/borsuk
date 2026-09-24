//! Generation-bound local SQ8 placement with authenticated disk block reads.

use crate::exact_sq8_nominee::{ScoredNominee, Sq8Geometry, Sq8ScoreError, score_nominees};
use crate::sq8_page_authority::{PageAuthority, PageError};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::Path;

const BLOCK_BYTES: usize = 4096;
const FORMAT_VERSION: u32 = 1;

/// Values come from an authenticated generation manifest, not query data.
#[derive(Clone, Debug)]
pub struct MirrorManifest {
    /// Local mirror sidecar format marker; currently one.
    pub format_version: u32,
    /// Authenticated generation number.
    pub generation: u64,
    /// Maximum candidate count accepted by one query.
    pub max_nominees: usize,
    /// Number of rows and dimensions bound to the SQ8 object.
    pub geometry: Sq8Geometry,
    /// Whole-object SHA-256 from the generation authority.
    pub object_sha256: String,
    /// Whole-sidecar SHA-256 from the generation authority.
    pub block_digest_sha256: String,
    /// Public SQ8 affine origins for the generation.
    pub low: Vec<f32>,
    /// Positive SQ8 affine steps for the generation.
    pub step: Vec<f32>,
}

/// Explicit local placement; there is no vector-count switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    /// Hold the verified object in process memory.
    Ram,
    /// Read local file blocks and authenticate each at query time.
    File,
}

/// Local mirror construction or query failure.
#[derive(Debug)]
pub enum MirrorError {
    /// The generation declaration is malformed.
    InvalidManifest,
    /// The object or sidecar has the wrong length.
    InvalidPlane,
    /// A whole-object or whole-sidecar digest differs.
    HashMismatch,
    /// A query-time block differs from its authenticated digest.
    BlockCorrupt,
    /// The requested physical page span or its content failed verification.
    Page(PageError),
    /// The local filesystem returned an error.
    Io(io::Error),
    /// The exact row scorer rejected query or record data.
    Score(Sq8ScoreError),
}

impl fmt::Display for MirrorError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidManifest => write!(output, "invalid SQ8 mirror manifest"),
            Self::InvalidPlane => write!(output, "invalid SQ8 mirror plane"),
            Self::HashMismatch => write!(output, "SQ8 mirror hash mismatch"),
            Self::BlockCorrupt => write!(output, "SQ8 mirror block corrupt"),
            Self::Page(error) => write!(output, "SQ8 page: {error:?}"),
            Self::Io(error) => write!(output, "SQ8 mirror I/O: {error}"),
            Self::Score(error) => write!(output, "SQ8 mirror score: {error:?}"),
        }
    }
}

impl std::error::Error for MirrorError {}

impl From<io::Error> for MirrorError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<Sq8ScoreError> for MirrorError {
    fn from(error: Sq8ScoreError) -> Self {
        Self::Score(error)
    }
}

enum Backing {
    Ram(Vec<u8>),
    File { file: File, digests: Vec<[u8; 32]> },
}

/// The disk variant checks every fetched block against a manifest-bound
/// digest. This does not authenticate the manifest itself; the caller must
/// obtain it through the generation authority before opening the mirror.
pub struct ExactSq8Mirror {
    manifest: MirrorManifest,
    backing: Backing,
    object_bytes: usize,
}

/// An authenticated, half-open physical byte range read from the local SQ8
/// placement. `verified_block_reads` counts disk verification blocks; RAM
/// reads have no query-time disk verification.
pub struct LocalVerifiedRange {
    pub start: usize,
    pub bytes: Vec<u8>,
    pub verified_block_reads: usize,
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn hash_bytes(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

fn hash_file(file: &File, object_bytes: usize, sidecar: &[u8]) -> Result<String, MirrorError> {
    let mut reader = io::BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = [0u8; BLOCK_BYTES];
    for (index, expected) in sidecar.chunks_exact(32).enumerate() {
        let block_start = index * BLOCK_BYTES;
        let length = (object_bytes - block_start).min(BLOCK_BYTES);
        reader.read_exact(&mut buffer[..length])?;
        if Sha256::digest(&buffer[..length]).as_slice() != expected {
            return Err(MirrorError::BlockCorrupt);
        }
        digest.update(&buffer[..length]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn read_exact_at(file: &File, output: &mut [u8], offset: usize) -> Result<(), io::Error> {
    let mut read = 0;
    while read < output.len() {
        let position = u64::try_from(offset + read)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "SQ8 offset overflow"))?;
        let count = file.read_at(&mut output[read..], position)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short SQ8 block",
            ));
        }
        read += count;
    }
    Ok(())
}

impl ExactSq8Mirror {
    /// Authenticate an SQ8 object and block-digest sidecar before serving.
    pub fn open(
        object_path: &Path,
        digest_path: &Path,
        manifest: MirrorManifest,
        placement: Placement,
    ) -> Result<Self, MirrorError> {
        let row_bytes = manifest
            .geometry
            .dimensions
            .checked_add(12)
            .ok_or(MirrorError::InvalidManifest)?;
        let object_bytes = manifest
            .geometry
            .rows
            .checked_mul(row_bytes)
            .ok_or(MirrorError::InvalidManifest)?;
        if manifest.format_version != FORMAT_VERSION
            || manifest.generation == 0
            || manifest.max_nominees == 0
            || manifest.max_nominees > manifest.geometry.rows
            || manifest.geometry.rows == 0
            || manifest.geometry.dimensions == 0
            || manifest.low.len() != manifest.geometry.dimensions
            || manifest.step.len() != manifest.geometry.dimensions
            || manifest.low.iter().any(|value| !value.is_finite())
            || manifest
                .step
                .iter()
                .any(|value| !value.is_finite() || *value <= 0.0)
            || !valid_hash(&manifest.object_sha256)
            || !valid_hash(&manifest.block_digest_sha256)
        {
            return Err(MirrorError::InvalidManifest);
        }
        let file = File::open(object_path)?;
        if file.metadata()?.len()
            != u64::try_from(object_bytes).map_err(|_| MirrorError::InvalidManifest)?
        {
            return Err(MirrorError::InvalidPlane);
        }
        let sidecar = fs::read(digest_path)?;
        let blocks = object_bytes.div_ceil(BLOCK_BYTES);
        if sidecar.len() != blocks.checked_mul(32).ok_or(MirrorError::InvalidManifest)? {
            return Err(MirrorError::InvalidPlane);
        }
        if hash_bytes(&sidecar) != manifest.block_digest_sha256 {
            return Err(MirrorError::HashMismatch);
        }
        if hash_file(&file, object_bytes, &sidecar)? != manifest.object_sha256 {
            return Err(MirrorError::HashMismatch);
        }
        let backing = match placement {
            Placement::Ram => {
                let bytes = fs::read(object_path)?;
                if bytes.len() != object_bytes || hash_bytes(&bytes) != manifest.object_sha256 {
                    return Err(MirrorError::HashMismatch);
                }
                Backing::Ram(bytes)
            }
            Placement::File => {
                let digests = sidecar
                    .chunks_exact(32)
                    .map(|chunk| chunk.try_into().unwrap())
                    .collect();
                Backing::File { file, digests }
            }
        };
        Ok(Self {
            manifest,
            backing,
            object_bytes,
        })
    }

    /// Return the pinned generation identity.
    pub fn generation(&self) -> u64 {
        self.manifest.generation
    }

    /// Read a complete inclusive span of SQ8 pages from the pinned local
    /// placement. File reads verify every touched 4 KiB block, including
    /// bytes outside the page span. Both placements verify the requested page
    /// digests before the bytes can be ranked.
    pub fn read_verified_pages(
        &self,
        pages: &PageAuthority,
        first_page: usize,
        last_page: usize,
        max_bytes: usize,
    ) -> Result<LocalVerifiedRange, MirrorError> {
        if self.manifest.generation != pages.generation()
            || self.manifest.geometry.rows != pages.rows()
            || self.manifest.geometry.dimensions != pages.dimensions()
            || self.manifest.object_sha256 != pages.object_sha256()
        {
            return Err(MirrorError::InvalidManifest);
        }
        let range = pages
            .byte_range(first_page, last_page)
            .map_err(MirrorError::Page)?;
        let length = range.end - range.start;
        if length > max_bytes {
            return Err(MirrorError::InvalidPlane);
        }
        let (bytes, verified_block_reads) = match &self.backing {
            Backing::Ram(plane) => (plane[range.clone()].to_vec(), 0),
            Backing::File { file, digests } => {
                let mut output = vec![0u8; length];
                let first_block = range.start / BLOCK_BYTES;
                let last_block = (range.end - 1) / BLOCK_BYTES;
                for block in first_block..=last_block {
                    let block_start = block * BLOCK_BYTES;
                    let block_len = (self.object_bytes - block_start).min(BLOCK_BYTES);
                    let mut data = [0u8; BLOCK_BYTES];
                    read_exact_at(file, &mut data[..block_len], block_start)?;
                    if Sha256::digest(&data[..block_len]).as_slice() != digests[block] {
                        return Err(MirrorError::BlockCorrupt);
                    }
                    let copy_start = range.start.max(block_start);
                    let copy_end = range.end.min(block_start + block_len);
                    output[copy_start - range.start..copy_end - range.start]
                        .copy_from_slice(&data[copy_start - block_start..copy_end - block_start]);
                }
                (output, last_block - first_block + 1)
            }
        };
        pages
            .verify_payload(first_page, last_page, &bytes)
            .map_err(MirrorError::Page)?;
        Ok(LocalVerifiedRange {
            start: range.start,
            bytes,
            verified_block_reads,
        })
    }

    /// Score nominated rows after verifying any local file blocks read.
    pub fn score(
        &self,
        ordinals: &[usize],
        query: &[f32],
    ) -> Result<Vec<ScoredNominee>, MirrorError> {
        if ordinals.len() > self.manifest.max_nominees {
            return Err(MirrorError::Score(Sq8ScoreError::InvalidRoster));
        }
        match &self.backing {
            Backing::Ram(bytes) => Ok(score_nominees(
                bytes,
                self.manifest.geometry,
                ordinals,
                query,
                &self.manifest.low,
                &self.manifest.step,
            )?),
            Backing::File { file, digests } => {
                let geometry = self.manifest.geometry;
                let row_bytes = geometry.dimensions + 12;
                if query.len() != geometry.dimensions
                    || query.iter().any(|value| !value.is_finite())
                {
                    return Err(MirrorError::Score(Sq8ScoreError::InvalidQuery));
                }
                let mut seen = HashSet::with_capacity(ordinals.len());
                if ordinals
                    .iter()
                    .any(|ordinal| *ordinal >= geometry.rows || !seen.insert(*ordinal))
                {
                    return Err(MirrorError::Score(Sq8ScoreError::InvalidRoster));
                }
                if ordinals.is_empty() {
                    return Ok(Vec::new());
                }
                let mut selected = vec![
                    0u8;
                    ordinals
                        .len()
                        .checked_mul(row_bytes)
                        .ok_or(MirrorError::InvalidPlane)?
                ];
                let mut cached = HashMap::<usize, Vec<u8>>::new();
                for (index, &ordinal) in ordinals.iter().enumerate() {
                    let row_start = ordinal * row_bytes;
                    let row_end = row_start + row_bytes;
                    for block in row_start / BLOCK_BYTES..=(row_end - 1) / BLOCK_BYTES {
                        if let std::collections::hash_map::Entry::Vacant(slot) = cached.entry(block)
                        {
                            let block_start = block * BLOCK_BYTES;
                            let length = (self.object_bytes - block_start).min(BLOCK_BYTES);
                            let mut bytes = vec![0u8; length];
                            read_exact_at(file, &mut bytes, block_start)?;
                            if Sha256::digest(&bytes).as_slice() != digests[block] {
                                return Err(MirrorError::BlockCorrupt);
                            }
                            slot.insert(bytes);
                        }
                        let block_start = block * BLOCK_BYTES;
                        let copy_start = row_start.max(block_start);
                        let copy_end = row_end.min(block_start + cached[&block].len());
                        let destination = index * row_bytes + copy_start - row_start;
                        selected[destination..destination + copy_end - copy_start].copy_from_slice(
                            &cached[&block][copy_start - block_start..copy_end - block_start],
                        );
                    }
                }
                let selected_ordinals = (0..ordinals.len()).collect::<Vec<_>>();
                let mut scored = score_nominees(
                    &selected,
                    Sq8Geometry {
                        rows: ordinals.len(),
                        dimensions: geometry.dimensions,
                    },
                    &selected_ordinals,
                    query,
                    &self.manifest.low,
                    &self.manifest.step,
                )?;
                for entry in &mut scored {
                    entry.ordinal = ordinals[entry.ordinal];
                }
                Ok(scored)
            }
        }
    }
}

#[cfg(test)]
mod range_tests {
    use super::*;

    #[test]
    fn local_page_ranges_preserve_identity_caps_and_detect_late_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let object_path = directory.path().join("sq8.bin");
        let digest_path = directory.path().join("blocks.bin");
        let object = (0..513 * 13)
            .map(|position| (position % 251) as u8)
            .collect::<Vec<_>>();
        let block_digests = object
            .chunks(BLOCK_BYTES)
            .flat_map(|block| Sha256::digest(block).to_vec())
            .collect::<Vec<_>>();
        let page_digests = object
            .chunks(256 * 13)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        fs::write(&object_path, &object).unwrap();
        fs::write(&digest_path, &block_digests).unwrap();
        let object_sha = hash_bytes(&object);
        let mirror_manifest = MirrorManifest {
            format_version: 1,
            generation: 7,
            max_nominees: 1,
            geometry: Sq8Geometry {
                rows: 513,
                dimensions: 1,
            },
            object_sha256: object_sha.clone(),
            block_digest_sha256: hash_bytes(&block_digests),
            low: vec![0.0],
            step: vec![1.0],
        };
        let page_manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2",
            "generation":7,"rows":513,"dimensions":1,"page_rows":256,
            "object_sha256":object_sha,
            "page_digest_sha256":hash_bytes(&page_digests),
        }))
        .unwrap();
        let pages = PageAuthority::load(&page_manifest, &hash_bytes(&page_manifest), &page_digests)
            .unwrap();
        let ram = ExactSq8Mirror::open(
            &object_path,
            &digest_path,
            mirror_manifest.clone(),
            Placement::Ram,
        )
        .unwrap();
        let file =
            ExactSq8Mirror::open(&object_path, &digest_path, mirror_manifest, Placement::File)
                .unwrap();
        let expected = &object[256 * 13..512 * 13];
        let from_ram = ram
            .read_verified_pages(&pages, 1, 1, expected.len())
            .unwrap();
        let from_file = file
            .read_verified_pages(&pages, 1, 1, expected.len())
            .unwrap();
        assert_eq!(from_ram.start, 256 * 13);
        assert_eq!(from_ram.bytes, expected);
        assert_eq!(from_ram.verified_block_reads, 0);
        assert_eq!(from_file.bytes, expected);
        assert_eq!(from_file.verified_block_reads, 2);
        assert!(matches!(
            file.read_verified_pages(&pages, 1, 1, expected.len() - 1),
            Err(MirrorError::InvalidPlane)
        ));
        assert!(matches!(
            file.read_verified_pages(&pages, 2, 3, object.len()),
            Err(MirrorError::Page(PageError::InvalidRange))
        ));

        let mut changed = object.clone();
        changed[4097] ^= 1;
        fs::write(&object_path, changed).unwrap();
        assert!(matches!(
            file.read_verified_pages(&pages, 1, 1, expected.len()),
            Err(MirrorError::BlockCorrupt)
        ));
        assert_eq!(
            ram.read_verified_pages(&pages, 1, 1, expected.len())
                .unwrap()
                .bytes,
            expected
        );
    }
}
