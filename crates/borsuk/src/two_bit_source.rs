//! Bounded, query-blind construction of rotated two-bit nomination metadata.
use crate::rotated_two_bit::{PreparedTwoBit, RotatedTwoBitCodec, TwoBitError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Source geometry, identity, codec or I/O failure. Failed builds publish no manifest.
#[derive(Debug)]
pub enum SourceBuildError {
    /// Filesystem error.
    Io(io::Error),
    /// Invalid identity, geometry, permutation or memory admission.
    Invalid(&'static str),
    /// Invalid source vector or codec geometry.
    Codec(TwoBitError),
}
impl std::fmt::Display for SourceBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "two-bit source: {self:?}")
    }
}
impl std::error::Error for SourceBuildError {}
impl From<io::Error> for SourceBuildError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<TwoBitError> for SourceBuildError {
    fn from(value: TwoBitError) -> Self {
        Self::Codec(value)
    }
}

/// Caller-owned immutable source snapshots, with ordinal IDs in the SQ8 object.
/// The builder accepts no queries or truth. Keep these files immutable during build.
pub struct TwoBitSource<'a> {
    /// Little-endian f32 source rows in source ordinal order.
    pub raw: &'a Path,
    /// Trusted SHA256 of the raw source.
    pub raw_sha256: &'a str,
    /// SQ8 records in physical order: i64 ID, f32 squared norm, dimension bytes.
    pub sq8: &'a Path,
    /// Trusted SHA256 of that physical-order object.
    pub sq8_sha256: &'a str,
    /// Exact source row count.
    pub rows: usize,
    /// Original source dimensions, excluding codec padding.
    pub dimensions: usize,
}

/// Final source-only manifest; a generation must bind its hash before serving.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePlaneReceipt {
    /// Format marker; historical research code planes have a different scalar.
    pub schema: String,
    /// Source row count.
    pub rows: usize,
    /// Source dimensions.
    pub dimensions: usize,
    /// Fixed source-only rotation seed.
    pub seed: u32,
    /// Encoded bytes per physical row.
    pub record_bytes: usize,
    /// Raw source identity.
    pub source_sha256: String,
    /// Physical SQ8 order and payload identity.
    pub sq8_sha256: String,
    /// Digest of little-endian f32 `mean.bin`.
    pub mean_sha256: String,
    /// Digest of Rust v1 `records.bin`.
    pub records_sha256: String,
    /// Always false: construction accepts no query or truth inputs.
    pub query_or_truth_used: bool,
}
fn new_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|x| x.is_ascii_hexdigit())
}

impl TwoBitSource<'_> {
    /// Build into a new directory; never replace an artifact. Payload memory is
    /// admitted before allocations: IDs/bitset O(rows), codec/buffers O(dimensions).
    /// Allocator/runtime overhead and OS page cache are outside this estimate.
    /// The final manifest is written last. An I/O/codec failure may leave an
    /// unpublished directory for caller cleanup; it is never safe to serve it.
    pub fn build(
        &self,
        output: &Path,
        max_memory_bytes: usize,
    ) -> Result<SourcePlaneReceipt, SourceBuildError> {
        let bad = SourceBuildError::Invalid;
        if self.rows == 0
            || self.dimensions == 0
            || !valid_digest(self.raw_sha256)
            || !valid_digest(self.sq8_sha256)
            || output.exists()
        {
            return Err(bad("input geometry, digest or output"));
        }
        let raw_width = self
            .dimensions
            .checked_mul(4)
            .ok_or(bad("geometry overflow"))?;
        let sq8_width = self
            .dimensions
            .checked_add(12)
            .ok_or(bad("geometry overflow"))?;
        let raw_bytes = self
            .rows
            .checked_mul(raw_width)
            .ok_or(bad("geometry overflow"))?;
        let sq8_bytes = self
            .rows
            .checked_mul(sq8_width)
            .ok_or(bad("geometry overflow"))?;
        let bit_bytes = self.rows.div_ceil(8);
        let required = self
            .rows
            .checked_mul(8)
            .and_then(|n| n.checked_add(bit_bytes))
            .and_then(|n| {
                self.dimensions
                    .checked_mul(128)
                    .and_then(|d| n.checked_add(d))
            })
            .and_then(|n| n.checked_add(256 * 1024))
            .ok_or(bad("memory overflow"))?;
        if required > max_memory_bytes {
            return Err(bad("memory budget"));
        }
        let raw_file = File::open(self.raw)?;
        let sq8_file = File::open(self.sq8)?;
        if raw_file.metadata()?.len() != raw_bytes as u64
            || sq8_file.metadata()?.len() != sq8_bytes as u64
        {
            return Err(bad("file geometry"));
        }
        let mut raw_reader = BufReader::with_capacity(64 * 1024, raw_file);
        let mut raw_buffer = vec![0_u8; raw_width];
        let mut sums = vec![0.0_f64; self.dimensions];
        let mut raw_digest = Sha256::new();
        for _ in 0..self.rows {
            raw_reader.read_exact(&mut raw_buffer)?;
            raw_digest.update(&raw_buffer);
            let mut norm = 0.0;
            for (sum, bytes) in sums.iter_mut().zip(raw_buffer.chunks_exact(4)) {
                let value = f64::from(f32::from_le_bytes(bytes.try_into().unwrap()));
                if !value.is_finite() {
                    return Err(bad("nonfinite source"));
                }
                *sum += value;
                norm += value * value;
            }
            if norm <= 0.0 || !norm.is_finite() {
                return Err(bad("zero or invalid source norm"));
            }
        }
        if format!("{:x}", raw_digest.finalize()) != self.raw_sha256 {
            return Err(bad("raw source digest"));
        }
        let mut raw_file = raw_reader.into_inner();
        let mean = sums
            .into_iter()
            .map(|sum| (sum / self.rows as f64) as f32)
            .collect::<Vec<_>>();
        let codec = RotatedTwoBitCodec::new(&mean, 20260923)?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(self.rows)
            .map_err(|_| bad("ID allocation"))?;
        let mut seen = Vec::new();
        seen.try_reserve_exact(bit_bytes)
            .map_err(|_| bad("ID bitset allocation"))?;
        seen.resize(bit_bytes, 0_u8);
        let mut sq8_reader = BufReader::with_capacity(64 * 1024, sq8_file);
        let mut sq8_buffer = vec![0_u8; sq8_width];
        let mut sq8_digest = Sha256::new();
        for _ in 0..self.rows {
            sq8_reader.read_exact(&mut sq8_buffer)?;
            sq8_digest.update(&sq8_buffer);
            let id = usize::try_from(i64::from_le_bytes(sq8_buffer[..8].try_into().unwrap()))
                .map_err(|_| bad("negative source ID"))?;
            let norm = f32::from_le_bytes(sq8_buffer[8..12].try_into().unwrap());
            if id >= self.rows || !norm.is_finite() || norm <= 0.0 {
                return Err(bad("source ID or SQ8 norm"));
            }
            let bit = 1 << (id % 8);
            if seen[id / 8] & bit != 0 {
                return Err(bad("duplicate source ID"));
            }
            seen[id / 8] |= bit;
            ids.push(id);
        }
        if format!("{:x}", sq8_digest.finalize()) != self.sq8_sha256 {
            return Err(bad("SQ8 source digest"));
        }
        drop(sq8_reader);
        drop(sq8_buffer);
        drop(seen);
        fs::create_dir(output)?;
        let mean_bytes = mean
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        let mut mean_file = new_file(&output.join("mean.bin"))?;
        mean_file.write_all(&mean_bytes)?;
        mean_file.sync_all()?;
        let mut records =
            BufWriter::with_capacity(64 * 1024, new_file(&output.join("records.bin"))?);
        let mut record_digest = Sha256::new();
        let mut row = vec![0.0_f32; self.dimensions];
        for id in ids {
            // ponytail: random raw-row seeks; use an externally reordered source stream if large builds become I/O-bound.
            raw_file.seek(SeekFrom::Start((id * raw_width) as u64))?;
            raw_file.read_exact(&mut raw_buffer)?;
            for (value, bytes) in row.iter_mut().zip(raw_buffer.chunks_exact(4)) {
                *value = f32::from_le_bytes(bytes.try_into().unwrap());
            }
            let encoded = codec.encode(&row)?;
            record_digest.update(&encoded);
            records.write_all(&encoded)?;
        }
        records.flush()?;
        records.get_ref().sync_all()?;
        let receipt = SourcePlaneReceipt {
            schema: "borsuk-two-bit-plane-v1".into(),
            rows: self.rows,
            dimensions: self.dimensions,
            seed: 20260923,
            record_bytes: codec.record_bytes(),
            source_sha256: self.raw_sha256.to_owned(),
            sq8_sha256: self.sq8_sha256.to_owned(),
            mean_sha256: format!("{:x}", Sha256::digest(&mean_bytes)),
            records_sha256: format!("{:x}", record_digest.finalize()),
            query_or_truth_used: false,
        };
        let mut manifest = new_file(&output.join("manifest.pending"))?;
        let manifest_body = serde_json::to_vec(&receipt).map_err(|_| bad("manifest encoding"))?;
        manifest.write_all(&manifest_body)?;
        manifest.write_all(b"\n")?;
        manifest.sync_all()?;
        File::open(output)?.sync_all()?;
        // Publish only after every fallible write/sync. A crash can lose this
        // staging rename; generation publication must bind the finished hash.
        fs::rename(
            output.join("manifest.pending"),
            output.join("manifest.json"),
        )?;
        Ok(receipt)
    }
}

/// Authenticated resident nomination metadata. Source vectors/SQ8 remain on demand.
/// The caller must bind the manifest and SQ8 digest to its trusted generation root.
pub struct TwoBitPlane {
    receipt: SourcePlaneReceipt,
    codec: RotatedTwoBitCodec,
    records: Vec<u8>,
}

// Exact allocation and an extra-byte probe bound concurrent file growth as well.
pub(crate) fn read_authenticated(
    path: &Path,
    size: usize,
    digest: &str,
) -> Result<Vec<u8>, SourceBuildError> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != size as u64 {
        return Err(SourceBuildError::Invalid("artifact length"));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| SourceBuildError::Invalid("allocation"))?;
    bytes.resize(size, 0);
    file.read_exact(&mut bytes)?;
    if file.read(&mut [0])? != 0 || format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err(SourceBuildError::Invalid("artifact identity"));
    }
    Ok(bytes)
}

impl TwoBitPlane {
    /// Reload immutable metadata under a payload memory cap. Includes manifest,
    /// records, mean and codec working storage; excludes runtime/allocator and OS
    /// page cache. Query scratch is separately admitted per concurrent query.
    pub fn open(
        root: &Path,
        trusted_manifest_sha256: &str,
        expected_sq8_sha256: &str,
        max_memory_bytes: usize,
    ) -> Result<Self, SourceBuildError> {
        let bad = SourceBuildError::Invalid;
        const MANIFEST_CAP: usize = 64 * 1024;
        if !valid_digest(trusted_manifest_sha256)
            || !valid_digest(expected_sq8_sha256)
            || max_memory_bytes < MANIFEST_CAP * 2
        {
            return Err(bad("identity or memory budget"));
        }
        let manifest_path = root.join("manifest.json");
        let size = usize::try_from(fs::metadata(&manifest_path)?.len())
            .map_err(|_| bad("manifest size"))?;
        if size == 0 || size > MANIFEST_CAP {
            return Err(bad("manifest size"));
        }
        let body = read_authenticated(&manifest_path, size, trusted_manifest_sha256)?;
        let receipt: SourcePlaneReceipt =
            serde_json::from_slice(&body).map_err(|_| bad("manifest schema"))?;
        if receipt.schema != "borsuk-two-bit-plane-v1"
            || receipt.seed != 20260923
            || receipt.rows == 0
            || receipt.query_or_truth_used
            || receipt.sq8_sha256 != expected_sq8_sha256
            || [
                &receipt.source_sha256,
                &receipt.sq8_sha256,
                &receipt.mean_sha256,
                &receipt.records_sha256,
            ]
            .iter()
            .any(|s| !valid_digest(s))
        {
            return Err(bad("manifest identity"));
        }
        let padded = RotatedTwoBitCodec::padded_dimensions(receipt.dimensions)?;
        if receipt.record_bytes != padded.div_ceil(4) + 8 {
            return Err(bad("record geometry"));
        }
        let record_size = receipt
            .rows
            .checked_mul(receipt.record_bytes)
            .ok_or(bad("geometry overflow"))?;
        let mean_size = receipt
            .dimensions
            .checked_mul(4)
            .ok_or(bad("geometry overflow"))?;
        let required = padded
            .checked_mul(128)
            .and_then(|n| n.checked_add(record_size))
            .and_then(|n| n.checked_add(MANIFEST_CAP * 2))
            .ok_or(bad("memory overflow"))?;
        if required > max_memory_bytes {
            return Err(bad("memory budget"));
        }
        let mean_bytes =
            read_authenticated(&root.join("mean.bin"), mean_size, &receipt.mean_sha256)?;
        let mean = mean_bytes
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let codec = RotatedTwoBitCodec::new(&mean, receipt.seed)?;
        let records = read_authenticated(
            &root.join("records.bin"),
            record_size,
            &receipt.records_sha256,
        )?;
        // Scalars are checked by the shared scorer before any value is returned.
        Ok(Self {
            receipt,
            codec,
            records,
        })
    }

    /// Generation-bound source geometry and artifact identities.
    pub fn receipt(&self) -> &SourcePlaneReceipt {
        &self.receipt
    }

    /// Borrow a physical row; no allocation or vector hydration.
    pub fn record(&self, physical_row: usize) -> Option<&[u8]> {
        if physical_row >= self.receipt.rows {
            return None;
        }
        let start = physical_row * self.receipt.record_bytes;
        Some(&self.records[start..start + self.receipt.record_bytes])
    }

    /// Prepare one query using the existing codec and its explicit scratch cap.
    pub fn prepare_query(
        &self,
        query: &[f32],
        max_scratch_bytes: usize,
    ) -> Result<PreparedTwoBit, TwoBitError> {
        self.codec.prepare_query(query, max_scratch_bytes)
    }
}
