//! Bounded SQ8 preparation from an immutable normalized source and approved order.
use crate::two_bit_source::SourceBuildError;
use sha2::{Digest, Sha256};
use std::{
    borrow::Cow,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

/// Calibration and identity of a completed physical SQ8 snapshot.
#[derive(Debug)]
pub struct Sq8SourceReceipt {
    /// Coordinate minimum from the source only.
    pub low: Vec<f32>,
    /// Positive coordinate quantization step.
    pub step: Vec<f32>,
    /// SHA256 of the completed SQ8 body.
    pub sha256: String,
}

pub(crate) fn cosine_vector(vector: &[f32]) -> Result<Cow<'_, [f32]>, SourceBuildError> {
    let squared = vector.iter().map(|&x| f64::from(x).powi(2)).sum::<f64>();
    if !squared.is_finite() || squared <= 0. {
        return Err(SourceBuildError::Invalid("cosine vector norm"));
    }
    Ok(if (squared - 1.).abs() <= 1e-6 {
        Cow::Borrowed(vector)
    } else {
        let norm = squared.sqrt();
        Cow::Owned(
            vector
                .iter()
                .map(|&x| (f64::from(x) / norm) as f32)
                .collect(),
        )
    })
}

/// Stream raw little-endian f32 source to a new cosine-normalized file.
/// Returns its SHA only after source authentication and output sync/install.
/// Source must remain immutable; payload admission excludes OS/runtime overhead.
/// An installation/sync error may leave unpublished output; discard on error.
pub fn normalize_source(
    source: &Path,
    source_sha256: &str,
    rows: usize,
    dimensions: usize,
    output: &Path,
    max_payload_bytes: usize,
) -> Result<String, SourceBuildError> {
    let bad = SourceBuildError::Invalid;
    let width = dimensions.checked_mul(4).ok_or(bad("source geometry"))?;
    let length = rows.checked_mul(width).ok_or(bad("source geometry"))?;
    let payload = dimensions
        .checked_mul(16)
        .and_then(|n| n.checked_add(135168))
        .ok_or(bad("normalization memory geometry"))?;
    if rows == 0
        || dimensions == 0
        || payload > max_payload_bytes
        || output.exists()
        || source_sha256.len() != 64
        || !source_sha256
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(bad("normalization inputs or memory budget"));
    }
    let input = File::open(source)?;
    if input.metadata()?.len() != length as u64 {
        return Err(bad("source length"));
    }
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut input = BufReader::with_capacity(65536, input);
    let mut row = vec![0_u8; width];
    let mut values = vec![0_f32; dimensions];
    let mut source_digest = Sha256::new();
    let mut normalized_digest = Sha256::new();
    {
        let mut writer = BufWriter::with_capacity(65536, temporary.as_file_mut());
        for _ in 0..rows {
            input.read_exact(&mut row)?;
            source_digest.update(&row);
            for (value, bytes) in values.iter_mut().zip(row.chunks_exact(4)) {
                *value = f32::from_le_bytes(bytes.try_into().unwrap());
            }
            let normalized = cosine_vector(&values)?;
            for (bytes, value) in row.chunks_exact_mut(4).zip(normalized.iter()) {
                bytes.copy_from_slice(&value.to_le_bytes());
            }
            writer.write_all(&row)?;
            normalized_digest.update(&row);
        }
        if input.read(&mut [0])? != 0 || format!("{:x}", source_digest.finalize()) != source_sha256
        {
            return Err(bad("source identity or length changed"));
        }
        writer.flush()?;
    }
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(output)
        .map_err(|e| SourceBuildError::Io(e.error))?;
    File::open(parent)?.sync_all()?;
    Ok(format!("{:x}", normalized_digest.finalize()))
}

/// Write existing ordinal-ID SQ8 records; never overwrite output.
/// Source is normalized little-endian f32. Order must be a full permutation.
/// Memory admission includes caller-owned order, buffers and a validation bitset.
/// Keep source immutable during both passes. A failed write may leave an
/// unpublished partial body; only a successful receipt authorizes publication.
/// Codes use f32 ties-to-even, norms use sequential f32 accumulation.
pub fn build_sq8_source(
    source: &Path,
    source_sha256: &str,
    dimensions: usize,
    order: &[u64],
    output: &Path,
    max_payload_bytes: usize,
) -> Result<Sq8SourceReceipt, SourceBuildError> {
    let bad = SourceBuildError::Invalid;
    let rows = order.len();
    let width = dimensions.checked_mul(4).ok_or(bad("SQ8 geometry"))?;
    let bytes = width.checked_mul(rows).ok_or(bad("SQ8 geometry"))?;
    let modeled = rows
        .checked_mul(8)
        .and_then(|n| n.checked_add(rows.div_ceil(8)))
        .and_then(|n| n.checked_add(dimensions.checked_mul(32)?))
        .and_then(|n| n.checked_add(131072))
        .ok_or(bad("SQ8 memory geometry"))?;
    if rows == 0
        || dimensions == 0
        || rows > i64::MAX as usize
        || modeled > max_payload_bytes
        || output.exists()
        || source_sha256.len() != 64
        || !source_sha256
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(bad("SQ8 inputs or memory budget"));
    }
    let mut seen = vec![0_u8; rows.div_ceil(8)];
    for &id in order {
        let id = usize::try_from(id).map_err(|_| bad("SQ8 ordinal"))?;
        if id >= rows || seen[id / 8] & (1 << (id % 8)) != 0 {
            return Err(bad("SQ8 permutation"));
        }
        seen[id / 8] |= 1 << (id % 8);
    }
    drop(seen);
    let input = File::open(source)?;
    if input.metadata()?.len() != bytes as u64 {
        return Err(bad("SQ8 source length"));
    }
    let mut input = BufReader::with_capacity(65536, input);
    let mut row = vec![0_u8; width];
    let mut low = vec![f32::INFINITY; dimensions];
    let mut high = vec![f32::NEG_INFINITY; dimensions];
    let mut digest = Sha256::new();
    for _ in 0..rows {
        input.read_exact(&mut row)?;
        digest.update(&row);
        let mut norm = 0_f64;
        for (d, bytes) in row.chunks_exact(4).enumerate() {
            let value = f32::from_le_bytes(bytes.try_into().unwrap());
            if !value.is_finite() {
                return Err(bad("SQ8 nonfinite source"));
            }
            norm += f64::from(value) * f64::from(value);
            low[d] = low[d].min(value);
            high[d] = high[d].max(value);
        }
        if (norm.sqrt() - 1.).abs() > 1e-4 {
            return Err(bad("SQ8 source unit norm"));
        }
    }
    if format!("{:x}", digest.finalize()) != source_sha256 {
        return Err(bad("SQ8 source identity"));
    }
    let span = high
        .iter()
        .zip(&low)
        .map(|(hi, lo)| (hi - lo).max(1e-12))
        .collect::<Vec<_>>();
    let step = span.iter().map(|v| v / 255.).collect::<Vec<_>>();
    let mut codes = vec![0_u8; dimensions];
    let mut output = BufWriter::with_capacity(
        65536,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?,
    );
    let mut digest = Sha256::new();
    // ponytail: one seek per ordinal; batch adjacent source reads if build I/O dominates.
    for &id in order {
        input.seek(SeekFrom::Start(id * width as u64))?;
        input.read_exact(&mut row)?;
        let mut norm = 0_f32;
        for (d, bytes) in row.chunks_exact(4).enumerate() {
            let value = f32::from_le_bytes(bytes.try_into().unwrap());
            if !value.is_finite() {
                return Err(bad("SQ8 source changed"));
            }
            codes[d] = ((value - low[d]) / span[d] * 255.)
                .round_ties_even()
                .clamp(0., 255.) as u8;
            let decoded = low[d] + f32::from(codes[d]) * step[d];
            norm += decoded * decoded;
        }
        if !norm.is_finite() || norm <= 0. {
            return Err(bad("SQ8 reconstructed norm"));
        }
        let id_bytes = (id as i64).to_le_bytes();
        let norm_bytes = norm.to_le_bytes();
        for part in [&id_bytes[..], &norm_bytes[..], &codes[..]] {
            output.write_all(part)?;
            digest.update(part);
        }
    }
    output.flush()?;
    output.get_ref().sync_all()?;
    Ok(Sq8SourceReceipt {
        low,
        step,
        sha256: format!("{:x}", digest.finalize()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    #[test]
    fn normalization_authentication_scale_and_no_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("raw");
        let output = dir.path().join("unit");
        let values = [2_f32, 1., 5e29, 2.5e29, 5e-31, 2.5e-31, 1., 0.];
        let bytes = values
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        std::fs::write(&source, &bytes).unwrap();
        let sha = format!("{:x}", Sha256::digest(&bytes));
        assert!(normalize_source(&source, &sha, 4, 2, &output, 0).is_err());
        assert!(normalize_source(&source, &"0".repeat(64), 4, 2, &output, 200000).is_err());
        assert!(!output.exists());
        let normalized_sha = normalize_source(&source, &sha, 4, 2, &output, 200000).unwrap();
        let body = std::fs::read(&output).unwrap();
        let values = body
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let expected = [(2_f64 / 5_f64.sqrt()) as f32, (1_f64 / 5_f64.sqrt()) as f32];
        for row in values[..6].chunks_exact(2) {
            for (a, b) in row.iter().zip(expected) {
                assert!((*a - b).abs() <= 1e-7);
            }
        }
        assert_eq!(&values[6..], &[1., 0.]);
        assert_eq!(normalized_sha, format!("{:x}", Sha256::digest(&body)));
        assert!(normalize_source(&source, &sha, 4, 2, &output, 200000).is_err());
        assert_eq!(std::fs::read(&output).unwrap(), body);
        for bad in [[0_f32, 0.], [f32::NAN, 1.], [f32::INFINITY, 1.]] {
            let body = bad
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>();
            std::fs::write(&source, &body).unwrap();
            let sha = format!("{:x}", Sha256::digest(&body));
            let rejected = dir.path().join("rejected");
            assert!(normalize_source(&source, &sha, 1, 2, &rejected, 200000).is_err());
            assert!(!rejected.exists());
        }
    }

    #[test]
    fn source_encoding_order_identity_and_admission() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let bytes = [1_f32, 0., 0., 1.]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        std::fs::write(&source, &bytes).unwrap();
        let sha = format!("{:x}", Sha256::digest(&bytes));
        let output = dir.path().join("sq8");
        assert!(build_sq8_source(&source, &sha, 2, &[1, 1], &output, 200000).is_err());
        assert!(build_sq8_source(&source, &sha, 2, &[1, 0], &output, 0).is_err());
        assert!(build_sq8_source(&source, &"0".repeat(64), 2, &[1, 0], &output, 200000).is_err());
        assert!(!output.exists());
        let receipt = build_sq8_source(&source, &sha, 2, &[1, 0], &output, 200000).unwrap();
        let mut expected = Vec::new();
        for (id, codes) in [(1_i64, [0_u8, 255]), (0, [255, 0])] {
            expected.extend_from_slice(&id.to_le_bytes());
            expected.extend_from_slice(&1_f32.to_le_bytes());
            expected.extend_from_slice(&codes);
        }
        assert_eq!(std::fs::read(&output).unwrap(), expected);
        assert_eq!(receipt.low, [0., 0.]);
        assert_eq!(receipt.step, [1_f32 / 255.; 2]);
        assert_eq!(receipt.sha256, format!("{:x}", Sha256::digest(&expected)));
        assert!(build_sq8_source(&source, &sha, 2, &[1, 0], &output, 200000).is_err());
        // Independent NumPy 2.3.3 rint/einsum reference: half-code rounds to
        // even128, negative minimum and zero-span coordinate remain valid.
        let ties = dir.path().join("ties");
        let ties_bytes = [
            1_f32, 0., 0., 0., -1., 0., 0., 0., 0., 1., 0., 0., 0., 0., 1., 0.,
        ]
        .into_iter()
        .flat_map(f32::to_le_bytes)
        .collect::<Vec<_>>();
        std::fs::write(&ties, &ties_bytes).unwrap();
        let ties_sha = format!("{:x}", Sha256::digest(&ties_bytes));
        let ties_output = dir.path().join("ties-sq8");
        let ties_receipt =
            build_sq8_source(&ties, &ties_sha, 4, &[0, 1, 2, 3], &ties_output, 200000).unwrap();
        assert!(ties_receipt.step[3] > 0.);
        let ties_body = std::fs::read(ties_output).unwrap();
        for (row, codes, norm) in [
            (0, [255, 0, 0, 0], 0x3f800000_u32),
            (1, [0, 0, 0, 0], 0x3f800000),
            (2, [128, 255, 0, 0], 0x3f800081),
            (3, [128, 0, 255, 0], 0x3f800081),
        ] {
            assert_eq!(&ties_body[row * 16 + 12..row * 16 + 16], &codes);
            assert_eq!(&ties_body[row * 16 + 8..row * 16 + 12], &norm.to_le_bytes());
        }
        std::fs::write(&source, [0_u8; 16]).unwrap();
        let zero_sha = format!("{:x}", Sha256::digest([0_u8; 16]));
        assert!(build_sq8_source(
            &source,
            &zero_sha,
            2,
            &[1, 0],
            &dir.path().join("bad"),
            200000
        )
        .is_err());
    }
}
