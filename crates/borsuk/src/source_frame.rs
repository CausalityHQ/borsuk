//! Lossless independent BSF1 record frames and the FramedRecordsV1 directory.
use std::io::{Error, ErrorKind, Result};
pub const FRAME_ROWS: usize = 256;
pub const HEADER_BYTES: usize = 25;
const MAGIC: &[u8; 4] = b"BSF1";
/// Caps are checked before codec buffers are allocated. Decoding rejects Zstd
/// windows above 1 MiB; this is not a context allocation or total RSS allowance.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_decoded_bytes: usize,
}
fn invalid() -> Error {
    Error::new(
        ErrorKind::InvalidData,
        "invalid or over-budget SOURCE frame",
    )
}
fn geometry(rows: usize, width: usize, limits: Limits) -> Result<usize> {
    if rows == 0 || rows > FRAME_ROWS || width == 0 {
        return Err(invalid());
    }
    rows.checked_mul(width)
        .filter(|&n| n <= limits.max_decoded_bytes && n <= (1 << 20))
        .ok_or_else(invalid)
}
/// Encode original record bytes, with raw original fallback when Zstd is not smaller.
pub fn encode(rows: usize, width: usize, original: &[u8], limits: Limits) -> Result<Vec<u8>> {
    let n = geometry(rows, width, limits)?;
    if original.len() != n {
        return Err(invalid());
    }
    let mut transposed = vec![0; n];
    for col in 0..width {
        for row in 0..rows {
            transposed[col * rows + row] = original[row * width + col];
        }
    }
    let compressed = zstd::bulk::compress(&transposed, 1)?;
    let (mode, payload) = if compressed.len() < n {
        (1u8, compressed.as_slice())
    } else {
        (0u8, original)
    };
    let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len());
    frame.extend_from_slice(MAGIC);
    frame.push(mode);
    frame.extend_from_slice(&(rows as u32).to_le_bytes());
    frame.extend_from_slice(&(width as u64).to_le_bytes());
    frame.extend_from_slice(&(n as u64).to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}
/// Decode exactly one frame with expected geometry and an explicit output cap.
/// Callers must authenticate decoded bytes; this codec supplies no integrity proof.
pub fn decode(frame: &[u8], rows: usize, width: usize, limits: Limits) -> Result<Vec<u8>> {
    let n = geometry(rows, width, limits)?;
    if frame.len() < HEADER_BYTES || frame.len() > HEADER_BYTES + n || &frame[..4] != MAGIC {
        return Err(invalid());
    }
    let field = |a: usize, b: usize| -> Result<usize> {
        usize::try_from(u64::from_le_bytes(
            frame[a..b].try_into().map_err(|_| invalid())?,
        ))
        .map_err(|_| invalid())
    };
    if u32::from_le_bytes(frame[5..9].try_into().unwrap()) as usize != rows
        || field(9, 17)? != width
        || field(17, 25)? != n
    {
        return Err(invalid());
    }
    let payload = &frame[HEADER_BYTES..];
    match frame[4] {
        0 if payload.len() == n => Ok(payload.to_vec()),
        1 if payload.len() < n => {
            // Reject skippable frames, extra frames and trailing bytes before decoding.
            if payload.len() < 6
                || payload[..4] != [0x28, 0xb5, 0x2f, 0xfd]
                || zstd::zstd_safe::find_frame_compressed_size(payload).map_err(|_| invalid())?
                    != payload.len()
            {
                return Err(invalid());
            }
            if zstd::zstd_safe::get_frame_content_size(payload).map_err(|_| invalid())?
                != Some(n as u64)
            {
                return Err(invalid());
            }
            if payload[4] & 0x20 == 0 {
                let descriptor = payload[5] as u32;
                let base = 1u64
                    .checked_shl(10 + (descriptor >> 3))
                    .ok_or_else(invalid)?;
                let window = base + (base / 8) * u64::from(descriptor & 7);
                if window > 1 << 20 {
                    return Err(invalid());
                }
            }
            let mut decoder = zstd::bulk::Decompressor::new()?;
            decoder.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(20))?;
            let mut transposed = vec![0; n];
            if decoder.decompress_to_buffer(payload, &mut transposed[..])? != n {
                return Err(invalid());
            }
            let mut original = vec![0; n];
            for col in 0..width {
                for row in 0..rows {
                    original[row * width + col] = transposed[col * rows + row];
                }
            }
            Ok(original)
        }
        _ => Err(invalid()),
    }
}
// Conservative allowances for the linked Zstd bulk API, independent of its window cap.
// The qualification RSS gate remains authoritative; a codec upgrade must requalify these.
pub(crate) const DECODER_CONTEXT_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const ENCODER_CONTEXT_BYTES: usize = 32 * 1024 * 1024;

pub(crate) fn directory_bytes(rows: usize) -> Result<usize> {
    rows.div_ceil(FRAME_ROWS)
        .checked_add(1)
        .and_then(|n| n.checked_mul(8))
        .ok_or_else(invalid)
}

pub(crate) fn frame_bytes(rows: usize, width: usize) -> Result<usize> {
    if rows == 0 {
        return if width > 0 { Ok(0) } else { Err(invalid()) };
    }
    geometry(
        rows.min(FRAME_ROWS),
        width,
        Limits {
            max_decoded_bytes: 1 << 20,
        },
    )
}

/// Offsets have already been authenticated against the generation root.
pub(crate) struct Directory {
    offsets: Vec<usize>,
    rows: usize,
    width: usize,
}
impl Directory {
    pub(crate) fn open(
        encoded: Vec<u8>,
        rows: usize,
        width: usize,
        blob_bytes: usize,
        max_memory_bytes: usize,
    ) -> Result<Self> {
        let length = directory_bytes(rows)?;
        frame_bytes(rows, width)?;
        rows.checked_mul(width).ok_or_else(invalid)?;
        let charge = length
            .checked_mul(2)
            .and_then(|n| n.checked_add(128))
            .ok_or_else(invalid)?;
        if encoded.len() != length || encoded.capacity() > length || charge > max_memory_bytes {
            return Err(invalid());
        }
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(length / 8)
            .map_err(|_| invalid())?;
        for bytes in encoded.chunks_exact(8) {
            offsets.push(
                usize::try_from(u64::from_le_bytes(bytes.try_into().unwrap()))
                    .map_err(|_| invalid())?,
            );
        }
        if offsets
            .capacity()
            .checked_mul(std::mem::size_of::<usize>())
            .is_none_or(|n| n > length)
            || offsets.first() != Some(&0)
            || offsets.last() != Some(&blob_bytes)
        {
            return Err(invalid());
        }
        for (i, pair) in offsets.windows(2).enumerate() {
            let rows = (rows - i * FRAME_ROWS).min(FRAME_ROWS);
            let maximum = HEADER_BYTES
                .checked_add(rows.checked_mul(width).ok_or_else(invalid)?)
                .ok_or_else(invalid)?;
            if pair[1] <= pair[0] || !(HEADER_BYTES + 1..=maximum).contains(&(pair[1] - pair[0])) {
                return Err(invalid());
            }
        }
        Ok(Self {
            offsets,
            rows,
            width,
        })
    }
    pub(crate) fn frames(&self) -> usize {
        self.offsets.len() - 1
    }
    pub(crate) fn blob_bytes(&self) -> usize {
        *self.offsets.last().unwrap()
    }
    pub(crate) fn physical_range(
        &self,
        first: usize,
        last: usize,
    ) -> Result<std::ops::Range<usize>> {
        if first > last || last >= self.frames() {
            return Err(invalid());
        }
        Ok(self.offsets[first]..self.offsets[last + 1])
    }
    pub(crate) fn decoded_range(
        &self,
        first: usize,
        last: usize,
    ) -> Result<std::ops::Range<usize>> {
        self.physical_range(first, last)?;
        Ok(first * FRAME_ROWS * self.width..((last + 1) * FRAME_ROWS).min(self.rows) * self.width)
    }
    pub(crate) fn decode_range(
        &self,
        first: usize,
        last: usize,
        body: &[u8],
        authority: &crate::sq8_page_authority::PageAuthority,
    ) -> Result<crate::sq8_s3_range::VerifiedRange> {
        let physical = self.physical_range(first, last)?;
        let logical = self.decoded_range(first, last)?;
        if body.len() != physical.len()
            || authority.object_bytes() != self.rows * self.width
            || authority.page_rows() != 32
        {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(logical.len())
            .map_err(|_| invalid())?;
        if bytes.capacity() > logical.len() {
            return Err(invalid());
        }
        for i in first..=last {
            let range = self.physical_range(i, i)?;
            let rows = (self.rows - i * FRAME_ROWS).min(FRAME_ROWS);
            let decoded = decode(
                &body[range.start - physical.start..range.end - physical.start],
                rows,
                self.width,
                Limits {
                    max_decoded_bytes: 1 << 20,
                },
            )?;
            authority
                .verify_payload(i * 8, (i * FRAME_ROWS + rows).div_ceil(32) - 1, &decoded)
                .map_err(|_| invalid())?;
            bytes.extend_from_slice(&decoded);
        }
        Ok(crate::sq8_s3_range::VerifiedRange {
            start: logical.start,
            bytes: bytes.into(),
        })
    }
    pub(crate) fn decode_physical(
        &self,
        offset: usize,
        body: &[u8],
        authority: &crate::sq8_page_authority::PageAuthority,
    ) -> Result<crate::sq8_s3_range::VerifiedRange> {
        let first = self.offsets.binary_search(&offset).map_err(|_| invalid())?;
        let end = offset.checked_add(body.len()).ok_or_else(invalid)?;
        let after = self.offsets.binary_search(&end).map_err(|_| invalid())?;
        let last = after.checked_sub(1).ok_or_else(invalid)?;
        self.decode_range(first, last, body, authority)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> Limits {
        Limits {
            max_decoded_bytes: 1 << 20,
        }
    }
    #[test]
    fn source_frame_geometry_and_exact_bytes() {
        for dimensions in [1usize, 3, 257, 768, 1024] {
            let tail = dimensions % 256;
            let padded = dimensions / 256 * 256
                + if tail == 0 {
                    0
                } else {
                    tail.next_power_of_two()
                };
            let width = padded.div_ceil(4) + 8;
            for rows in [1, 32, 64, 255, 256] {
                for varied in [false, true] {
                    let input: Vec<u8> = (0..rows * width)
                        .map(|i| {
                            if varied {
                                (i.wrapping_mul(137) ^ (i / 17)) as u8
                            } else {
                                7
                            }
                        })
                        .collect();
                    let frame = encode(rows, width, &input, limits()).unwrap();
                    assert_eq!(frame, encode(rows, width, &input, limits()).unwrap());
                    assert_eq!(input, decode(&frame, rows, width, limits()).unwrap());
                }
            }
        }
    }
    #[test]
    fn source_frame_raw_fallback_and_refusals() {
        let mut state = 9u64;
        let input: Vec<u8> = (0..256 * 264)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();
        let frame = encode(256, 264, &input, limits()).unwrap();
        assert_eq!(frame[4], 0);
        assert_eq!(decode(&frame, 256, 264, limits()).unwrap(), input);
        for cut in [0, 1, 24, 25, frame.len() - 1] {
            assert!(decode(&frame[..cut], 256, 264, limits()).is_err());
        }
        let mut extra = frame.clone();
        extra.push(0);
        assert!(decode(&extra, 256, 264, limits()).is_err());
        assert!(decode(&frame, 255, 264, limits()).is_err());
        assert!(
            decode(
                &frame,
                256,
                264,
                Limits {
                    max_decoded_bytes: 1
                }
            )
            .is_err()
        );
        assert!(encode(256, usize::MAX, &[], limits()).is_err());
        assert!(encode(257, 1, &[0; 257], limits()).is_err());
        for offset in [0, 4, 5, 9, 17] {
            let mut bad = frame.clone();
            bad[offset] = 255;
            assert!(decode(&bad, 256, 264, limits()).is_err());
        }
    }
    #[test]
    fn source_frame_compressed_corruption_and_window() {
        let frame = encode(256, 264, &vec![0; 256 * 264], limits()).unwrap();
        assert_eq!(frame[4], 1);
        for cut in 25..frame.len() {
            assert!(decode(&frame[..cut], 256, 264, limits()).is_err());
        }
        let mut joined = frame.clone();
        joined.extend_from_slice(&frame[25..]);
        assert!(decode(&joined, 256, 264, limits()).is_err());
        let mut trailing = frame.clone();
        trailing.push(0);
        assert!(decode(&trailing, 256, 264, limits()).is_err());
        let mut bad = frame.clone();
        bad[25] = 0;
        assert!(decode(&bad, 256, 264, limits()).is_err());
        // A streaming frame claiming a 128 MiB window must fail before allocation.
        let mut window = frame;
        window[29] &= !0x20;
        window.insert(30, 0x88);
        assert!(decode(&window, 256, 264, limits()).is_err());
    }
    #[test]
    fn framed_source_directory_caps_corruption_and_bridge_authentication() {
        use crate::sq8_page_authority::PageAuthority;
        use sha2::{Digest, Sha256};
        let hash = |b: &[u8]| format!("{:x}", Sha256::digest(b));
        for dimensions in [1, 257, 1024] {
            let width = crate::rotated_two_bit::RotatedTwoBitCodec::padded_dimensions(dimensions)
                .unwrap()
                .div_ceil(4)
                + 8;
            for rows in [1usize, 31, 32, 33, 255, 256, 257, 513, 769] {
                let original = vec![7u8; rows * width];
                let digests = original
                    .chunks(32 * width)
                    .flat_map(|b| Sha256::digest(b).to_vec())
                    .collect::<Vec<_>>();
                let manifest = serde_json::to_vec(&serde_json::json!({
                    "schema":"borsuk-two-bit-plane-v3","rows":rows,"dimensions":dimensions,
                    "seed":20260923,"record_bytes":width,"page_rows":32,
                    "source_sha256":"0".repeat(64),"sq8_sha256":"1".repeat(64),
                    "source_order_sha256":"2".repeat(64),"mean_sha256":"3".repeat(64),
                    "records_sha256":hash(&original),"page_digest_sha256":hash(&digests),
                    "query_or_truth_used":false
                }))
                .unwrap();
                let authority =
                    PageAuthority::load_two_bit(&manifest, &hash(&manifest), 2, &digests).unwrap();
                let mut blob = Vec::new();
                let mut offsets = 0u64.to_le_bytes().to_vec();
                for body in original.chunks(256 * width) {
                    blob.extend(encode(body.len() / width, width, body, limits()).unwrap());
                    offsets.extend_from_slice(&(blob.len() as u64).to_le_bytes());
                }
                let cap = offsets.len() * 2 + 128;
                assert!(
                    Directory::open(offsets.clone(), rows, width, blob.len(), cap - 1).is_err()
                );
                let directory =
                    Directory::open(offsets.clone(), rows, width, blob.len(), cap).unwrap();
                let decoded = directory
                    .decode_range(0, directory.frames() - 1, &blob, &authority)
                    .unwrap();
                assert_eq!(decoded.start, 0);
                assert_eq!(decoded.bytes, original);
                assert!(
                    directory
                        .decode_range(
                            0,
                            directory.frames() - 1,
                            &blob[..blob.len() - 1],
                            &authority
                        )
                        .is_err()
                );
                let mut extra = blob.clone();
                extra.push(0);
                assert!(
                    directory
                        .decode_range(0, directory.frames() - 1, &extra, &authority)
                        .is_err()
                );
                for position in [0, offsets.len() - 8] {
                    let mut bad = offsets.clone();
                    bad[position..position + 8].copy_from_slice(&u64::MAX.to_le_bytes());
                    assert!(Directory::open(bad, rows, width, blob.len(), cap).is_err());
                }
                if rows == 769 {
                    // A validly encoded but unselected middle frame must still authenticate.
                    let range = directory.physical_range(1, 1).unwrap();
                    let wrong = encode(256, width, &vec![8; 256 * width], limits()).unwrap();
                    let mut bad_blob = blob.clone();
                    bad_blob.splice(range.clone(), wrong.iter().copied());
                    let delta = wrong.len() as i64 - range.len() as i64;
                    let mut adjusted = offsets.clone();
                    for entry in adjusted.chunks_exact_mut(8).skip(2) {
                        let offset = u64::from_le_bytes(entry.try_into().unwrap());
                        entry.copy_from_slice(
                            &offset.checked_add_signed(delta).unwrap().to_le_bytes(),
                        );
                    }
                    let bad = Directory::open(adjusted, rows, width, bad_blob.len(), cap).unwrap();
                    assert!(bad.decode_range(0, 3, &bad_blob, &authority).is_err());
                }
            }
        }
        assert!(frame_bytes(256, 4096).is_ok());
        assert!(frame_bytes(256, 4097).is_err());
        assert!(Directory::open(vec![0; 8], usize::MAX, usize::MAX, 0, usize::MAX).is_err());
        assert_eq!(
            Directory::open(vec![0; 8], 0, 9, 0, 144).unwrap().frames(),
            0
        );
    }
    #[test]
    fn framed_source_linked_codec_context_allowance() {
        let data = vec![0u8; 1 << 20];
        let encoded = zstd::bulk::compress(&data, 1).unwrap();
        let mut dctx = zstd::zstd_safe::DCtx::create();
        dctx.set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(20))
            .unwrap();
        let mut output = vec![0u8; data.len()];
        assert_eq!(
            dctx.decompress(&mut output[..], &encoded).unwrap(),
            data.len()
        );
        assert!(dctx.sizeof() <= DECODER_CONTEXT_BYTES);
        let mut cctx = zstd::zstd_safe::CCtx::create();
        let mut output = vec![0u8; zstd::zstd_safe::compress_bound(data.len())];
        cctx.compress(&mut output[..], &data, 1).unwrap();
        assert!(cctx.sizeof() <= ENCODER_CONTEXT_BYTES);
        eprintln!(
            "linked Zstd contexts decoder={} encoder={}",
            dctx.sizeof(),
            cctx.sizeof()
        );
    }
}
