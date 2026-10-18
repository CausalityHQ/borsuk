//! Experimental, lossless independent record frames. No serving format is changed.
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
}
