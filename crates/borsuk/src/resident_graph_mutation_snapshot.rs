//! Bounded, authenticated latest-state mutations for one immutable graph root.

use half::f16;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{resident_graph_generation::valid_sha256, resident_graph_overlay::ResidentMutation};

const MAGIC: &[u8; 8] = b"BRGMUT01";
const HEADER_BYTES: usize = 8 + 64 + 4 + 4;

#[derive(Debug, Error)]
pub enum ResidentGraphMutationSnapshotError {
    #[error("mutation snapshot is invalid: {0}")]
    Invalid(&'static str),
}

/// Encode a complete latest-state map, sorted by public ID. Every put is
/// stored as finite FP16 coordinates, matching the resident query overlay.
pub fn encode_mutation_snapshot(
    base_root_sha256: &str,
    dimensions: usize,
    mutations: &[ResidentMutation],
    max_bytes: usize,
) -> Result<(Vec<u8>, String), ResidentGraphMutationSnapshotError> {
    if !valid_sha256(base_root_sha256)
        || dimensions == 0
        || u32::try_from(dimensions).is_err()
        || u32::try_from(mutations.len()).is_err()
        || max_bytes < HEADER_BYTES
    {
        return Err(ResidentGraphMutationSnapshotError::Invalid(
            "header geometry",
        ));
    }
    let mut rows = mutations.iter().collect::<Vec<_>>();
    rows.sort_unstable_by_key(|row| row.id);
    let mut bytes = Vec::with_capacity(HEADER_BYTES);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(base_root_sha256.as_bytes());
    bytes.extend_from_slice(&(dimensions as u32).to_le_bytes());
    bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    let mut previous = None;
    for row in rows {
        if previous.is_some_and(|id| row.id <= id) {
            return Err(ResidentGraphMutationSnapshotError::Invalid("duplicate ID"));
        }
        previous = Some(row.id);
        let row_bytes = 9_usize
            .checked_add(if row.vector.is_some() {
                dimensions
                    .checked_mul(2)
                    .ok_or(ResidentGraphMutationSnapshotError::Invalid("row width"))?
            } else {
                0
            })
            .ok_or(ResidentGraphMutationSnapshotError::Invalid("row width"))?;
        if bytes
            .len()
            .checked_add(row_bytes)
            .is_none_or(|len| len > max_bytes)
        {
            return Err(ResidentGraphMutationSnapshotError::Invalid("byte cap"));
        }
        bytes.extend_from_slice(&row.id.to_le_bytes());
        bytes.push(u8::from(row.vector.is_some()));
        if let Some(vector) = &row.vector {
            if vector.len() != dimensions || vector.iter().any(|x| !x.is_finite()) {
                return Err(ResidentGraphMutationSnapshotError::Invalid("put vector"));
            }
            let mut norm_squared = 0.0_f64;
            for &value in vector {
                let half = f16::from_f32(value);
                if !half.is_finite() {
                    return Err(ResidentGraphMutationSnapshotError::Invalid("FP16 overflow"));
                }
                let decoded = f64::from(half.to_f32());
                norm_squared += decoded * decoded;
                bytes.extend_from_slice(&half.to_bits().to_le_bytes());
            }
            if !norm_squared.is_finite() || norm_squared <= 0.0 {
                return Err(ResidentGraphMutationSnapshotError::Invalid("put norm"));
            }
        }
    }
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    Ok((bytes, sha256))
}

/// Authenticate the entire snapshot before interpreting a single row. The
/// root digest binds the snapshot to its immutable graph generation.
pub fn decode_mutation_snapshot(
    bytes: &[u8],
    expected_sha256: &str,
    base_root_sha256: &str,
    dimensions: usize,
    max_bytes: usize,
) -> Result<Vec<ResidentMutation>, ResidentGraphMutationSnapshotError> {
    if bytes.len() < HEADER_BYTES
        || bytes.len() > max_bytes
        || !valid_sha256(expected_sha256)
        || !valid_sha256(base_root_sha256)
        || format!("{:x}", Sha256::digest(bytes)) != expected_sha256
        || &bytes[..8] != MAGIC
        || &bytes[8..72] != base_root_sha256.as_bytes()
        || usize::try_from(u32::from_le_bytes(bytes[72..76].try_into().unwrap())).ok()
            != Some(dimensions)
        || dimensions == 0
    {
        return Err(ResidentGraphMutationSnapshotError::Invalid(
            "authenticated header",
        ));
    }
    let count = u32::from_le_bytes(bytes[76..80].try_into().unwrap()) as usize;
    if count > (bytes.len() - HEADER_BYTES) / 9 {
        return Err(ResidentGraphMutationSnapshotError::Invalid("row count"));
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| ResidentGraphMutationSnapshotError::Invalid("row allocation"))?;
    let mut offset = HEADER_BYTES;
    let mut previous = None;
    for _ in 0..count {
        let header = bytes
            .get(offset..offset + 9)
            .ok_or(ResidentGraphMutationSnapshotError::Invalid("truncated row"))?;
        let id = u64::from_le_bytes(header[..8].try_into().unwrap());
        if previous.is_some_and(|old| id <= old) {
            return Err(ResidentGraphMutationSnapshotError::Invalid("ID order"));
        }
        previous = Some(id);
        offset += 9;
        let vector =
            match header[8] {
                0 => None,
                1 => {
                    let width = dimensions
                        .checked_mul(2)
                        .ok_or(ResidentGraphMutationSnapshotError::Invalid("row width"))?;
                    let end = offset
                        .checked_add(width)
                        .ok_or(ResidentGraphMutationSnapshotError::Invalid("row width"))?;
                    let data = bytes.get(offset..end).ok_or(
                        ResidentGraphMutationSnapshotError::Invalid("truncated vector"),
                    )?;
                    let mut norm_squared = 0.0_f64;
                    let mut vector = Vec::new();
                    vector.try_reserve_exact(dimensions).map_err(|_| {
                        ResidentGraphMutationSnapshotError::Invalid("vector allocation")
                    })?;
                    for coordinate in data.chunks_exact(2) {
                        let value =
                            f16::from_bits(u16::from_le_bytes(coordinate.try_into().unwrap()));
                        if !value.is_finite() {
                            return Err(ResidentGraphMutationSnapshotError::Invalid("FP16 value"));
                        }
                        let decoded = value.to_f32();
                        norm_squared += f64::from(decoded) * f64::from(decoded);
                        vector.push(decoded);
                    }
                    if !norm_squared.is_finite() || norm_squared <= 0.0 {
                        return Err(ResidentGraphMutationSnapshotError::Invalid("put norm"));
                    }
                    offset = end;
                    Some(vector)
                }
                _ => return Err(ResidentGraphMutationSnapshotError::Invalid("operation")),
            };
        rows.push(ResidentMutation { id, vector });
    }
    if offset != bytes.len() {
        return Err(ResidentGraphMutationSnapshotError::Invalid(
            "trailing bytes",
        ));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_snapshot_roundtrip_and_rejection() {
        let base = "11".repeat(32);
        let mutations = vec![
            ResidentMutation {
                id: 9,
                vector: Some(vec![0.5, 1.0]),
            },
            ResidentMutation {
                id: 2,
                vector: None,
            },
        ];
        let (bytes, sha) = encode_mutation_snapshot(&base, 2, &mutations, 1024).unwrap();
        let rows = decode_mutation_snapshot(&bytes, &sha, &base, 2, 1024).unwrap();
        assert_eq!((rows[0].id, rows[1].id), (2, 9));
        assert_eq!(rows[1].vector.as_deref(), Some([0.5, 1.0].as_slice()));
        assert!(decode_mutation_snapshot(&bytes, &sha, &"22".repeat(32), 2, 1024).is_err());
        assert!(decode_mutation_snapshot(&bytes, &sha, &base, 2, bytes.len() - 1).is_err());
        let mut corrupt = bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decode_mutation_snapshot(&corrupt, &sha, &base, 2, 1024).is_err());
        let mut invalid_operation = bytes.clone();
        invalid_operation[HEADER_BYTES + 8] = 2;
        let signed_invalid = format!("{:x}", Sha256::digest(&invalid_operation));
        assert!(
            decode_mutation_snapshot(&invalid_operation, &signed_invalid, &base, 2, 1024).is_err()
        );
        assert!(
            encode_mutation_snapshot(
                &base,
                2,
                &[
                    ResidentMutation {
                        id: 9,
                        vector: None
                    },
                    ResidentMutation {
                        id: 9,
                        vector: None
                    },
                ],
                1024
            )
            .is_err()
        );
        assert!(encode_mutation_snapshot(&base, 2, &mutations, bytes.len() - 1).is_err());
    }
}
