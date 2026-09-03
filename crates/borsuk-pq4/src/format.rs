use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{BorsukError, Result};

const SNAPSHOT_SCHEMA: &str = "borsuk-pq4-snapshot-v1";
const CODEBOOK_SCHEMA: &str = "centroids:non-nullable-fixed-list-f32[1536]";
const CODES_SCHEMA: &str = "block_ordinal:u64,packed_codes:non-nullable-fixed-binary[512]";
const VECTORS_SCHEMA: &str = "vector:non-nullable-fixed-list-f32[96]";
const IDS_SCHEMA: &str = "id:non-nullable-binary";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pq4ArtifactIdentity {
    pub(crate) role: String,
    pub(crate) file_name: String,
    pub(crate) sha256: String,
    pub(crate) encoded_bytes: u64,
    pub(crate) row_count: u64,
    pub(crate) schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pq4Manifest {
    pub(crate) schema: String,
    pub(crate) generation: String,
    pub(crate) source_uri: String,
    pub(crate) source_sha256: String,
    pub(crate) source_encoded_bytes: u64,
    pub(crate) row_count: u64,
    pub(crate) dimension: u32,
    pub(crate) subquantizer_count: u32,
    pub(crate) subspace_dimensions: u32,
    pub(crate) centroid_count: u32,
    pub(crate) lloyd_iterations: u32,
    pub(crate) block_rows: u32,
    pub(crate) block_count: u64,
    pub(crate) padding_rows: u32,
    pub(crate) code_bytes_per_row: u32,
    pub(crate) byte_order: String,
    pub(crate) nibble_order: String,
    pub(crate) source_order: String,
    pub(crate) candidate_depth: u32,
    pub(crate) codebook: Pq4ArtifactIdentity,
    pub(crate) codes: Pq4ArtifactIdentity,
    pub(crate) vectors: Pq4ArtifactIdentity,
    pub(crate) ids: Pq4ArtifactIdentity,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn exact_lower_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_identity(
    identity: &Pq4ArtifactIdentity,
    role: &str,
    file_name: &str,
    row_count: u64,
    schema: &str,
) -> Result<()> {
    if identity.role != role
        || identity.file_name != file_name
        || !exact_lower_hex(&identity.sha256)
        || identity.encoded_bytes == 0
        || identity.row_count != row_count
        || identity.schema != schema
    {
        return Err(invalid("PQ4 snapshot artifact identity differs"));
    }
    Ok(())
}

fn validate_manifest(manifest: &Pq4Manifest) -> Result<()> {
    let block_count = manifest.row_count.div_ceil(32);
    let padding_rows = block_count
        .checked_mul(32)
        .and_then(|rows| rows.checked_sub(manifest.row_count))
        .and_then(|rows| u32::try_from(rows).ok())
        .ok_or_else(|| invalid("PQ4 snapshot row arithmetic differs"))?;
    if manifest.schema != SNAPSHOT_SCHEMA
        || manifest.generation.is_empty()
        || manifest.source_uri.is_empty()
        || !exact_lower_hex(&manifest.source_sha256)
        || manifest.source_encoded_bytes == 0
        || manifest.row_count < u64::from(manifest.candidate_depth)
        || manifest.dimension != 96
        || manifest.subquantizer_count != 32
        || manifest.subspace_dimensions != 3
        || manifest.centroid_count != 16
        || manifest.lloyd_iterations != 4
        || manifest.block_rows != 32
        || manifest.block_count != block_count
        || manifest.padding_rows != padding_rows
        || manifest.code_bytes_per_row != 16
        || manifest.byte_order != "subquantizer-major"
        || manifest.nibble_order != "even-low-odd-high"
        || manifest.source_order != "ascending-source-ordinal"
        || manifest.candidate_depth != 3_072
    {
        return Err(invalid("PQ4 snapshot manifest differs"));
    }

    validate_identity(
        &manifest.codebook,
        "codebook-arrow",
        "codebook.arrow",
        1,
        CODEBOOK_SCHEMA,
    )?;
    validate_identity(
        &manifest.codes,
        "codes-arrow",
        "codes.arrow",
        block_count,
        CODES_SCHEMA,
    )?;
    validate_identity(
        &manifest.vectors,
        "vectors-arrow",
        "vectors.arrow",
        manifest.row_count,
        VECTORS_SCHEMA,
    )?;
    validate_identity(
        &manifest.ids,
        "ids-arrow",
        "ids.arrow",
        manifest.row_count,
        IDS_SCHEMA,
    )?;
    let names = [
        &manifest.codebook.file_name,
        &manifest.codes.file_name,
        &manifest.vectors.file_name,
        &manifest.ids.file_name,
    ];
    if names.into_iter().collect::<BTreeSet<_>>().len() != 4 {
        return Err(invalid("PQ4 snapshot artifact names overlap"));
    }
    Ok(())
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect(),
        ),
        value => value,
    }
}

pub(crate) fn canonical_manifest_bytes(manifest: &Pq4Manifest) -> Result<Vec<u8>> {
    validate_manifest(manifest)?;
    let value = serde_json::to_value(manifest)
        .map_err(|error| invalid(&format!("PQ4 snapshot manifest encoding failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("PQ4 snapshot manifest encoding failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}
