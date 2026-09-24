//! Authenticated source-only PQ64 router artifact loading.

use crate::pq64_nominee::{Pq64Error, Pq64Router};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

/// Authenticated router and source-derived SQ8 affine coefficients.
pub struct SourceRouterArtifact {
    pub router: Pq64Router,
    pub low: Vec<f32>,
    pub step: Vec<f32>,
    pub generation: u64,
    /// Whole-manifest digest checked by the loader and retained for binding.
    pub manifest_sha256: String,
    pub source_sha256: String,
    pub layout_sha256: String,
    pub sq8_sha256: String,
}

#[derive(Debug)]
pub enum RouterArtifactError {
    Io(io::Error),
    Invalid,
    HashMismatch,
    Router(Pq64Error),
}

impl std::fmt::Display for RouterArtifactError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(output, "source router artifact: {self:?}")
    }
}

impl std::error::Error for RouterArtifactError {}

impl From<io::Error> for RouterArtifactError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<Pq64Error> for RouterArtifactError {
    fn from(value: Pq64Error) -> Self {
        Self::Router(value)
    }
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

fn number(value: &Value, key: &str) -> Result<usize, RouterArtifactError> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|number| usize::try_from(number).ok())
        .filter(|number| *number > 0)
        .ok_or(RouterArtifactError::Invalid)
}

fn hash_field(value: &Value, key: &str) -> Result<String, RouterArtifactError> {
    let hash = value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(RouterArtifactError::Invalid)?;
    if !hex64(hash) {
        return Err(RouterArtifactError::Invalid);
    }
    Ok(hash.to_owned())
}

fn read_section(
    root: &Path,
    sections: &Value,
    name: &str,
    expected_bytes: usize,
) -> Result<Vec<u8>, RouterArtifactError> {
    let section = sections.get(name).ok_or(RouterArtifactError::Invalid)?;
    if number(section, "bytes")? != expected_bytes {
        return Err(RouterArtifactError::Invalid);
    }
    let expected_sha = hash_field(section, "sha256")?;
    let path = root.join(format!("{name}.bin"));
    let mut file = File::open(path)?;
    if usize::try_from(file.metadata()?.len()).ok() != Some(expected_bytes) {
        return Err(RouterArtifactError::Invalid);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(expected_bytes)
        .map_err(|_| RouterArtifactError::Invalid)?;
    bytes.resize(expected_bytes, 0);
    file.read_exact(&mut bytes)?;
    let mut extra = [0_u8; 1];
    if file.read(&mut extra)? != 0 {
        return Err(RouterArtifactError::Invalid);
    }
    if digest(&bytes) != expected_sha {
        return Err(RouterArtifactError::HashMismatch);
    }
    Ok(bytes)
}

fn floats(bytes: &[u8]) -> Result<Vec<f32>, RouterArtifactError> {
    if bytes.len() % 4 != 0 {
        return Err(RouterArtifactError::Invalid);
    }
    let values = bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    if values.iter().any(|value| !value.is_finite()) {
        return Err(RouterArtifactError::Invalid);
    }
    Ok(values)
}

/// Load only when the caller has a trusted whole-manifest SHA-256.
pub fn load_source_router(
    root: &Path,
    expected_manifest_sha256: &str,
) -> Result<SourceRouterArtifact, RouterArtifactError> {
    if !hex64(expected_manifest_sha256) {
        return Err(RouterArtifactError::Invalid);
    }
    let file = File::open(root.join("manifest.json"))?;
    if file.metadata()?.len() > 64 * 1024 {
        return Err(RouterArtifactError::Invalid);
    }
    let mut raw = Vec::new();
    file.take(64 * 1024 + 1).read_to_end(&mut raw)?;
    if raw.len() > 64 * 1024 {
        return Err(RouterArtifactError::Invalid);
    }
    if digest(&raw) != expected_manifest_sha256 {
        return Err(RouterArtifactError::HashMismatch);
    }
    let manifest: Value = serde_json::from_slice(&raw).map_err(|_| RouterArtifactError::Invalid)?;
    if manifest.get("schema").and_then(Value::as_str) != Some("borsuk-source-router-v2") {
        return Err(RouterArtifactError::Invalid);
    }
    let geometry = manifest
        .get("geometry")
        .ok_or(RouterArtifactError::Invalid)?;
    let rows = number(geometry, "rows")?;
    let dimensions = number(geometry, "dimensions")?;
    let page_rows = number(geometry, "page_rows")?;
    let blocks_per_page = number(geometry, "blocks_per_page")?;
    if number(geometry, "subspaces")? != 64
        || number(geometry, "pq_width")? != dimensions.div_ceil(64)
        || geometry.get("pq_partition").and_then(Value::as_str) != Some("balanced_floor_v1")
    {
        return Err(RouterArtifactError::Invalid);
    }
    let generation = u64::try_from(number(&manifest, "generation")?)
        .map_err(|_| RouterArtifactError::Invalid)?;
    let source_sha256 = hash_field(&manifest, "source_sha256")?;
    let layout_sha256 = hash_field(&manifest, "layout_sha256")?;
    let sq8_sha256 = hash_field(&manifest, "sq8_sha256")?;
    let sections = manifest
        .get("sections")
        .ok_or(RouterArtifactError::Invalid)?;
    let page_count = rows.div_ceil(page_rows);
    let summary_values = page_count
        .checked_mul(blocks_per_page)
        .and_then(|count| count.checked_mul(dimensions))
        .ok_or(RouterArtifactError::Invalid)?;
    let book_values = 64usize
        .checked_mul(256)
        .and_then(|count| count.checked_mul(dimensions.div_ceil(64)))
        .ok_or(RouterArtifactError::Invalid)?;
    let code_bytes = rows.checked_mul(64).ok_or(RouterArtifactError::Invalid)?;
    let summaries = floats(&read_section(
        root,
        sections,
        "summaries",
        summary_values
            .checked_mul(4)
            .ok_or(RouterArtifactError::Invalid)?,
    )?)?;
    let books = floats(&read_section(
        root,
        sections,
        "books",
        book_values
            .checked_mul(4)
            .ok_or(RouterArtifactError::Invalid)?,
    )?)?;
    let codes = read_section(root, sections, "codes", code_bytes)?;
    let low = floats(&read_section(
        root,
        sections,
        "low",
        dimensions
            .checked_mul(4)
            .ok_or(RouterArtifactError::Invalid)?,
    )?)?;
    let step = floats(&read_section(
        root,
        sections,
        "step",
        dimensions
            .checked_mul(4)
            .ok_or(RouterArtifactError::Invalid)?,
    )?)?;
    if step.iter().any(|value| *value <= 0.0) {
        return Err(RouterArtifactError::Invalid);
    }
    let router = Pq64Router::new(
        rows,
        dimensions,
        page_rows,
        blocks_per_page,
        summaries,
        books,
        codes,
    )?;
    Ok(SourceRouterArtifact {
        router,
        low,
        step,
        generation,
        manifest_sha256: expected_manifest_sha256.to_owned(),
        source_sha256,
        layout_sha256,
        sq8_sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::{digest, load_source_router};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rejects_a_missing_source_router_manifest() {
        let missing = std::env::temp_dir().join("borsuk-missing-router-artifact-v115");
        assert!(load_source_router(&missing, "0".repeat(64).as_str()).is_err());
    }

    #[test]
    fn loads_authenticated_sections_and_rejects_a_changed_code() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("borsuk-v115-router-{suffix}"));
        fs::create_dir(&root).unwrap();
        let section_data = [
            ("summaries", vec![0u8; 4 * 64 * 4]),
            ("books", vec![0u8; 64 * 256 * 4]),
            ("codes", vec![0u8; 512 * 64]),
            ("low", vec![0u8; 64 * 4]),
            ("step", vec![0u8; 64 * 4]),
        ];
        let mut sections = serde_json::Map::new();
        for (name, bytes) in &section_data {
            let mut bytes = bytes.clone();
            if *name == "step" {
                for chunk in bytes.chunks_exact_mut(4) {
                    chunk.copy_from_slice(&1.0f32.to_le_bytes());
                }
            }
            fs::write(root.join(format!("{name}.bin")), &bytes).unwrap();
            sections.insert(
                (*name).to_owned(),
                serde_json::json!({
                    "bytes": bytes.len(), "sha256": digest(&bytes),
                }),
            );
        }
        let manifest = serde_json::json!({
            "schema": "borsuk-source-router-v2", "generation": 1,
            "source_sha256": "a".repeat(64), "layout_sha256": "b".repeat(64),
            "sq8_sha256": "c".repeat(64),
            "geometry": {"rows": 512, "dimensions": 64, "page_rows": 256,
                         "blocks_per_page": 2, "subspaces": 64, "pq_width": 1,
                         "pq_partition": "balanced_floor_v1"},
            "sections": sections,
        });
        let raw = serde_json::to_vec(&manifest).unwrap();
        fs::write(root.join("manifest.json"), &raw).unwrap();
        let loaded = load_source_router(&root, &digest(&raw)).unwrap();
        assert_eq!(loaded.manifest_sha256, digest(&raw));
        assert_eq!(
            loaded.router.nominate(&vec![0.0f32; 64], 1, 128).unwrap(),
            (0..128).collect::<Vec<_>>()
        );
        fs::write(root.join("codes.bin"), vec![1u8; 512 * 64]).unwrap();
        assert!(load_source_router(&root, &digest(&raw)).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
