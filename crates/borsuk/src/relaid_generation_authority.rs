//! Authenticated immutable root for one relaid SQ8 serving generation.

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::native_source_tier::decoded_sha256;

/// Identity and S3 object pin supplied by a trusted whole-manifest digest.
#[derive(Debug)]
pub struct RelayedGenerationAuthority {
    manifest_sha256: [u8; 32],
    generation: u64,
    rows: usize,
    dimensions: usize,
    source_sha256: [u8; 32],
    router_manifest_sha256: [u8; 32],
    row_map_sha256: [u8; 32],
    old_sq8_sha256: [u8; 32],
    new_sq8_sha256: [u8; 32],
    object_key: String,
    etag: String,
}

/// Failure to authenticate or parse the relaid generation root.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RelayedAuthorityError {
    /// The trusted root digest does not match the JSON bytes.
    #[error("relaid generation root SHA-256 differs")]
    HashMismatch,
    /// The strict v1 schema or one of its identities is invalid.
    #[error("relaid generation root contract differs")]
    Invalid,
}

fn hash(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    decoded_sha256(value).ok()
}

impl RelayedGenerationAuthority {
    /// Load a strict v1 root only when its complete JSON bytes match a
    /// separately trusted SHA-256 supplied by the generation publisher.
    pub fn load_authenticated(
        raw: &[u8],
        trusted_sha256: &str,
    ) -> Result<Self, RelayedAuthorityError> {
        let expected = hash(trusted_sha256).ok_or(RelayedAuthorityError::Invalid)?;
        if raw.len() > 16 * 1024 {
            return Err(RelayedAuthorityError::Invalid);
        }
        if Sha256::digest(raw)[..] != expected {
            return Err(RelayedAuthorityError::HashMismatch);
        }
        Self::parse(raw, expected).ok_or(RelayedAuthorityError::Invalid)
    }

    fn parse(raw: &[u8], expected: [u8; 32]) -> Option<Self> {
        let value: Value = serde_json::from_slice(raw).ok()?;
        let fields = value.as_object()?;
        let names = [
            "schema",
            "generation",
            "rows",
            "dimensions",
            "source_sha256",
            "router_manifest_sha256",
            "row_map_sha256",
            "old_sq8_sha256",
            "new_sq8_sha256",
            "object_key",
            "etag",
        ];
        if fields.len() != names.len()
            || names.iter().any(|name| !fields.contains_key(*name))
            || value["schema"] != "borsuk-relaid-generation-v1"
        {
            return None;
        }
        let generation = value["generation"].as_u64().filter(|number| *number > 0)?;
        let rows = usize::try_from(value["rows"].as_u64()?)
            .ok()
            .filter(|number| *number > 0 && *number <= u32::MAX as usize)?;
        let dimensions = usize::try_from(value["dimensions"].as_u64()?)
            .ok()
            .filter(|number| *number > 0)?;
        let object_key = value["object_key"].as_str()?.to_owned();
        let etag = value["etag"].as_str()?.to_owned();
        if object_key.is_empty() || etag.is_empty() {
            return None;
        }
        Some(Self {
            manifest_sha256: expected,
            generation,
            rows,
            dimensions,
            source_sha256: hash(value["source_sha256"].as_str()?)?,
            router_manifest_sha256: hash(value["router_manifest_sha256"].as_str()?)?,
            row_map_sha256: hash(value["row_map_sha256"].as_str()?)?,
            old_sq8_sha256: hash(value["old_sq8_sha256"].as_str()?)?,
            new_sq8_sha256: hash(value["new_sq8_sha256"].as_str()?)?,
            object_key,
            etag,
        })
    }

    /// Trusted root digest.
    pub fn manifest_sha256(&self) -> [u8; 32] {
        self.manifest_sha256
    }
    /// Generation number.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Number of rows.
    pub fn rows(&self) -> usize {
        self.rows
    }
    /// SQ8 dimensions.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
    /// Exact source identity.
    pub fn source_sha256(&self) -> [u8; 32] {
        self.source_sha256
    }
    /// Old router manifest identity.
    pub fn router_manifest_sha256(&self) -> [u8; 32] {
        self.router_manifest_sha256
    }
    /// Checked physical row-map artifact identity.
    pub fn row_map_sha256(&self) -> [u8; 32] {
        self.row_map_sha256
    }
    /// Old router SQ8 identity.
    pub fn old_sq8_sha256(&self) -> [u8; 32] {
        self.old_sq8_sha256
    }
    /// Relaid SQ8 object identity.
    pub fn new_sq8_sha256(&self) -> [u8; 32] {
        self.new_sq8_sha256
    }
    /// S3 key of the relaid SQ8 object.
    pub fn object_key(&self) -> &str {
        &self.object_key
    }
    /// Conditional S3 ETag to pin through page requests.
    pub fn etag(&self) -> &str {
        &self.etag
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_root_requires_a_trusted_whole_digest_and_declared_map() {
        let raw = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-relaid-generation-v1", "generation":7,
            "rows":2, "dimensions":1, "source_sha256":"a".repeat(64),
            "router_manifest_sha256":"b".repeat(64),
            "row_map_sha256":"c".repeat(64),
            "old_sq8_sha256":"d".repeat(64),
            "new_sq8_sha256":"e".repeat(64),
            "object_key":"sq8/new.bin", "etag":"etag-7",
        }))
        .unwrap();
        let digest = format!("{:x}", Sha256::digest(&raw));
        let loaded = RelayedGenerationAuthority::load_authenticated(&raw, &digest).unwrap();
        assert_eq!(loaded.rows(), 2);
        assert_eq!(loaded.row_map_sha256(), hash(&"c".repeat(64)).unwrap());
        assert!(matches!(
            RelayedGenerationAuthority::load_authenticated(&raw, &"f".repeat(64)),
            Err(RelayedAuthorityError::HashMismatch)
        ));
        let mut changed = raw.clone();
        changed.push(b' ');
        assert!(matches!(
            RelayedGenerationAuthority::load_authenticated(&changed, &digest),
            Err(RelayedAuthorityError::HashMismatch)
        ));
    }
}
