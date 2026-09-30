//! Generation-bound SHA-256 verification of exact S3 record-page ranges.

use sha2::{Digest, Sha256};
use std::ops::Range;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageError {
    InvalidManifest,
    InvalidSidecar,
    InvalidRange,
    InvalidResponse,
    HashMismatch,
}

/// Loaded from a manifest whose SHA-256 is pinned by the generation authority.
pub struct PageAuthority {
    generation: u64,
    rows: usize,
    dimensions: usize,
    record_bytes: usize,
    sq8: bool,
    page_rows: usize,
    object_sha256: String,
    digests: Vec<[u8; 32]>,
}

/// S3 metadata and bytes returned for one conditional inclusive byte range.
pub struct RangeResponse<'a> {
    pub status: u16,
    pub content_range: &'a str,
    pub content_length: usize,
    pub etag: &'a str,
    pub payload: &'a [u8],
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

impl PageAuthority {
    pub fn load(
        manifest_json: &[u8],
        expected_manifest_sha256: &str,
        sidecar: &[u8],
    ) -> Result<Self, PageError> {
        if !hex64(expected_manifest_sha256)
            || format!("{:x}", Sha256::digest(manifest_json)) != expected_manifest_sha256
        {
            return Err(PageError::HashMismatch);
        }
        let manifest: serde_json::Value =
            serde_json::from_slice(manifest_json).map_err(|_| PageError::InvalidManifest)?;
        let fields = manifest.as_object().ok_or(PageError::InvalidManifest)?;
        let names = [
            "schema",
            "generation",
            "rows",
            "dimensions",
            "page_rows",
            "object_sha256",
            "page_digest_sha256",
        ];
        if fields.len() != names.len()
            || names.iter().any(|key| !fields.contains_key(*key))
            || manifest["schema"] != "borsuk-v115-sq8-page-authority-v2"
        {
            return Err(PageError::InvalidManifest);
        }
        let positive = |key: &str| -> Result<usize, PageError> {
            let number = manifest[key].as_u64().ok_or(PageError::InvalidManifest)?;
            usize::try_from(number)
                .ok()
                .filter(|value| *value > 0)
                .ok_or(PageError::InvalidManifest)
        };
        let generation =
            u64::try_from(positive("generation")?).map_err(|_| PageError::InvalidManifest)?;
        let rows = positive("rows")?;
        let dimensions = positive("dimensions")?;
        let page_rows = positive("page_rows")?;
        let object_hash = manifest["object_sha256"]
            .as_str()
            .ok_or(PageError::InvalidManifest)?;
        let sidecar_hash = manifest["page_digest_sha256"]
            .as_str()
            .ok_or(PageError::InvalidManifest)?;
        if !hex64(object_hash)
            || !hex64(sidecar_hash)
            || rows
                .checked_mul(
                    dimensions
                        .checked_add(12)
                        .ok_or(PageError::InvalidManifest)?,
                )
                .is_none()
            || format!("{:x}", Sha256::digest(sidecar)) != sidecar_hash
        {
            return Err(PageError::InvalidSidecar);
        }
        let page_count = rows.div_ceil(page_rows);
        if sidecar.len()
            != page_count
                .checked_mul(32)
                .ok_or(PageError::InvalidSidecar)?
        {
            return Err(PageError::InvalidSidecar);
        }
        Ok(Self {
            generation,
            rows,
            dimensions,
            record_bytes: dimensions + 12,
            sq8: true,
            page_rows,
            object_sha256: object_hash.to_owned(),
            digests: sidecar
                .chunks_exact(32)
                .map(|chunk| chunk.try_into().unwrap())
                .collect(),
        })
    }

    /// Load encoded source-unit pages from a pinned v3 source-plane manifest.
    /// The caller binds both its manifest digest and generation to the trusted
    /// generation root. This authority is not valid for SQ8 ranking.
    pub fn load_two_bit(
        manifest_json: &[u8],
        expected_manifest_sha256: &str,
        generation: u64,
        sidecar: &[u8],
    ) -> Result<Self, PageError> {
        if !hex64(expected_manifest_sha256)
            || format!("{:x}", Sha256::digest(manifest_json)) != expected_manifest_sha256
        {
            return Err(PageError::HashMismatch);
        }
        let manifest: serde_json::Value =
            serde_json::from_slice(manifest_json).map_err(|_| PageError::InvalidManifest)?;
        let fields = manifest.as_object().ok_or(PageError::InvalidManifest)?;
        let names = [
            "schema",
            "rows",
            "dimensions",
            "seed",
            "record_bytes",
            "source_sha256",
            "sq8_sha256",
            "source_order_sha256",
            "mean_sha256",
            "records_sha256",
            "query_or_truth_used",
            "page_rows",
            "page_digest_sha256",
        ];
        if fields.len() != names.len()
            || names.iter().any(|key| !fields.contains_key(*key))
            || manifest["schema"] != "borsuk-two-bit-plane-v3"
            || manifest["seed"].as_u64() != Some(20260923)
            || manifest["query_or_truth_used"] != false
            || generation == 0
        {
            return Err(PageError::InvalidManifest);
        }
        let positive = |key: &str| -> Result<usize, PageError> {
            manifest[key]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .filter(|&n| n > 0)
                .ok_or(PageError::InvalidManifest)
        };
        let rows = positive("rows")?;
        let dimensions = positive("dimensions")?;
        let page_rows = positive("page_rows")?;
        let record_bytes = positive("record_bytes")?;
        let padded = crate::rotated_two_bit::RotatedTwoBitCodec::padded_dimensions(dimensions)
            .map_err(|_| PageError::InvalidManifest)?;
        if page_rows != 32
            || padded.div_ceil(4).checked_add(8) != Some(record_bytes)
            || rows.checked_mul(record_bytes).is_none()
        {
            return Err(PageError::InvalidManifest);
        }
        for key in [
            "source_sha256",
            "sq8_sha256",
            "source_order_sha256",
            "mean_sha256",
            "records_sha256",
            "page_digest_sha256",
        ] {
            if !manifest[key].as_str().is_some_and(hex64) {
                return Err(PageError::InvalidManifest);
            }
        }
        if rows.div_ceil(page_rows).checked_mul(32) != Some(sidecar.len())
            || format!("{:x}", Sha256::digest(sidecar)) != manifest["page_digest_sha256"]
        {
            return Err(PageError::InvalidSidecar);
        }
        Ok(Self {
            generation,
            rows,
            dimensions,
            record_bytes,
            page_rows,
            sq8: false,
            object_sha256: manifest["records_sha256"].as_str().unwrap().to_owned(),
            digests: sidecar
                .chunks_exact(32)
                .map(|chunk| chunk.try_into().unwrap())
                .collect(),
        })
    }

    pub(crate) fn is_sq8(&self) -> bool {
        self.sq8
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
    pub fn page_rows(&self) -> usize {
        self.page_rows
    }
    pub fn object_sha256(&self) -> &str {
        &self.object_sha256
    }

    pub fn object_bytes(&self) -> usize {
        self.rows * self.record_bytes
    }

    pub fn byte_range(
        &self,
        first_page: usize,
        last_page: usize,
    ) -> Result<Range<usize>, PageError> {
        if first_page > last_page || last_page >= self.digests.len() {
            return Err(PageError::InvalidRange);
        }
        let row_bytes = self.record_bytes;
        let start = first_page * self.page_rows * row_bytes;
        let stop = (last_page + 1)
            .saturating_mul(self.page_rows)
            .min(self.rows)
            * row_bytes;
        Ok(start..stop)
    }

    /// Authenticate payload bytes after the transport has checked 206 and
    /// Content-Range, and verified the pinned ETag in the same request.
    pub fn verify_payload(
        &self,
        first_page: usize,
        last_page: usize,
        payload: &[u8],
    ) -> Result<(), PageError> {
        let range = self.byte_range(first_page, last_page)?;
        if payload.len() != range.end - range.start {
            return Err(PageError::InvalidResponse);
        }
        let row_bytes = self.record_bytes;
        for page in first_page..=last_page {
            let local_start = (page - first_page) * self.page_rows * row_bytes;
            let local_stop =
                (page + 1).saturating_mul(self.page_rows).min(self.rows) * row_bytes - range.start;
            if Sha256::digest(&payload[local_start..local_stop]).as_slice() != self.digests[page] {
                return Err(PageError::HashMismatch);
            }
        }
        Ok(())
    }

    /// Validate the exact response to a conditional `If-Match` page range GET.
    pub fn verify_range(
        &self,
        first_page: usize,
        last_page: usize,
        expected_etag: &str,
        response: &RangeResponse<'_>,
    ) -> Result<(), PageError> {
        if expected_etag.is_empty() {
            return Err(PageError::InvalidRange);
        }
        let range = self.byte_range(first_page, last_page)?;
        let expected_range = format!(
            "bytes {}-{}/{}",
            range.start,
            range.end - 1,
            self.object_bytes()
        );
        if response.status != 206
            || response.content_range != expected_range
            || response.content_length != range.end - range.start
            || response.etag != expected_etag
        {
            return Err(PageError::InvalidResponse);
        }
        self.verify_payload(first_page, last_page, response.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_bit_unit_pages_bind_geometry_and_authenticate_partial_tail() {
        let object = vec![7u8; 33 * 10];
        let sidecar = object
            .chunks(32 * 10)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::json!({
            "schema":"borsuk-two-bit-plane-v3", "rows":33, "dimensions":5,
            "seed":20260923, "record_bytes":10, "page_rows":32,
            "source_sha256":"0".repeat(64), "sq8_sha256":"1".repeat(64),
            "source_order_sha256":"2".repeat(64), "mean_sha256":"3".repeat(64),
            "records_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
            "query_or_truth_used":false,
        });
        let load = |value: &serde_json::Value, digests: &[u8], generation| {
            let body = serde_json::to_vec(value).unwrap();
            PageAuthority::load_two_bit(
                &body,
                &format!("{:x}", Sha256::digest(&body)),
                generation,
                digests,
            )
        };
        let authority = load(&manifest, &sidecar, 9).unwrap();
        assert!(!authority.is_sq8());
        assert_eq!(authority.generation(), 9);
        assert_eq!(authority.object_bytes(), 330);
        assert_eq!(authority.byte_range(1, 1), Ok(320..330));
        assert_eq!(authority.verify_payload(0, 1, &object), Ok(()));
        assert_eq!(authority.verify_payload(1, 1, &object[320..]), Ok(()));
        assert_eq!(
            authority.verify_payload(1, 1, &[8; 10]),
            Err(PageError::HashMismatch)
        );
        assert!(load(&manifest, &sidecar[..32], 9).is_err());
        assert!(load(&manifest, &sidecar, 0).is_err());
        for (key, value) in [
            ("schema", serde_json::json!("borsuk-two-bit-plane-v2")),
            ("page_rows", serde_json::json!(256)),
            ("record_bytes", serde_json::json!(11)),
            ("seed", serde_json::json!(1)),
            ("rows", serde_json::json!(0)),
            ("dimensions", serde_json::json!(0)),
            ("query_or_truth_used", serde_json::json!(true)),
            ("extra", serde_json::json!(0)),
        ] {
            let mut bad = manifest.clone();
            bad[key] = value;
            assert!(load(&bad, &sidecar, 9).is_err(), "accepted {key}");
        }
        let body = serde_json::to_vec(&manifest).unwrap();
        assert!(PageAuthority::load_two_bit(&body, &"4".repeat(64), 9, &sidecar).is_err());
    }

    #[test]
    fn authenticates_short_final_page_and_rejects_changed_bytes_or_etag() {
        let object = vec![7u8; 273 * 13];
        let sidecar = object
            .chunks(256 * 13)
            .flat_map(|page| Sha256::digest(page).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":273, "dimensions":1, "page_rows":256,
            "object_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
        });
        let manifest = serde_json::to_vec(&manifest).unwrap();
        let authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        let mut response = RangeResponse {
            status: 206,
            content_range: "bytes 3328-3548/3549",
            content_length: 17 * 13,
            etag: "etag-a",
            payload: &object[256 * 13..],
        };
        assert_eq!(authority.verify_range(1, 1, "etag-a", &response), Ok(()));
        assert_eq!(
            authority.verify_range(1, 1, "etag-b", &response),
            Err(PageError::InvalidResponse)
        );
        response.status = 200;
        assert_eq!(
            authority.verify_range(1, 1, "etag-a", &response),
            Err(PageError::InvalidResponse)
        );
        let changed = vec![8u8; 17 * 13];
        response.status = 206;
        response.payload = &changed;
        assert_eq!(
            authority.verify_range(1, 1, "etag-a", &response),
            Err(PageError::HashMismatch)
        );
    }
}
