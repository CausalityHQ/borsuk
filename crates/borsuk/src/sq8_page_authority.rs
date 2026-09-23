//! Generation-bound SHA-256 verification of exact S3 SQ8 page ranges.

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
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()
        && !byte.is_ascii_uppercase())
}

impl PageAuthority {
    pub fn load(
        manifest_json: &[u8], expected_manifest_sha256: &str, sidecar: &[u8],
    ) -> Result<Self, PageError> {
        if !hex64(expected_manifest_sha256)
            || format!("{:x}", Sha256::digest(manifest_json)) != expected_manifest_sha256
        {
            return Err(PageError::HashMismatch);
        }
        let manifest: serde_json::Value = serde_json::from_slice(manifest_json)
            .map_err(|_| PageError::InvalidManifest)?;
        let fields = manifest.as_object().ok_or(PageError::InvalidManifest)?;
        let names = ["schema", "generation", "rows", "dimensions", "page_rows",
                     "object_sha256", "page_digest_sha256"];
        if fields.len() != names.len() || names.iter().any(|key| !fields.contains_key(*key))
            || manifest["schema"] != "borsuk-v115-sq8-page-authority-v2"
        {
            return Err(PageError::InvalidManifest);
        }
        let positive = |key: &str| -> Result<usize, PageError> {
            let number = manifest[key].as_u64().ok_or(PageError::InvalidManifest)?;
            usize::try_from(number).ok().filter(|value| *value > 0)
                .ok_or(PageError::InvalidManifest)
        };
        let generation = u64::try_from(positive("generation")?)
            .map_err(|_| PageError::InvalidManifest)?;
        let rows = positive("rows")?;
        let dimensions = positive("dimensions")?;
        let page_rows = positive("page_rows")?;
        let object_hash = manifest["object_sha256"].as_str()
            .ok_or(PageError::InvalidManifest)?;
        let sidecar_hash = manifest["page_digest_sha256"].as_str()
            .ok_or(PageError::InvalidManifest)?;
        if !hex64(object_hash) || !hex64(sidecar_hash)
            || rows.checked_mul(dimensions.checked_add(12)
                .ok_or(PageError::InvalidManifest)?).is_none()
            || format!("{:x}", Sha256::digest(sidecar)) != sidecar_hash
        {
            return Err(PageError::InvalidSidecar);
        }
        let page_count = rows.div_ceil(page_rows);
        if sidecar.len() != page_count.checked_mul(32)
            .ok_or(PageError::InvalidSidecar)?
        {
            return Err(PageError::InvalidSidecar);
        }
        Ok(Self { generation, rows, dimensions, page_rows,
            object_sha256: object_hash.to_owned(),
            digests: sidecar.chunks_exact(32)
                .map(|chunk| chunk.try_into().unwrap()).collect() })
    }

    pub fn generation(&self) -> u64 { self.generation }
    pub fn rows(&self) -> usize { self.rows }
    pub fn dimensions(&self) -> usize { self.dimensions }
    pub fn page_rows(&self) -> usize { self.page_rows }
    pub fn object_sha256(&self) -> &str { &self.object_sha256 }

    pub fn object_bytes(&self) -> usize {
        self.rows * (self.dimensions + 12)
    }

    pub fn byte_range(&self, first_page: usize, last_page: usize)
        -> Result<Range<usize>, PageError>
    {
        if first_page > last_page || last_page >= self.digests.len() {
            return Err(PageError::InvalidRange);
        }
        let row_bytes = self.dimensions + 12;
        let start = first_page * self.page_rows * row_bytes;
        let stop = (last_page + 1).saturating_mul(self.page_rows)
            .min(self.rows) * row_bytes;
        Ok(start..stop)
    }

    /// Authenticate payload bytes after the transport has checked 206 and
    /// Content-Range, and verified the pinned ETag in the same request.
    pub fn verify_payload(&self, first_page: usize, last_page: usize,
        payload: &[u8]) -> Result<(), PageError>
    {
        let range = self.byte_range(first_page, last_page)?;
        if payload.len() != range.end - range.start {
            return Err(PageError::InvalidResponse);
        }
        let row_bytes = self.dimensions + 12;
        for page in first_page..=last_page {
            let local_start = (page - first_page) * self.page_rows * row_bytes;
            let local_stop = (page + 1).saturating_mul(self.page_rows)
                .min(self.rows) * row_bytes - range.start;
            if Sha256::digest(&payload[local_start..local_stop]).as_slice()
                != self.digests[page]
            {
                return Err(PageError::HashMismatch);
            }
        }
        Ok(())
    }

    /// Validate the exact response to a conditional `If-Match` page range GET.
    pub fn verify_range(
        &self, first_page: usize, last_page: usize, expected_etag: &str,
        response: &RangeResponse<'_>,
    ) -> Result<(), PageError> {
        if expected_etag.is_empty() {
            return Err(PageError::InvalidRange);
        }
        let range = self.byte_range(first_page, last_page)?;
        let expected_range = format!("bytes {}-{}/{}", range.start,
                                     range.end - 1, self.object_bytes());
        if response.status != 206 || response.content_range != expected_range
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
    fn authenticates_short_final_page_and_rejects_changed_bytes_or_etag() {
        let object = vec![7u8; 273 * 13];
        let sidecar = object.chunks(256 * 13)
            .flat_map(|page| Sha256::digest(page).to_vec()).collect::<Vec<_>>();
        let manifest = serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":273, "dimensions":1, "page_rows":256,
            "object_sha256":format!("{:x}", Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}", Sha256::digest(&sidecar)),
        });
        let manifest = serde_json::to_vec(&manifest).unwrap();
        let authority = PageAuthority::load(&manifest,
            &format!("{:x}", Sha256::digest(&manifest)), &sidecar).unwrap();
        let mut response = RangeResponse {
            status:206, content_range:"bytes 3328-3548/3549",
            content_length:17 * 13, etag:"etag-a", payload:&object[256 * 13..],
        };
        assert_eq!(authority.verify_range(1, 1, "etag-a", &response), Ok(()));
        assert_eq!(authority.verify_range(1, 1, "etag-b", &response),
                   Err(PageError::InvalidResponse));
        response.status = 200;
        assert_eq!(authority.verify_range(1, 1, "etag-a", &response),
                   Err(PageError::InvalidResponse));
        let changed = vec![8u8; 17 * 13];
        response.status = 206;
        response.payload = &changed;
        assert_eq!(authority.verify_range(1, 1, "etag-a", &response),
                   Err(PageError::HashMismatch));
    }
}
