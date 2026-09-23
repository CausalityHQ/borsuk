//! One identity fence for router, SQ8 mirror and S3 page authority.

use crate::exact_sq8_mirror::MirrorManifest;
use crate::pq64_router_artifact::SourceRouterArtifact;
use crate::sq8_page_authority::PageAuthority;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationError {
    IdentityMismatch,
}

/// All immutable query planes and the S3 object identity agree before serving.
/// The caller retains the authenticated manifest SHA-256 inputs used to load
/// the router, mirror and page authority, and pins this ETag through serving.
pub struct ServingGeneration<'a> {
    router: &'a SourceRouterArtifact,
    mirror: &'a MirrorManifest,
    pages: &'a PageAuthority,
    object_key: &'a str,
    etag: &'a str,
}

impl<'a> ServingGeneration<'a> {
    pub fn router(&self) -> &SourceRouterArtifact {
        self.router
    }
    pub fn mirror(&self) -> &MirrorManifest {
        self.mirror
    }
    pub fn pages(&self) -> &PageAuthority {
        self.pages
    }
    pub fn object_key(&self) -> &str {
        self.object_key
    }
    pub fn etag(&self) -> &str {
        self.etag
    }

    pub fn bind(
        router: &'a SourceRouterArtifact,
        mirror: &'a MirrorManifest,
        pages: &'a PageAuthority,
        object_key: &'a str,
        etag: &'a str,
    ) -> Result<Self, GenerationError> {
        if router.generation == 0
            || router.generation != mirror.generation
            || router.generation != pages.generation()
            || router.sq8_sha256 != mirror.object_sha256
            || router.sq8_sha256 != pages.object_sha256()
            || router.router.rows() != mirror.geometry.rows
            || router.router.rows() != pages.rows()
            || router.router.dimensions() != mirror.geometry.dimensions
            || router.router.dimensions() != pages.dimensions()
            || router.router.page_rows() != pages.page_rows()
            || router.low.len() != mirror.low.len()
            || router.step.len() != mirror.step.len()
            || router
                .low
                .iter()
                .zip(&mirror.low)
                .any(|(left, right)| left.to_bits() != right.to_bits())
            || router
                .step
                .iter()
                .zip(&mirror.step)
                .any(|(left, right)| left.to_bits() != right.to_bits())
            || object_key.is_empty()
            || etag.is_empty()
        {
            return Err(GenerationError::IdentityMismatch);
        }
        Ok(Self {
            router,
            mirror,
            pages,
            object_key,
            etag,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exact_sq8_nominee::Sq8Geometry;
    use crate::pq64_nominee::Pq64Router;
    use sha2::{Digest, Sha256};

    #[test]
    fn same_shape_from_another_generation_fails_before_serving() {
        let object = vec![7u8; 13];
        let object_sha = format!("{:x}", Sha256::digest(&object));
        let sidecar = Sha256::digest(&object).to_vec();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2",
            "generation":1,"rows":1,"dimensions":1,"page_rows":1,
            "object_sha256":object_sha,
            "page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar)),
        }))
        .unwrap();
        let pages = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        let router = SourceRouterArtifact {
            router: Pq64Router::new(1, 1, 1, 1, vec![0.0], vec![0.0; 64 * 256], vec![0; 64])
                .unwrap(),
            low: vec![0.0],
            step: vec![1.0],
            generation: 1,
            source_sha256: "a".repeat(64),
            layout_sha256: "b".repeat(64),
            sq8_sha256: pages.object_sha256().to_owned(),
        };
        let mut mirror = MirrorManifest {
            format_version: 1,
            generation: 1,
            max_nominees: 1,
            geometry: Sq8Geometry {
                rows: 1,
                dimensions: 1,
            },
            object_sha256: pages.object_sha256().to_owned(),
            block_digest_sha256: "c".repeat(64),
            low: vec![0.0],
            step: vec![1.0],
        };
        assert!(
            ServingGeneration::bind(&router, &mirror, &pages, "index/sq8.bin", "etag-1").is_ok()
        );
        mirror.generation = 2;
        assert_eq!(
            ServingGeneration::bind(&router, &mirror, &pages, "index/sq8.bin", "etag-1").err(),
            Some(GenerationError::IdentityMismatch)
        );
        mirror.generation = 1;
        mirror.object_sha256 = "d".repeat(64);
        assert_eq!(
            ServingGeneration::bind(&router, &mirror, &pages, "index/sq8.bin", "etag-1").err(),
            Some(GenerationError::IdentityMismatch)
        );
    }
}
