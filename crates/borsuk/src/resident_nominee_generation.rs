//! Authenticated, generation-pinned full-roster FP16 search without vector-body GETs.

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    native_source_tier::decoded_sha256,
    physical_row_permutation::PhysicalRowPermutation,
    pq64_nominee::Pq64Error,
    pq64_router_artifact::SourceRouterArtifact,
    resident_fp16_tier::{ResidentFp16Error, ResidentFp16Tier},
};

/// One trusted root pins the router, physical map and resident vector plane.
/// The key and ETag identify the immutable S3 object to hydrate before the
/// plane's complete SHA-256 and generation header are checked.
pub struct ResidentNomineeAuthority {
    generation: u64,
    rows: usize,
    dimensions: usize,
    source_sha256: [u8; 32],
    router_manifest_sha256: [u8; 32],
    layout_sha256: [u8; 32],
    row_map_sha256: [u8; 32],
    resident_fp16_sha256: [u8; 32],
    plane_object_key: String,
    plane_etag: String,
}

/// A bad trusted root, mismatched generation, or invalid query fails closed.
#[derive(Debug, Error)]
pub enum ResidentNomineeError {
    #[error("resident nominee root SHA-256 differs")]
    RootHashMismatch,
    #[error("resident nominee generation identity differs")]
    IdentityMismatch,
    #[error("resident nominee request or root is invalid")]
    Invalid,
    #[error("resident nominee router rejected the request: {0:?}")]
    Router(Pq64Error),
    #[error("resident nominee FP16 tier rejected the request: {0}")]
    Tier(#[from] ResidentFp16Error),
}

fn hash(value: &Value) -> Result<[u8; 32], ResidentNomineeError> {
    let text = value.as_str().ok_or(ResidentNomineeError::Invalid)?;
    if text.len() != 64
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ResidentNomineeError::Invalid);
    }
    decoded_sha256(text).map_err(|_| ResidentNomineeError::Invalid)
}

impl ResidentNomineeAuthority {
    /// Authenticate the complete strict v1 root against a separately trusted
    /// digest before using any key, ETag or generation identity from it.
    pub fn load_authenticated(
        raw: &[u8],
        trusted_sha256: &str,
    ) -> Result<Self, ResidentNomineeError> {
        if raw.len() > 16 * 1024 {
            return Err(ResidentNomineeError::Invalid);
        }
        let trusted = hash(&Value::String(trusted_sha256.to_owned()))?;
        if Sha256::digest(raw)[..] != trusted {
            return Err(ResidentNomineeError::RootHashMismatch);
        }
        let value: Value =
            serde_json::from_slice(raw).map_err(|_| ResidentNomineeError::Invalid)?;
        let fields = value.as_object().ok_or(ResidentNomineeError::Invalid)?;
        const NAMES: [&str; 11] = [
            "schema",
            "generation",
            "rows",
            "dimensions",
            "source_sha256",
            "router_manifest_sha256",
            "layout_sha256",
            "row_map_sha256",
            "resident_fp16_sha256",
            "plane_object_key",
            "plane_etag",
        ];
        if fields.len() != NAMES.len()
            || NAMES.iter().any(|name| !fields.contains_key(*name))
            || value["schema"] != "borsuk-resident-nominee-generation-v1"
        {
            return Err(ResidentNomineeError::Invalid);
        }
        let positive = |name| -> Result<usize, ResidentNomineeError> {
            value[name]
                .as_u64()
                .and_then(|number| usize::try_from(number).ok())
                .filter(|number| *number > 0)
                .ok_or(ResidentNomineeError::Invalid)
        };
        let generation =
            u64::try_from(positive("generation")?).map_err(|_| ResidentNomineeError::Invalid)?;
        let rows = positive("rows")?;
        let dimensions = positive("dimensions")?;
        if rows > u32::MAX as usize {
            return Err(ResidentNomineeError::Invalid);
        }
        let plane_object_key = value["plane_object_key"]
            .as_str()
            .filter(|key| !key.is_empty())
            .ok_or(ResidentNomineeError::Invalid)?
            .to_owned();
        let plane_etag = value["plane_etag"]
            .as_str()
            .filter(|etag| !etag.is_empty())
            .ok_or(ResidentNomineeError::Invalid)?
            .to_owned();
        Ok(Self {
            generation,
            rows,
            dimensions,
            source_sha256: hash(&value["source_sha256"])?,
            router_manifest_sha256: hash(&value["router_manifest_sha256"])?,
            layout_sha256: hash(&value["layout_sha256"])?,
            row_map_sha256: hash(&value["row_map_sha256"])?,
            resident_fp16_sha256: hash(&value["resident_fp16_sha256"])?,
            plane_object_key,
            plane_etag,
        })
    }

    /// Immutable object key pinned by the trusted root.
    pub fn plane_object_key(&self) -> &str {
        &self.plane_object_key
    }

    /// Conditional ETag pinned by the trusted root.
    pub fn plane_etag(&self) -> &str {
        &self.plane_etag
    }

    /// Complete plane digest for authenticated hydration.
    pub fn resident_fp16_sha256(&self) -> [u8; 32] {
        self.resident_fp16_sha256
    }
}

/// One immutable query generation. A replacement generation can be built
/// separately and atomically published while outstanding readers keep this
/// instance pinned. RAM admission for concurrent generations is external.
pub struct ResidentNomineeGeneration<'a> {
    router: &'a SourceRouterArtifact,
    row_map: &'a PhysicalRowPermutation,
    plane: &'a ResidentFp16Tier,
    authority: &'a ResidentNomineeAuthority,
}

impl<'a> ResidentNomineeGeneration<'a> {
    /// Bind only fully authenticated artifacts with the same source,
    /// geometry, generation, layout and trusted root digests.
    pub fn bind(
        router: &'a SourceRouterArtifact,
        row_map: &'a PhysicalRowPermutation,
        plane: &'a ResidentFp16Tier,
        authority: &'a ResidentNomineeAuthority,
    ) -> Result<Self, ResidentNomineeError> {
        let router_digest = decoded_sha256(&router.manifest_sha256)
            .map_err(|_| ResidentNomineeError::IdentityMismatch)?;
        let router_source = decoded_sha256(&router.source_sha256)
            .map_err(|_| ResidentNomineeError::IdentityMismatch)?;
        let router_layout = decoded_sha256(&router.layout_sha256)
            .map_err(|_| ResidentNomineeError::IdentityMismatch)?;
        let router_sq8 = decoded_sha256(&router.sq8_sha256)
            .map_err(|_| ResidentNomineeError::IdentityMismatch)?;
        if router.generation != authority.generation
            || row_map.generation() != authority.generation
            || plane.generation() != authority.generation
            || router.router.rows() != authority.rows
            || row_map.rows() != authority.rows
            || plane.rows() != authority.rows
            || router.router.dimensions() != authority.dimensions
            || plane.dimensions() != authority.dimensions
            || router_digest != authority.router_manifest_sha256
            || row_map.router_manifest_sha256() != authority.router_manifest_sha256
            || router_source != authority.source_sha256
            || row_map.source_sha256() != authority.source_sha256
            || plane.source_sha256() != authority.source_sha256
            || router_layout != authority.layout_sha256
            || row_map.artifact_sha256() != authority.row_map_sha256
            || plane.artifact_sha256() != authority.resident_fp16_sha256
            || row_map.old_sq8_sha256() != router_sq8
        {
            return Err(ResidentNomineeError::IdentityMismatch);
        }
        Ok(Self {
            router,
            row_map,
            plane,
            authority,
        })
    }

    /// Rank every nominated vector from the resident plane. There are no
    /// object-body GETs or byte/GET caps in this query path.
    pub fn search_cosine(
        &self,
        query: &[f32],
        regions: usize,
        shortlist: usize,
        top_k: usize,
    ) -> Result<Vec<u64>, ResidentNomineeError> {
        if top_k == 0 || top_k > shortlist || shortlist > self.authority.rows {
            return Err(ResidentNomineeError::Invalid);
        }
        let old_rows = self
            .router
            .router
            .nominate(query, regions, shortlist)
            .map_err(ResidentNomineeError::Router)?;
        let physical = old_rows
            .into_iter()
            .map(|old| {
                let old = u32::try_from(old).map_err(|_| ResidentNomineeError::Invalid)?;
                self.row_map
                    .old_to_new(old)
                    .map(|new| new as usize)
                    .ok_or(ResidentNomineeError::IdentityMismatch)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self.plane.rank_ordinals_cosine(query, &physical, top_k)?)
    }

    /// Generation number pinned for the lifetime of this query instance.
    pub fn generation(&self) -> u64 {
        self.authority.generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        physical_row_permutation::{RowMapBinding, write_row_permutation},
        pq64_nominee::Pq64Router,
        resident_fp16_tier::write_resident_fp16_tier,
    };

    fn root(router: &SourceRouterArtifact, row_map_sha256: &str, plane_sha256: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-resident-nominee-generation-v1",
            "generation":router.generation,
            "rows":router.router.rows(),
            "dimensions":router.router.dimensions(),
            "source_sha256":router.source_sha256,
            "router_manifest_sha256":router.manifest_sha256,
            "layout_sha256":router.layout_sha256,
            "row_map_sha256":row_map_sha256,
            "resident_fp16_sha256":plane_sha256,
            "plane_object_key":"generations/7/resident-fp16.bin",
            "plane_etag":"etag-7",
        }))
        .unwrap()
    }

    fn authenticate(raw: &[u8]) -> ResidentNomineeAuthority {
        ResidentNomineeAuthority::load_authenticated(raw, &format!("{:x}", Sha256::digest(raw)))
            .unwrap()
    }

    #[test]
    fn authenticated_generation_ranks_full_relaid_roster_in_ram() {
        let directory = tempfile::tempdir().unwrap();
        let router = SourceRouterArtifact {
            router: Pq64Router::new(
                2,
                2,
                1,
                1,
                vec![0.0; 4],
                vec![0.0; 64 * 256],
                vec![0; 2 * 64],
            )
            .unwrap(),
            low: vec![0.0; 2],
            step: vec![1.0; 2],
            generation: 7,
            manifest_sha256: "b".repeat(64),
            source_sha256: "a".repeat(64),
            layout_sha256: "c".repeat(64),
            sq8_sha256: "d".repeat(64),
        };
        let map_path = directory.path().join("row-map.bin");
        let new_sq8 = "e".repeat(64);
        let binding = RowMapBinding {
            generation: 7,
            rows: 2,
            source_sha256: &router.source_sha256,
            router_manifest_sha256: &router.manifest_sha256,
            old_sq8_sha256: &router.sq8_sha256,
            new_sq8_sha256: &new_sq8,
        };
        let map_sha = write_row_permutation(&map_path, binding, &[1, 0]).unwrap();
        let row_map =
            PhysicalRowPermutation::open_authenticated(&map_path, &map_sha, binding).unwrap();
        let plane_path = directory.path().join("resident-fp16.bin");
        let plane_sha = write_resident_fp16_tier(
            &plane_path,
            2,
            2,
            7,
            &router.source_sha256,
            [(7, vec![0.0, 1.0]), (42, vec![1.0, 0.0])].into_iter(),
        )
        .unwrap();
        let plane = ResidentFp16Tier::open_authenticated(
            &plane_path,
            &plane_sha,
            &router.source_sha256,
            2,
            2,
            7,
            32,
        )
        .unwrap();
        let raw = root(&router, &map_sha, &plane_sha);
        let authority = authenticate(&raw);
        assert_eq!(
            authority.plane_object_key(),
            "generations/7/resident-fp16.bin"
        );
        assert_eq!(authority.plane_etag(), "etag-7");
        assert_eq!(authority.resident_fp16_sha256(), plane.artifact_sha256());
        let generation =
            ResidentNomineeGeneration::bind(&router, &row_map, &plane, &authority).unwrap();
        assert_eq!(generation.generation(), 7);
        assert_eq!(
            generation.search_cosine(&[1.0, 0.0], 2, 2, 2).unwrap(),
            vec![42, 7]
        );
        assert!(generation.search_cosine(&[1.0, 0.0], 2, 2, 3).is_err());
        assert!(generation.search_cosine(&[0.0, 0.0], 2, 2, 1).is_err());

        let wrong_raw = root(&router, &map_sha, &"f".repeat(64));
        let wrong = authenticate(&wrong_raw);
        assert!(matches!(
            ResidentNomineeGeneration::bind(&router, &row_map, &plane, &wrong),
            Err(ResidentNomineeError::IdentityMismatch)
        ));
        assert!(matches!(
            ResidentNomineeAuthority::load_authenticated(&raw, &"f".repeat(64)),
            Err(ResidentNomineeError::RootHashMismatch)
        ));
        let mut modified: Value = serde_json::from_slice(&raw).unwrap();
        modified["extra"] = Value::Bool(true);
        let modified = serde_json::to_vec(&modified).unwrap();
        assert!(matches!(
            ResidentNomineeAuthority::load_authenticated(
                &modified,
                &format!("{:x}", Sha256::digest(&modified))
            ),
            Err(ResidentNomineeError::Invalid)
        ));
    }
}
