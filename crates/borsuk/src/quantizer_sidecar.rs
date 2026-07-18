//! Persistence for the IVF coarse quantizer as a small on-storage object.
//!
//! On a WARMED/resident index the coarse quantizer (an HNSW over the cell
//! centroids) is built in memory from the resident routing summaries. A
//! COLD/paged index — queried straight off object storage without warming,
//! BORSUK's near-zero-RAM path — has no resident summaries, so historically it
//! got no quantizer and fell back to the paged routing tree, which degrades on
//! high-dimensional data (the curse of dimensionality: centroid+radius bubbles
//! overlap so nothing prunes).
//!
//! This module lets compaction persist the quantizer as one content-addressed
//! object so a cold query can load it with a single read and route to the
//! `nprobe` nearest cells without pulling every centroid resident. The object
//! carries the cell centroids + HNSW adjacency (inside [`CentroidHnsw`]) plus
//! the full [`SegmentSummary`] per cell — the summaries are the references a
//! search needs to then read only the probed cells' segments.
//!
//! Format: the [`PersistedQuantizer`] struct is serialized with `serde_json`
//! (which round-trips `f32` losslessly via the shortest round-trippable
//! representation, so the loaded graph routes bit-identically to the resident
//! one) and then zstd-compressed. The object is small: centroids + adjacency +
//! light per-cell records, never the full vectors. It is content-addressed by
//! the BLAKE3 checksum of the compressed bytes.

use crate::centroid_hnsw::CentroidHnsw;
use crate::error::{BorsukError, Result};
use crate::manifest::SegmentSummary;

/// zstd level for the quantizer object. Modest: the object is small and written
/// once per compaction, so a low level keeps the write cheap.
const QUANTIZER_ZSTD_LEVEL: i32 = 3;

/// The on-storage coarse-quantizer object: the HNSW over cell centroids plus the
/// per-cell segment summaries it indexes (node `i` routes to `summaries[i]`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistedQuantizer {
    /// Per-cell segment summaries, in cell order. Node `i` of `graph` indexes
    /// `summaries[i]`; a probe returns these summaries so the search reads only
    /// the probed cells' segments.
    pub summaries: Vec<SegmentSummary>,
    /// HNSW over the (routing-geometry) cell centroids. Self-contained: it holds
    /// the centroid vectors and adjacency, so `nearest` needs nothing resident.
    pub graph: CentroidHnsw,
}

impl PersistedQuantizer {
    /// Serialize to the content-addressed object bytes (serde_json + zstd).
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(self).map_err(|err| {
            BorsukError::InvalidStorage(format!("failed to serialize quantizer: {err}"))
        })?;
        zstd::encode_all(json.as_slice(), QUANTIZER_ZSTD_LEVEL).map_err(|err| {
            BorsukError::InvalidStorage(format!("failed to compress quantizer: {err}"))
        })
    }

    /// Parse the object bytes back into a quantizer. Returns an error (never
    /// panics) on corrupt bytes so the loader can fall back to the tree path.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let json = zstd::decode_all(bytes).map_err(|err| {
            BorsukError::InvalidStorage(format!("failed to decompress quantizer: {err}"))
        })?;
        let quantizer: PersistedQuantizer = serde_json::from_slice(&json).map_err(|err| {
            BorsukError::InvalidStorage(format!("failed to parse quantizer: {err}"))
        })?;
        // Defend the search-time walk against a truncated/inconsistent object:
        // the graph must be structurally sound and index exactly the summaries.
        if !quantizer.graph.is_structurally_valid()
            || quantizer.graph.node_count() != quantizer.summaries.len()
        {
            return Err(BorsukError::InvalidStorage(
                "persisted quantizer graph does not match its summaries".to_string(),
            ));
        }
        Ok(quantizer)
    }
}

/// Content-addressed path of a quantizer object, keyed by the checksum of its
/// bytes (so a superseded quantizer lands at a distinct path and GC reclaims it).
pub(crate) fn quantizer_relative_path(checksum: &str) -> String {
    let prefix = &checksum[..2];
    format!("quantizer/{prefix}/quant-{checksum}.zst")
}

/// Whether a path is a quantizer object (used by GC to list the prefix).
pub(crate) fn is_quantizer_path(path: &str) -> bool {
    path.starts_with("quantizer/") && path.ends_with(".zst")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(id: &str, centroid: Vec<f32>) -> SegmentSummary {
        SegmentSummary {
            id: id.to_string(),
            level: 0,
            path: format!("segments/{id}.parquet"),
            object_count: 4,
            dimensions: centroid.len(),
            centroid,
            radius: 1.0,
            bounds_min: Vec::new(),
            bounds_max: Vec::new(),
            checksum: format!("cs-{id}"),
            size_bytes: 10,
            graph_path: format!("graphs/{id}.parquet"),
            graph_checksum: format!("gcs-{id}"),
            graph_size_bytes: 5,
            leaf_mode: crate::record::LeafMode::PqScan,
            id_bloom: Vec::new(),
            vector_signature_bloom: Vec::new(),
            metadata_stats: crate::MetadataStats::default(),
            sparse_encoded: 0,
            dense_encoded: 4,
            text_doc_count: 0,
            text_total_doc_length: 0,
            created_at: chrono::Utc::now(),
        }
    }

    fn centroids(n: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| {
                (0..dim)
                    .map(|d| ((i * 31 + d * 7) % 97) as f32 / 97.0)
                    .collect()
            })
            .collect()
    }

    #[test]
    fn round_trip_preserves_routing() {
        let dim = 12;
        let cs = centroids(80, dim);
        let summaries: Vec<SegmentSummary> = cs
            .iter()
            .enumerate()
            .map(|(i, c)| summary(&format!("s{i}"), c.clone()))
            .collect();
        let graph = CentroidHnsw::build(&cs).unwrap();
        let persisted = PersistedQuantizer {
            summaries: summaries.clone(),
            graph: graph.clone(),
        };
        let bytes = persisted.encode().unwrap();
        let loaded = PersistedQuantizer::decode(&bytes).unwrap();

        // Same summaries, and the loaded graph routes identically for every
        // query — the whole correctness contract of the cold path.
        assert_eq!(loaded.summaries.len(), summaries.len());
        for q in cs.iter() {
            assert_eq!(graph.nearest(q, 8), loaded.graph.nearest(q, 8));
        }
    }

    #[test]
    fn decode_rejects_corrupt_bytes() {
        assert!(PersistedQuantizer::decode(b"not a quantizer object").is_err());
    }

    #[test]
    fn path_is_content_addressed_and_recognized() {
        let path = quantizer_relative_path("abcdef0123456789");
        assert_eq!(path, "quantizer/ab/quant-abcdef0123456789.zst");
        assert!(is_quantizer_path(&path));
        assert!(!is_quantizer_path("segments/foo.parquet"));
    }
}
