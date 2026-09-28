//! Assemble a generation and canonical source from immutable source/SQ8 snapshots.
use crate::{
    canonical_source::write_canonical_source,
    object_native_generation::valid_object_key,
    resident_vector_graph::HashingReader,
    two_bit_generation::{Manifest, TwoBitGenerationError},
    two_bit_source::TwoBitSource,
    unit_centroid_graph::UnitCentroidGraph,
    unit_centroid_pages::UnitCentroidPages,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::Path,
};
type Result<T> = std::result::Result<T, TwoBitGenerationError>;
/// Approved source-only preparation. No query/truth input or layout refitting.
pub struct TwoBitGenerationBuilder<'a> {
    /// Raw ordinal source and physically ordered SQ8 identities/geometry.
    pub source: TwoBitSource<'a>,
    /// Captured sealed control epoch; zero only for initial publication.
    pub base_epoch: u64,
    /// Positive generation ID.
    pub generation: u64,
    /// Approved SQ8 coordinate offsets (same encoding as the supplied body).
    pub low: &'a [f32],
    /// Approved positive SQ8 coordinate steps.
    pub step: &'a [f32],
    /// Application-authorized immutable remote SQ8 key ending objects/SHA.
    pub sq8_object_key: &'a str,
    /// Strong remote SQ8 ETag; publication checks HEAD, serving uses If-Match.
    pub sq8_etag: &'a str,
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(TwoBitGenerationError::Io)?;
    file.write_all(bytes).map_err(TwoBitGenerationError::Io)?;
    file.sync_all().map_err(TwoBitGenerationError::Io)
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
impl TwoBitGenerationBuilder<'_> {
    /// Build in a new directory and return the completed root SHA. Root is
    /// renamed last; failures leave unpublished scratch for caller cleanup.
    /// Payload model includes streamed buffers and conservative graph workspace;
    /// runtime/allocator/OS cache and other pinned generations are caller charges.
    pub fn build(&self, output: &Path, max_build_payload_bytes: usize) -> Result<String> {
        self.build_inner(None, output, max_build_payload_bytes)
    }

    /// Assemble a generation with an explicit source-row order and logical SQ8 IDs.
    /// Caller-owned order is included in payload admission; no ID map is hydrated.
    pub fn build_with_order(
        &self,
        order: &[u64],
        output: &Path,
        max_build_payload_bytes: usize,
    ) -> Result<String> {
        self.build_inner(Some(order), output, max_build_payload_bytes)
    }

    fn build_inner(
        &self,
        order: Option<&[u64]>,
        output: &Path,
        max_build_payload_bytes: usize,
    ) -> Result<String> {
        let bad = TwoBitGenerationError::Invalid;
        let rows = self.source.rows;
        let dimensions = self.source.dimensions;
        if self
            .sq8_object_key
            .len()
            .checked_add(self.sq8_etag.len())
            .is_none_or(|n| n > 65536)
        {
            return Err(bad("root identity length"));
        }
        if rows == 0
            || dimensions == 0
            || order.is_some_and(|order| order.len() != rows)
            || self.generation == 0
            || output.exists()
            || self.low.len() != dimensions
            || self.step.len() != dimensions
            || self.low.iter().any(|v| !v.is_finite())
            || self.step.iter().any(|v| !v.is_finite() || *v <= 0.)
            || !valid_object_key(self.sq8_object_key, self.source.sq8_sha256)
            || self.sq8_etag.is_empty()
            || self.sq8_etag.chars().any(char::is_control)
        {
            return Err(bad("build inputs"));
        }
        let units = rows.div_ceil(32);
        let pages = rows.div_ceil(256);
        let page_bytes = dimensions
            .checked_add(12)
            .and_then(|n| n.checked_mul(256))
            .ok_or(bad("page geometry"))?;
        // Covers f16 blob/f32 decode/build copies, adjacency (levels capped17
        // in the existing builder), visit/order arrays, heap and serialization.
        // Conservative model; measured process RSS is a separate qualification.
        let graph_bytes = dimensions
            .checked_mul(32)
            .and_then(|n| n.checked_add(16384))
            .and_then(|n| n.checked_mul(units))
            .ok_or(bad("graph build memory"))?;
        let modeled = dimensions
            .checked_mul(128)
            .and_then(|n| n.checked_add(graph_bytes))
            .and_then(|n| n.checked_add(pages.checked_mul(32)?))
            .and_then(|n| n.checked_add(page_bytes))
            .and_then(|n| n.checked_add(262144))
            .and_then(|n| {
                n.checked_add(if order.is_some() {
                    rows.checked_mul(8)?
                } else {
                    0
                })
            })
            .ok_or(bad("build memory"))?;
        if modeled > max_build_payload_bytes || units > u32::MAX as usize {
            return Err(bad("build memory budget"));
        }
        fs::create_dir(output).map_err(TwoBitGenerationError::Io)?;
        match order {
            Some(order) => {
                self.source
                    .build_with_order(order, &output.join("plane"), max_build_payload_bytes)
            }
            None => self
                .source
                .build(&output.join("plane"), max_build_payload_bytes),
        }
        .map_err(TwoBitGenerationError::Plane)?;
        let canonical = write_canonical_source(
            &self.source,
            order,
            &output.join("canonical.bin"),
            self.sq8_object_key,
        )
        .map_err(TwoBitGenerationError::Plane)?;
        let mut sq8 = BufReader::with_capacity(
            65536,
            HashingReader {
                inner: File::open(self.source.sq8).map_err(TwoBitGenerationError::Io)?,
                digest: Sha256::new(),
            },
        );
        let centroid = UnitCentroidPages::build_from_sq8_reader(
            &mut sq8, rows, dimensions, 32, 256, self.low, self.step,
        )
        .map_err(TwoBitGenerationError::Centroid)?;
        if format!("{:x}", sq8.into_inner().digest.finalize()) != self.source.sq8_sha256 {
            return Err(bad("SQ8 centroid input identity"));
        }
        let centers =
            UnitCentroidPages::decode(&centroid).map_err(TwoBitGenerationError::Centroid)?;
        let graph = UnitCentroidGraph::build(&centers, &centroid)
            .map_err(TwoBitGenerationError::Graph)?
            .encode()
            .map_err(TwoBitGenerationError::Graph)?;
        let graph_resident = UnitCentroidGraph::preflight_resident_bytes(&graph, &centers)
            .map_err(TwoBitGenerationError::Graph)?;
        let centroid_sha = hash(&centroid);
        let graph_sha = hash(&graph);
        write_new(&output.join("centroids.bin"), &centroid)?;
        write_new(&output.join("graph.bin"), &graph)?;
        drop(centers);
        drop(graph);
        drop(centroid);
        let mut input = File::open(self.source.sq8).map_err(TwoBitGenerationError::Io)?;
        let mut buffer = vec![0; page_bytes];
        let mut sidecar = Vec::with_capacity(pages * 32);
        let mut digest = Sha256::new();
        for page in 0..pages {
            let bytes = (rows - page * 256).min(256) * (dimensions + 12);
            input
                .read_exact(&mut buffer[..bytes])
                .map_err(TwoBitGenerationError::Io)?;
            digest.update(&buffer[..bytes]);
            sidecar.extend_from_slice(&Sha256::digest(&buffer[..bytes]));
        }
        if input.read(&mut [0]).map_err(TwoBitGenerationError::Io)? != 0
            || format!("{:x}", digest.finalize()) != self.source.sq8_sha256
        {
            return Err(bad("SQ8 page input identity"));
        }
        drop(buffer);
        write_new(&output.join("page_digests.bin"), &sidecar)?;
        let page_manifest = serde_json::to_vec(
            &serde_json::json!({"schema":"borsuk-v115-sq8-page-authority-v2",
            "generation":self.generation,"rows":rows,"dimensions":dimensions,"page_rows":256,
            "object_sha256":self.source.sq8_sha256,"page_digest_sha256":hash(&sidecar)}),
        )
        .map_err(|_| bad("page manifest encoding"))?;
        write_new(&output.join("page_manifest.json"), &page_manifest)?;
        let plane_body =
            fs::read(output.join("plane/manifest.json")).map_err(TwoBitGenerationError::Io)?;
        let body=serde_json::to_vec(&serde_json::json!({"schema":"borsuk-two-bit-generation-v3",
            "generation":self.generation,"base_epoch":self.base_epoch,"plane_manifest_sha256":hash(&plane_body),
            "page_manifest_sha256":hash(&page_manifest),"centroids_sha256":centroid_sha,
            "graph_sha256":graph_sha,
            "graph_resident_bytes":graph_resident,"sq8_object_sha256":self.source.sq8_sha256,
            "sq8_object_key":self.sq8_object_key,"sq8_etag":self.sq8_etag,"low":self.low,"step":self.step,
            "canonical":canonical}))
            .map_err(|_|bad("root encoding"))?;
        if body.len() > 65536 {
            return Err(bad("root manifest cap"));
        }
        // Parse the same strict manifest schema without loading the code plane.
        let _: Manifest = serde_json::from_slice(&body).map_err(|_| bad("root schema"))?;
        let root_sha = hash(&body);
        write_new(&output.join("manifest.pending"), &body)?;
        File::open(output)
            .and_then(|f| f.sync_all())
            .map_err(TwoBitGenerationError::Io)?;
        fs::rename(
            output.join("manifest.pending"),
            output.join("manifest.json"),
        )
        .map_err(TwoBitGenerationError::Io)?;
        Ok(root_sha)
    }
}
