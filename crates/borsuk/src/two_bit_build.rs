//! Assemble a generation and canonical source from immutable source/SQ8 snapshots.
use crate::semantic_unit_router::SemanticProfile;
use crate::{
    canonical_source::write_canonical_source,
    object_native_generation::valid_object_key,
    resident_vector_graph::HashingReader,
    two_bit_generation::{
        Discovery, DiscoveryMode, Manifest, SCHEMA, TwoBitGeneration, TwoBitGenerationError,
        TwoBitGenerationLimits,
    },
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
/// Authenticated construction evidence for importing an unchanged fitted router.
pub struct SemanticRouterImport<'a> {
    /// Complete original router artifacts.
    pub artifacts: &'a crate::semantic_unit_router::RouterArtifacts,
    /// Original construction identity, preserved byte-for-byte in the router.
    pub input: crate::semantic_unit_router::SourceIdentity<'a>,
    /// Original generation root body bound by input.root_sha256 (at most 64 KiB).
    pub original_root: &'a [u8],
    /// Original plane receipt authenticated by that original root (at most 64 KiB).
    pub original_plane: &'a [u8],
}
impl SemanticRouterImport<'_> {
    fn validate(
        &self,
        current: &crate::two_bit_source::SourcePlaneReceipt,
        centroid_sha: &str,
        low: &[f32],
        step: &[f32],
    ) -> Result<()> {
        let bad = TwoBitGenerationError::Invalid;
        if self.original_root.len() > 65536
            || self.original_plane.len() > 65536
            || hash(self.original_root) != self.input.root_sha256
            || self.input.rows != current.rows
            || self.input.dimensions != current.dimensions
            || self.input.centroids_sha256 != centroid_sha
        {
            return Err(bad("router import root/geometry"));
        }
        // Publication evidence only; these historical envelopes are never serving readers.
        let root: serde_json::Value =
            serde_json::from_slice(self.original_root).map_err(|_| bad("router import root"))?;
        let plane: serde_json::Value =
            serde_json::from_slice(self.original_plane).map_err(|_| bad("router import plane"))?;
        // The source coefficients are f32; historical JSON may spell the same
        // f32 value with different f64 precision. Authenticate the actual bits.
        let same_coefficients = |value: &serde_json::Value, expected: &[f32]| {
            value.as_array().is_some_and(|values| {
                values.len() == expected.len()
                    && values.iter().zip(expected).all(|(value, expected)| {
                        value.as_f64().is_some_and(|value| {
                            let value = value as f32;
                            value.is_finite() && value.to_bits() == expected.to_bits()
                        })
                    })
            })
        };
        if root["schema"].as_str() != Some(self.input.schema)
            || root["plane_manifest_sha256"].as_str() != Some(&hash(self.original_plane))
            || root["centroids_sha256"].as_str() != Some(centroid_sha)
            || root["sq8_object_sha256"].as_str() != Some(&current.sq8_sha256)
            || !same_coefficients(&root["low"], low)
            || !same_coefficients(&root["step"], step)
        {
            return Err(bad("router import authority"));
        }
        let current = serde_json::to_value(current).map_err(|_| bad("router import source"))?;
        for key in [
            "rows",
            "dimensions",
            "seed",
            "record_bytes",
            "source_sha256",
            "source_order_sha256",
            "sq8_sha256",
            "mean_sha256",
            "records_sha256",
        ] {
            if plane[key].is_null() || plane[key] != current[key] {
                return Err(bad("router import source mismatch"));
            }
        }
        Ok(())
    }
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
fn finish_root(output: &Path, body: &[u8]) -> Result<String> {
    let bad = TwoBitGenerationError::Invalid;
    if body.len() > 65536 {
        return Err(bad("root manifest cap"));
    }
    // Parse the same strict manifest schema without loading the code plane.
    let _: Manifest = serde_json::from_slice(body).map_err(|_| bad("root schema"))?;
    let root_sha = hash(body);
    write_new(&output.join("manifest.pending"), body)?;
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
fn write_semantic_router(
    output: &Path,
    artifacts: &crate::semantic_unit_router::RouterArtifacts,
    input: &crate::semantic_unit_router::SourceIdentity<'_>,
    plane: &crate::two_bit_source::SourcePlaneReceipt,
    centroid: &[u8],
) -> Result<Discovery> {
    if artifacts.manifest.len() > input.profile.root_cap() {
        return Err(TwoBitGenerationError::Invalid("router root cap"));
    }
    crate::semantic_unit_router::validate_publication(
        &artifacts.manifest,
        &artifacts.membership,
        &artifacts.leaves,
        input,
        centroid,
    )
    .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
    fs::create_dir(output.join("router")).map_err(TwoBitGenerationError::Io)?;
    for (name, bytes) in [
        ("root.bin", &artifacts.manifest),
        ("membership.bin", &artifacts.membership),
        ("leaves.bin", &artifacts.leaves),
    ] {
        write_new(&output.join("router").join(name), bytes)?;
    }
    // Publication-only proof; never uploaded or staged by serving.
    write_new(&output.join("centroids.bin"), centroid)?;
    Ok(Discovery::Semantic {
        profile: input.profile,
        root_sha256: hash(&artifacts.manifest),
        root_bytes: artifacts.manifest.len(),
        membership_sha256: hash(&artifacts.membership),
        membership_bytes: artifacts.membership.len(),
        leaves_sha256: hash(&artifacts.leaves),
        leaves_bytes: artifacts.leaves.len(),
        input_schema: input.schema.into(),
        input_root_sha256: input.root_sha256.into(),
        centroids_sha256: input.centroids_sha256.into(),
        source_sha256: plane.source_sha256.clone(),
        source_order_sha256: plane.source_order_sha256.clone(),
        mean_sha256: plane.mean_sha256.clone(),
        records_sha256: plane.records_sha256.clone(),
        sq8_sha256: plane.sq8_sha256.clone(),
    })
}
/// Repackage an authenticated current graph generation with an unchanged fitted
/// semantic router. Copies source/canonical/page authorities byte-for-byte into
/// a new directory; only the discovery envelope changes. Does not encode rows,
/// reorder IDs, fit a router, upload objects, or read historical serving formats.
/// Inputs are immutable caller snapshots; limits include caller-retained pins.
pub fn repackage_semantic_router(
    current: &Path,
    trusted_root_sha256: &str,
    sq8_path: &Path,
    import: &SemanticRouterImport<'_>,
    output: &Path,
    limits: TwoBitGenerationLimits,
) -> Result<String> {
    use crate::two_bit_source::{SourcePlaneReceipt, read_authenticated};
    let bad = TwoBitGenerationError::Invalid;
    if output.exists() || import.artifacts.manifest.len() > import.input.profile.root_cap() {
        return Err(bad("repackage output/router cap"));
    }
    let imported_bytes = import
        .artifacts
        .manifest
        .capacity()
        .checked_add(import.artifacts.membership.capacity())
        .and_then(|n| n.checked_add(import.artifacts.leaves.capacity()))
        .ok_or(bad("repackage input memory"))? as u64;
    let available = limits
        .max_memory_bytes
        .checked_sub(limits.already_pinned_bytes)
        .and_then(|n| n.checked_sub(imported_bytes))
        .ok_or(bad("repackage memory"))?;
    if available < 131072 {
        return Err(bad("repackage metadata admission"));
    }
    let read = |name: &str, digest: &str| -> Result<Vec<u8>> {
        let length = fs::metadata(current.join(name))
            .map_err(TwoBitGenerationError::Io)?
            .len();
        if length == 0 || length > 65536 {
            return Err(bad("repackage manifest cap"));
        }
        read_authenticated(&current.join(name), length as usize, digest)
            .map_err(TwoBitGenerationError::Plane)
    };
    let root_body = read("manifest.json", trusted_root_sha256)?;
    let mut root: Manifest =
        serde_json::from_slice(&root_body).map_err(|_| bad("repackage schema"))?;
    if root.schema != SCHEMA
        || import.input.profile != SemanticProfile::Native100k
        || root.discovery.mode() != DiscoveryMode::Graph
        || !root.canonical.valid()
        || !(1..=100_000).contains(&root.canonical.rows)
        || !(1..=768).contains(&root.canonical.dimensions)
    {
        return Err(bad("repackage current graph required"));
    }
    let rows = root.canonical.rows;
    let dimensions = root.canonical.dimensions;
    let geometry = crate::semantic_unit_router::Geometry {
        rows,
        dimensions,
        units: rows.div_ceil(32),
        blob_bytes: 32 + rows.div_ceil(32) * dimensions * 2,
    };
    crate::semantic_unit_router::admit(
        geometry,
        usize::try_from(available).map_err(|_| bad("repackage memory"))?,
        import.input.profile,
    )
    .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
    // Authenticate the full current source/graph under its ordinary admission,
    // then release it before constructing the publication proof.
    drop(TwoBitGeneration::open(
        current,
        trusted_root_sha256,
        TwoBitGenerationLimits {
            already_pinned_bytes: limits
                .already_pinned_bytes
                .checked_add(imported_bytes)
                .ok_or(bad("repackage memory"))?,
            ..limits
        },
    )?);
    let plane_body = read("plane/manifest.json", &root.plane_manifest_sha256)?;
    let plane: SourcePlaneReceipt =
        serde_json::from_slice(&plane_body).map_err(|_| bad("repackage plane"))?;
    let page_body = read("page_manifest.json", &root.page_manifest_sha256)?;
    let page: serde_json::Value =
        serde_json::from_slice(&page_body).map_err(|_| bad("repackage pages"))?;
    let file = File::open(sq8_path).map_err(TwoBitGenerationError::Io)?;
    if file.metadata().map_err(TwoBitGenerationError::Io)?.len()
        != (rows * (dimensions + 12)) as u64
    {
        return Err(bad("repackage SQ8 length"));
    }
    let mut sq8 = BufReader::with_capacity(
        65536,
        HashingReader {
            inner: file,
            digest: Sha256::new(),
        },
    );
    let centroid = UnitCentroidPages::build_from_sq8_reader(
        &mut sq8, rows, dimensions, 32, 256, &root.low, &root.step,
    )
    .map_err(TwoBitGenerationError::Centroid)?;
    if format!("{:x}", sq8.into_inner().digest.finalize()) != root.sq8_object_sha256 {
        return Err(bad("repackage SQ8 identity"));
    }
    let centroid_sha = hash(&centroid);
    let Discovery::Graph {
        centroids_sha256, ..
    } = &root.discovery
    else {
        unreachable!()
    };
    if *centroids_sha256 != centroid_sha {
        return Err(bad("repackage centroid identity"));
    }
    import.validate(&plane, &centroid_sha, &root.low, &root.step)?;
    fs::create_dir(output).map_err(TwoBitGenerationError::Io)?;
    fs::create_dir(output.join("plane")).map_err(TwoBitGenerationError::Io)?;
    for (name, length, digest) in [
        (
            "plane/manifest.json",
            plane_body.len() as u64,
            root.plane_manifest_sha256.as_str(),
        ),
        (
            "plane/mean.bin",
            (dimensions * 4) as u64,
            plane.mean_sha256.as_str(),
        ),
        (
            "plane/records.bin",
            (rows * plane.record_bytes) as u64,
            plane.records_sha256.as_str(),
        ),
        (
            "plane/page_digests.bin",
            (rows.div_ceil(32) * 32) as u64,
            plane.page_digest_sha256.as_str(),
        ),
        (
            "page_manifest.json",
            page_body.len() as u64,
            root.page_manifest_sha256.as_str(),
        ),
        (
            "page_digests.bin",
            (rows.div_ceil(256) * 32) as u64,
            page["page_digest_sha256"]
                .as_str()
                .ok_or(bad("repackage page digest"))?,
        ),
        (
            "canonical.bin",
            root.canonical.bytes,
            root.canonical.sha256.as_str(),
        ),
    ] {
        let mut input = HashingReader {
            inner: File::open(current.join(name)).map_err(TwoBitGenerationError::Io)?,
            digest: Sha256::new(),
        };
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output.join(name))
            .map_err(TwoBitGenerationError::Io)?;
        let copied = std::io::copy(&mut (&mut input).take(length + 1), &mut out)
            .map_err(TwoBitGenerationError::Io)?;
        if copied != length || format!("{:x}", input.digest.finalize()) != digest {
            return Err(bad("repackage copy identity"));
        }
        out.sync_all().map_err(TwoBitGenerationError::Io)?;
    }
    root.discovery =
        write_semantic_router(output, import.artifacts, &import.input, &plane, &centroid)?;
    let body = serde_json::to_vec(&root).map_err(|_| bad("repackage root encoding"))?;
    finish_root(output, &body)
}

impl TwoBitGenerationBuilder<'_> {
    /// Build in a new directory and return the completed root SHA. Root is
    /// renamed last; failures leave unpublished scratch for caller cleanup.
    /// Payload model includes streamed buffers and conservative graph workspace;
    /// runtime/allocator/OS cache and other pinned generations are caller charges.
    pub fn build(&self, output: &Path, max_build_payload_bytes: usize) -> Result<String> {
        self.build_inner(
            None,
            DiscoveryMode::Graph,
            SemanticProfile::Native100k,
            None,
            output,
            max_build_payload_bytes,
        )
    }

    /// Assemble a generation with an explicit source-row order and logical SQ8 IDs.
    /// Caller-owned order is included in payload admission; no ID map is hydrated.
    pub fn build_with_order(
        &self,
        order: &[u64],
        output: &Path,
        max_build_payload_bytes: usize,
    ) -> Result<String> {
        self.build_inner(
            Some(order),
            DiscoveryMode::Graph,
            SemanticProfile::Native100k,
            None,
            output,
            max_build_payload_bytes,
        )
    }

    /// Build the explicit discovery mode using the same source and SQ8 identities.
    pub fn build_with_discovery(
        &self,
        order: Option<&[u64]>,
        mode: DiscoveryMode,
        output: &Path,
        max_build_payload_bytes: usize,
    ) -> Result<String> {
        self.build_inner(
            order,
            mode,
            SemanticProfile::Native100k,
            None,
            output,
            max_build_payload_bytes,
        )
    }

    /// Source-only semantic construction under an explicit authenticated profile.
    pub fn build_with_semantic_profile(
        &self,
        order: Option<&[u64]>,
        profile: SemanticProfile,
        output: &Path,
        max_build_payload_bytes: usize,
    ) -> Result<String> {
        self.build_inner(
            order,
            DiscoveryMode::Semantic,
            profile,
            None,
            output,
            max_build_payload_bytes,
        )
    }

    /// Import a previously fitted router, authenticating its original root/plane
    /// receipts and exact current SQ8-derived FP16 means. Never refits the router.
    pub fn build_with_semantic_router(
        &self,
        order: Option<&[u64]>,
        import: &SemanticRouterImport<'_>,
        output: &Path,
        max_build_payload_bytes: usize,
    ) -> Result<String> {
        self.build_inner(
            order,
            DiscoveryMode::Semantic,
            import.input.profile,
            Some(import),
            output,
            max_build_payload_bytes,
        )
    }

    fn build_inner(
        &self,
        order: Option<&[u64]>,
        mode: DiscoveryMode,
        profile: SemanticProfile,
        import: Option<&SemanticRouterImport<'_>>,
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
            || (mode == DiscoveryMode::Semantic && !profile.valid_geometry(rows, dimensions))
            || order.is_some_and(|order| order.len() != rows)
            || self.generation == 0
            || output.exists()
            || self.low.len() != dimensions
            || self.step.len() != dimensions
            || self.low.iter().any(|v| !v.is_finite())
            || self.step.iter().any(|v| !v.is_finite() || *v <= 0.)
            || !valid_object_key(self.sq8_object_key, self.source.sq8_sha256)
            || self.sq8_etag.is_empty()
            || self.sq8_etag.starts_with("W/")
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
            .and_then(|n| {
                n.checked_add(if mode == DiscoveryMode::Graph {
                    graph_bytes
                } else {
                    0
                })
            })
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
        let semantic_memory = if mode == DiscoveryMode::Semantic {
            let geometry = crate::semantic_unit_router::Geometry {
                rows,
                dimensions,
                units,
                blob_bytes: 32 + units * dimensions * 2,
            };
            crate::semantic_unit_router::admit(
                geometry,
                max_build_payload_bytes.min(profile.allocation_cap()),
                profile,
            )
            .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?
        } else {
            0
        };
        if modeled
            .checked_add(semantic_memory)
            .is_none_or(|n| n > max_build_payload_bytes)
            || units > u32::MAX as usize
            || (mode == DiscoveryMode::Semantic && !profile.valid_geometry(rows, dimensions))
        {
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
        let plane_body =
            fs::read(output.join("plane/manifest.json")).map_err(TwoBitGenerationError::Io)?;
        let plane: crate::two_bit_source::SourcePlaneReceipt =
            serde_json::from_slice(&plane_body).map_err(|_| bad("plane receipt"))?;
        let discovery = if mode == DiscoveryMode::Graph {
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
            drop(graph);
            // Build sequentially: no first graph adjacency/blob is retained in the
            // second graph's construction workspace. The centroid plane is shared.
            let diverse_graph = UnitCentroidGraph::build_diverse(&centers, &centroid)
                .map_err(TwoBitGenerationError::Graph)?
                .encode()
                .map_err(TwoBitGenerationError::Graph)?;
            let diverse_graph_resident =
                UnitCentroidGraph::preflight_resident_bytes(&diverse_graph, &centers)
                    .map_err(TwoBitGenerationError::Graph)?;
            let diverse_graph_sha = hash(&diverse_graph);
            write_new(&output.join("diverse_graph.bin"), &diverse_graph)?;
            drop(diverse_graph);
            drop(centers);
            Discovery::Graph {
                centroids_sha256: centroid_sha,
                graph_sha256: graph_sha,
                graph_resident_bytes: graph_resident,
                diverse_graph_sha256: diverse_graph_sha,
                diverse_graph_resident_bytes: diverse_graph_resident,
            }
        } else {
            let centroid_sha = hash(&centroid);
            let receipt_sha = hash(&plane_body);
            let input = if let Some(import) = import {
                import.validate(&plane, &centroid_sha, self.low, self.step)?;
                crate::semantic_unit_router::SourceIdentity {
                    profile,
                    schema: import.input.schema,
                    root_sha256: import.input.root_sha256,
                    centroids_sha256: &centroid_sha,
                    rows,
                    dimensions,
                }
            } else {
                crate::semantic_unit_router::SourceIdentity {
                    profile,
                    schema: &plane.schema,
                    root_sha256: &receipt_sha,
                    centroids_sha256: &centroid_sha,
                    rows,
                    dimensions,
                }
            };
            let constructed;
            let artifacts = if let Some(import) = import {
                import.artifacts
            } else {
                constructed =
                    crate::semantic_unit_router::build(&centroid, &input, semantic_memory)
                        .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
                &constructed
            };
            write_semantic_router(output, artifacts, &input, &plane, &centroid)?
        };
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
        let body=serde_json::to_vec(&serde_json::json!({"schema":SCHEMA,
            "generation":self.generation,"base_epoch":self.base_epoch,"plane_manifest_sha256":hash(&plane_body),
            "page_manifest_sha256":hash(&page_manifest),"discovery":discovery,"sq8_object_sha256":self.source.sq8_sha256,
            "sq8_object_key":self.sq8_object_key,"sq8_etag":self.sq8_etag,"low":self.low,"step":self.step,
            "canonical":canonical}))
            .map_err(|_|bad("root encoding"))?;
        finish_root(output, &body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn imported_router_requires_original_source_authority() {
        let centroid_sha = "1".repeat(64);
        let plane = crate::two_bit_source::SourcePlaneReceipt {
            schema: "borsuk-two-bit-plane-v3".into(),
            rows: 33,
            dimensions: 2,
            seed: 1,
            record_bytes: 9,
            source_sha256: "2".repeat(64),
            sq8_sha256: "3".repeat(64),
            source_order_sha256: "4".repeat(64),
            mean_sha256: "5".repeat(64),
            records_sha256: "6".repeat(64),
            page_rows: 32,
            page_digest_sha256: "7".repeat(64),
            query_or_truth_used: false,
        };
        let mut original_plane = serde_json::to_value(&plane).unwrap();
        original_plane["schema"] = "borsuk-two-bit-plane-v2".into();
        original_plane.as_object_mut().unwrap().remove("page_rows");
        original_plane
            .as_object_mut()
            .unwrap()
            .remove("page_digest_sha256");
        let old_plane = serde_json::to_vec(&original_plane).unwrap();
        let old_root = serde_json::to_vec(&serde_json::json!({"schema":"borsuk-two-bit-generation-v4", "centroids_sha256":centroid_sha, "plane_manifest_sha256":hash(&old_plane), "sq8_object_sha256":plane.sq8_sha256, "low":[-0.1_f64,0.2_f64],"step":[0.1_f64,0.2_f64]})).unwrap();
        let low = [-0.1_f32, 0.2];
        let step = [0.1_f32, 0.2];
        let artifacts = crate::semantic_unit_router::RouterArtifacts {
            manifest: Vec::new(),
            membership: Vec::new(),
            leaves: Vec::new(),
        };
        let input_root = hash(&old_root);
        let import = SemanticRouterImport {
            artifacts: &artifacts,
            input: crate::semantic_unit_router::SourceIdentity {
                profile: SemanticProfile::Native100k,
                schema: "borsuk-two-bit-generation-v4",
                root_sha256: &input_root,
                centroids_sha256: &centroid_sha,
                rows: 33,
                dimensions: 2,
            },
            original_root: &old_root,
            original_plane: &old_plane,
        };
        import.validate(&plane, &centroid_sha, &low, &step).unwrap();
        assert_ne!(input_root, hash(&serde_json::to_vec(&plane).unwrap()));
        assert!(
            import
                .validate(&plane, &"8".repeat(64), &low, &step)
                .is_err()
        );
        assert!(
            import
                .validate(&plane, &centroid_sha, &low, &[2.; 2])
                .is_err()
        );
        for (changed_low, changed_step) in [
            ([f32::from_bits(low[0].to_bits() ^ 1), low[1]], step),
            (low, [f32::from_bits(step[0].to_bits() ^ 1), step[1]]),
            ([f32::NAN, low[1]], step),
            (low, [f32::INFINITY, step[1]]),
        ] {
            assert!(
                import
                    .validate(&plane, &centroid_sha, &changed_low, &changed_step)
                    .is_err()
            );
        }
        assert!(
            import
                .validate(&plane, &centroid_sha, &low[..1], &step)
                .is_err()
        );
        assert!(
            import
                .validate(&plane, &centroid_sha, &low, &step[..1])
                .is_err()
        );
        let mut changed = plane;
        changed.source_order_sha256 = "9".repeat(64);
        assert!(
            import
                .validate(&changed, &centroid_sha, &low, &step)
                .is_err()
        );
    }

    #[test]
    fn union_generation_has_two_authenticated_graphs() {
        let temp = tempfile::tempdir().unwrap();
        let raw = (0..512)
            .flat_map(|_| [0.5_f32, 0.25].into_iter().flat_map(f32::to_le_bytes))
            .collect::<Vec<_>>();
        let sq8 = (0..512_i64)
            .flat_map(|id| {
                let mut row = id.to_le_bytes().to_vec();
                row.extend(0.3125_f32.to_le_bytes());
                row.extend([128, 64]);
                row
            })
            .collect::<Vec<_>>();
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let source_sha = hash(&raw);
        let sq8_sha = hash(&sq8);
        let key = format!("test/objects/{sq8_sha}");
        let builder = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &source_sha,
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows: 512,
                dimensions: 2,
            },
            base_epoch: 0,
            generation: 1,
            low: &[0.; 2],
            step: &[1. / 255.; 2],
            sq8_object_key: &key,
            sq8_etag: "etag",
        };
        let output = temp.path().join("generation");
        let root_sha = builder.build(&output, 2_000_000).unwrap();
        let body = fs::read(output.join("manifest.json")).unwrap();
        assert_eq!(hash(&body), root_sha);
        let mut manifest: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            manifest["schema"], "borsuk-two-bit-generation-v8",
            "two-graph root format missing"
        );
        let nearest = fs::read(output.join("graph.bin")).unwrap();
        let diverse = fs::read(output.join("diverse_graph.bin")).unwrap();
        assert_eq!(manifest["discovery"]["graph_sha256"], hash(&nearest));
        assert_eq!(
            manifest["discovery"]["diverse_graph_sha256"],
            hash(&diverse)
        );
        assert!(
            manifest["discovery"]["graph_resident_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            manifest["discovery"]["diverse_graph_resident_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        use crate::two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits};
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 4_000_000,
            max_active_queries: 1,
            max_query_bytes: 16384,
            max_query_gets: 2,
            max_parallel_gets: 2,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 8192,
            already_pinned_bytes: 0,
        };
        assert!(TwoBitGeneration::open(&output, &root_sha, limits).is_ok());
        // Import proof is a historical construction receipt, independent of the
        // new root; repackaging must preserve every existing source payload bit.
        let mut original: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let graph = original
            .as_object_mut()
            .unwrap()
            .remove("discovery")
            .unwrap();
        for (key, value) in graph.as_object().unwrap() {
            if key != "mode" {
                original[key] = value.clone();
            }
        }
        original["schema"] = "borsuk-two-bit-generation-v4".into();
        let original_root = serde_json::to_vec(&original).unwrap();
        let original_plane = fs::read(output.join("plane/manifest.json")).unwrap();
        let original_sha = hash(&original_root);
        let centroid = fs::read(output.join("centroids.bin")).unwrap();
        let centroid_sha = hash(&centroid);
        let input = crate::semantic_unit_router::SourceIdentity {
            profile: SemanticProfile::Native100k,
            schema: "borsuk-two-bit-generation-v4",
            root_sha256: &original_sha,
            centroids_sha256: &centroid_sha,
            rows: 512,
            dimensions: 2,
        };
        let artifacts = crate::semantic_unit_router::build(&centroid, &input, 64_000_000).unwrap();
        let import = SemanticRouterImport {
            artifacts: &artifacts,
            input,
            original_root: &original_root,
            original_plane: &original_plane,
        };
        let repacked = temp.path().join("repacked");
        let repack_limits = TwoBitGenerationLimits {
            max_memory_bytes: 64_000_000,
            ..limits
        };
        let repacked_sha = repackage_semantic_router(
            &output,
            &root_sha,
            &sq8_path,
            &import,
            &repacked,
            repack_limits,
        )
        .unwrap();
        for name in [
            "plane/manifest.json",
            "plane/mean.bin",
            "plane/records.bin",
            "plane/page_digests.bin",
            "page_manifest.json",
            "page_digests.bin",
            "canonical.bin",
        ] {
            assert_eq!(
                fs::read(output.join(name)).unwrap(),
                fs::read(repacked.join(name)).unwrap(),
                "{name} reencoded"
            );
        }
        assert_eq!(
            fs::read(repacked.join("router/root.bin")).unwrap(),
            artifacts.manifest
        );
        assert_eq!(
            fs::read(repacked.join("router/membership.bin")).unwrap(),
            artifacts.membership
        );
        assert_eq!(
            fs::read(repacked.join("router/leaves.bin")).unwrap(),
            artifacts.leaves
        );
        assert!(!repacked.join("graph.bin").exists());
        assert!(!repacked.join("diverse_graph.bin").exists());
        assert_eq!(fs::read(output.join("manifest.json")).unwrap(), body);
        assert_eq!(fs::read(&sq8_path).unwrap(), sq8);
        assert_eq!(
            TwoBitGeneration::open(&repacked, &repacked_sha, repack_limits)
                .unwrap()
                .discovery_mode(),
            DiscoveryMode::Semantic
        );
        let rejected = temp.path().join("rejected-repack");
        fs::write(&sq8_path, vec![0; sq8.len()]).unwrap();
        assert!(
            repackage_semantic_router(
                &output,
                &root_sha,
                &sq8_path,
                &import,
                &rejected,
                repack_limits
            )
            .is_err()
        );
        assert!(!rejected.exists());
        fs::write(&sq8_path, &sq8).unwrap();

        // A declared second resident cannot bypass either preflight total
        // admission or exact structural identity, even with a new trusted root.
        let original_resident = manifest["discovery"]["diverse_graph_resident_bytes"].clone();
        for resident in [1_u64, u64::MAX] {
            manifest["discovery"]["diverse_graph_resident_bytes"] = resident.into();
            let bytes = serde_json::to_vec(&manifest).unwrap();
            fs::write(output.join("manifest.json"), &bytes).unwrap();
            assert!(TwoBitGeneration::open(&output, &hash(&bytes), limits).is_err());
        }
        manifest["discovery"]["diverse_graph_resident_bytes"] = original_resident;
        manifest["schema"] = "borsuk-two-bit-generation-v3".into();
        let bytes = serde_json::to_vec(&manifest).unwrap();
        fs::write(output.join("manifest.json"), &bytes).unwrap();
        assert!(TwoBitGeneration::open(&output, &hash(&bytes), limits).is_err());
        fs::write(output.join("manifest.json"), &body).unwrap();
        let mut corrupted = diverse.clone();
        corrupted[0] ^= 1;
        fs::write(output.join("diverse_graph.bin"), &corrupted).unwrap();
        assert!(TwoBitGeneration::open(&output, &root_sha, limits).is_err());
        fs::remove_file(output.join("diverse_graph.bin")).unwrap();
        assert!(TwoBitGeneration::open(&output, &root_sha, limits).is_err());
    }
}
