//! Assemble a generation and canonical source from immutable source/SQ8 snapshots.
use crate::semantic_unit_router::{LeafPartition, SemanticProfile};
use crate::{
    canonical_source::write_canonical_source,
    object_native_generation::valid_object_key,
    resident_vector_graph::HashingReader,
    sq8_page_authority::SQ8_PAGE_ROWS,
    two_bit_generation::{
        Discovery, DiscoveryMode, Manifest, SCHEMA, SourceLayout, SourceLayoutMode,
        TwoBitGeneration, TwoBitGenerationError, TwoBitGenerationLimits, capsule_closures,
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
// Three bounded receipts and parsed metadata coexist with the publication proof.
const REPACKAGE_METADATA_BYTES: usize = 3 * 65536 * 8 + 65536;
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
    fn retained_bytes(&self) -> Result<usize> {
        self.artifacts
            .manifest
            .capacity()
            .checked_add(self.artifacts.membership.capacity())
            .and_then(|n| n.checked_add(self.artifacts.leaves.capacity()))
            .and_then(|n| n.checked_add(self.original_root.len()))
            .and_then(|n| n.checked_add(self.original_plane.len()))
            .ok_or(TwoBitGenerationError::Invalid(
                "router import retained memory",
            ))
    }

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
    finish_root_with_sync(output, body, |_, file| file.sync_all())
}
fn finish_root_with_sync(
    output: &Path,
    body: &[u8],
    mut sync: impl FnMut(&Path, &File) -> std::io::Result<()>,
) -> Result<String> {
    let bad = TwoBitGenerationError::Invalid;
    if body.len() > 65536 {
        return Err(bad("root manifest cap"));
    }
    // Parse the same strict manifest schema without loading the code plane.
    let manifest: Manifest = serde_json::from_slice(body).map_err(|_| bad("root schema"))?;
    let root_sha = hash(body);
    let plane = output.join("plane");
    sync(
        &plane,
        &File::open(&plane).map_err(TwoBitGenerationError::Io)?,
    )
    .map_err(TwoBitGenerationError::Io)?;
    if manifest.discovery.mode() == DiscoveryMode::Semantic {
        let router = output.join("router");
        sync(
            &router,
            &File::open(&router).map_err(TwoBitGenerationError::Io)?,
        )
        .map_err(TwoBitGenerationError::Io)?;
    }
    let pending = output.join("manifest.pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)
        .map_err(TwoBitGenerationError::Io)?;
    file.write_all(body).map_err(TwoBitGenerationError::Io)?;
    sync(&pending, &file).map_err(TwoBitGenerationError::Io)?;
    fs::rename(&pending, output.join("manifest.json")).map_err(TwoBitGenerationError::Io)?;
    // A sync error after rename leaves an ambiguous root; preserve it for the caller.
    sync(
        output,
        &File::open(output).map_err(TwoBitGenerationError::Io)?,
    )
    .map_err(TwoBitGenerationError::Io)?;
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    sync(
        parent,
        &File::open(parent).map_err(TwoBitGenerationError::Io)?,
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
    let imported_bytes = import.retained_bytes()? as u64;
    let available = limits
        .max_memory_bytes
        .checked_sub(limits.already_pinned_bytes)
        .and_then(|n| n.checked_sub(imported_bytes))
        .and_then(|n| n.checked_sub(REPACKAGE_METADATA_BYTES as u64))
        .ok_or(bad("repackage memory"))?;
    if available < 131072 {
        return Err(bad("repackage metadata admission"));
    }
    crate::semantic_unit_router::admit_authenticated_root(
        &import.artifacts.manifest,
        &hash(&import.artifacts.manifest),
        &import.input,
        usize::try_from(available).map_err(|_| bad("repackage memory"))?,
    )
    .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
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
    // Authenticate the full current source/graph under its ordinary admission,
    // then release it before constructing the publication proof.
    drop(TwoBitGeneration::open(
        current,
        trusted_root_sha256,
        TwoBitGenerationLimits {
            already_pinned_bytes: limits
                .already_pinned_bytes
                .checked_add(imported_bytes)
                .and_then(|n| n.checked_add(REPACKAGE_METADATA_BYTES as u64))
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
            (rows.div_ceil(SQ8_PAGE_ROWS) * 32) as u64,
            page["page_digest_sha256"]
                .as_str()
                .ok_or(bad("repackage page digest"))?,
        ),
        (
            "canonical_row_digests.bin",
            root.canonical.row_digests_bytes as u64,
            root.canonical.row_digests_sha256.as_str(),
        ),
        (
            "canonical.bin",
            root.canonical.bytes,
            root.canonical.sha256.as_str(),
        ),
    ] {
        copy_authenticated(current, output, name, length, digest)?;
    }
    root.discovery =
        write_semantic_router(output, import.artifacts, &import.input, &plane, &centroid)?;
    let body = serde_json::to_vec(&root).map_err(|_| bad("repackage root encoding"))?;
    finish_root(output, &body)
}

fn digest_hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

// Streaming pack owns only one original page buffer; fixed router and codec metadata
// coexist under the same caller budget. The old flat body disappears before root seal.
fn pack_source_layout(
    output: &Path,
    discovery: &Discovery,
    plane: &crate::two_bit_source::SourcePlaneReceipt,
    generation: u64,
    cap: usize,
) -> Result<SourceLayout> {
    use crate::{
        source_capsule,
        two_bit_source::{TwoBitPlane, read_authenticated},
    };
    let bad = TwoBitGenerationError::Invalid;
    let Discovery::Semantic {
        profile,
        root_sha256,
        root_bytes,
        membership_sha256,
        membership_bytes,
        ..
    } = discovery
    else {
        return Err(bad("capsules require semantic discovery"));
    };
    let cap = cap.min(profile.allocation_cap());
    let retained = root_bytes
        .checked_mul(35)
        .and_then(|n| n.checked_add(membership_bytes.checked_mul(8)?))
        .and_then(|n| n.checked_add(plane.dimensions.checked_mul(256)?))
        .and_then(|n| n.checked_add(plane.rows.div_ceil(32).checked_mul(96)?))
        .and_then(|n| n.checked_add(131072))
        .ok_or(bad("capsule pack metadata"))?;
    let available = cap
        .checked_sub(retained)
        .ok_or(bad("capsule pack memory"))?;
    let geometry = source_capsule::Geometry::new(plane.rows, plane.dimensions, *profile)
        .map_err(TwoBitGenerationError::Plane)?;
    let input = discovery.input(plane)?;
    let router = crate::semantic_unit_router::SemanticUnitRouter::open(
        &read_authenticated(&output.join("router/root.bin"), *root_bytes, root_sha256)
            .map_err(TwoBitGenerationError::Plane)?,
        &read_authenticated(
            &output.join("router/membership.bin"),
            *membership_bytes,
            membership_sha256,
        )
        .map_err(TwoBitGenerationError::Plane)?,
        root_sha256,
        &input,
        profile.root_cap(),
    )
    .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?;
    let closures = capsule_closures(&router, available)?;
    let leaves = closures
        .iter()
        .enumerate()
        .map(|(id, pages)| source_capsule::LeafPages {
            leaf_id: id as u32,
            pages,
        })
        .collect::<Vec<_>>();
    let extra = closures
        .capacity()
        .checked_mul(std::mem::size_of::<Vec<u32>>())
        .and_then(|n| {
            n.checked_add(leaves.capacity() * std::mem::size_of::<source_capsule::LeafPages<'_>>())
        })
        .and_then(|n| {
            n.checked_add(
                closures
                    .iter()
                    .map(|p| (p.capacity() - p.len()) * 4)
                    .sum::<usize>(),
            )
        })
        .ok_or(bad("capsule pack capacities"))?;
    let available = available
        .checked_sub(extra)
        .ok_or(bad("capsule pack memory"))?;
    let charge =
        source_capsule::build_charge(geometry, &leaves).map_err(TwoBitGenerationError::Plane)?;
    if charge.memory_bytes > available {
        return Err(bad("capsule pack memory"));
    }
    let plane_body =
        fs::read(output.join("plane/manifest.json")).map_err(TwoBitGenerationError::Io)?;
    let (_codec, authority) = TwoBitPlane::open_metadata(
        &output.join("plane"),
        &hash(&plane_body),
        &plane.sq8_sha256,
        generation,
        retained,
    )
    .map_err(TwoBitGenerationError::Plane)?;
    let mut records =
        File::open(output.join("plane/records.bin")).map_err(TwoBitGenerationError::Io)?;
    let mut blob = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("plane/capsules.bin"))
        .map_err(TwoBitGenerationError::Io)?;
    let mut directory = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("plane/capsule_directory.bin"))
        .map_err(TwoBitGenerationError::Io)?;
    let receipt = source_capsule::pack(
        geometry,
        &leaves,
        &authority,
        &mut records,
        &mut blob,
        &mut directory,
        available,
    )
    .map_err(TwoBitGenerationError::Plane)?;
    drop(leaves);
    drop(closures);
    receipt
        .directory
        .validate_router(&router, available)
        .map_err(TwoBitGenerationError::Plane)?;
    if digest_hex(&receipt.logical_flat_sha256) != plane.records_sha256 {
        return Err(bad("capsule logical identity"));
    }
    blob.sync_all().map_err(TwoBitGenerationError::Io)?;
    directory.sync_all().map_err(TwoBitGenerationError::Io)?;
    drop(records);
    fs::remove_file(output.join("plane/records.bin")).map_err(TwoBitGenerationError::Io)?;
    File::open(output.join("plane"))
        .and_then(|f| f.sync_all())
        .map_err(TwoBitGenerationError::Io)?;
    Ok(SourceLayout::LeafCapsulesV2 {
        blob_bytes: receipt.charge.physical_bytes as u64,
        blob_sha256: digest_hex(&receipt.physical_blob_sha256),
        directory_bytes: receipt.charge.encoded_directory_bytes,
        directory_sha256: digest_hex(&receipt.directory_sha256),
    })
}

fn pack_framed_source(
    output: &Path,
    plane: &crate::two_bit_source::SourcePlaneReceipt,
    generation: u64,
    cap: usize,
) -> Result<SourceLayout> {
    use crate::{source_frame, two_bit_source::TwoBitPlane};
    let bad = TwoBitGenerationError::Invalid;
    let f = source_frame::frame_bytes(plane.rows, plane.record_bytes)
        .map_err(TwoBitGenerationError::Io)?;
    let directory_bytes =
        source_frame::directory_bytes(plane.rows).map_err(TwoBitGenerationError::Io)?;
    let retained = plane
        .rows
        .div_ceil(32)
        .checked_mul(96)
        .and_then(|n| n.checked_add(plane.dimensions.checked_mul(256)?))
        .and_then(|n| n.checked_add(131072))
        .ok_or(bad("frame pack metadata"))?;
    let charge = f
        .checked_mul(4)
        .and_then(|n| n.checked_add(zstd::zstd_safe::compress_bound(f)))
        .and_then(|n| n.checked_add(source_frame::ENCODER_CONTEXT_BYTES))
        .and_then(|n| n.checked_add(source_frame::DECODER_CONTEXT_BYTES))
        .and_then(|n| n.checked_add(directory_bytes.checked_mul(2)?))
        .and_then(|n| n.checked_add(retained))
        .ok_or(bad("frame pack memory"))?;
    if charge > cap {
        return Err(bad("frame pack memory"));
    }
    let plane_body =
        fs::read(output.join("plane/manifest.json")).map_err(TwoBitGenerationError::Io)?;
    let (_codec, authority) = TwoBitPlane::open_metadata(
        &output.join("plane"),
        &hash(&plane_body),
        &plane.sq8_sha256,
        generation,
        retained,
    )
    .map_err(TwoBitGenerationError::Plane)?;
    let mut input =
        File::open(output.join("plane/records.bin")).map_err(TwoBitGenerationError::Io)?;
    let mut blob = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("plane/frames.bin"))
        .map_err(TwoBitGenerationError::Io)?;
    let mut directory = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("plane/frame_offsets.bin"))
        .map_err(TwoBitGenerationError::Io)?;
    let (mut logical_sha, mut physical_sha, mut directory_sha) =
        (Sha256::new(), Sha256::new(), Sha256::new());
    let mut offset = 0u64;
    directory
        .write_all(&offset.to_le_bytes())
        .map_err(TwoBitGenerationError::Io)?;
    directory_sha.update(offset.to_le_bytes());
    for first in (0..plane.rows).step_by(256) {
        let rows = (plane.rows - first).min(256);
        let mut original = vec![0; rows * plane.record_bytes];
        input
            .read_exact(&mut original)
            .map_err(TwoBitGenerationError::Io)?;
        authority
            .verify_payload(first / 32, (first + rows).div_ceil(32) - 1, &original)
            .map_err(TwoBitGenerationError::Page)?;
        logical_sha.update(&original);
        let frame = source_frame::encode(
            rows,
            plane.record_bytes,
            &original,
            source_frame::Limits {
                max_decoded_bytes: 1 << 20,
            },
        )
        .map_err(TwoBitGenerationError::Io)?;
        blob.write_all(&frame).map_err(TwoBitGenerationError::Io)?;
        physical_sha.update(&frame);
        offset = offset
            .checked_add(frame.len() as u64)
            .ok_or(bad("frame blob size"))?;
        directory
            .write_all(&offset.to_le_bytes())
            .map_err(TwoBitGenerationError::Io)?;
        directory_sha.update(offset.to_le_bytes());
    }
    if input.read(&mut [0]).map_err(TwoBitGenerationError::Io)? != 0
        || format!("{:x}", logical_sha.finalize()) != plane.records_sha256
    {
        return Err(bad("frame logical identity"));
    }
    blob.sync_all().map_err(TwoBitGenerationError::Io)?;
    directory.sync_all().map_err(TwoBitGenerationError::Io)?;
    drop(input);
    fs::remove_file(output.join("plane/records.bin")).map_err(TwoBitGenerationError::Io)?;
    File::open(output.join("plane"))
        .and_then(|f| f.sync_all())
        .map_err(TwoBitGenerationError::Io)?;
    Ok(SourceLayout::FramedRecordsV1 {
        blob_bytes: offset,
        blob_sha256: format!("{:x}", physical_sha.finalize()),
        directory_bytes,
        directory_sha256: format!("{:x}", directory_sha.finalize()),
    })
}

fn copy_authenticated(
    current: &Path,
    output: &Path,
    name: &str,
    bytes: u64,
    digest: &str,
) -> Result<()> {
    let mut input = HashingReader {
        inner: File::open(current.join(name)).map_err(TwoBitGenerationError::Io)?,
        digest: Sha256::new(),
    };
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join(name))
        .map_err(TwoBitGenerationError::Io)?;
    let copied = std::io::copy(
        &mut (&mut input).take(
            bytes
                .checked_add(1)
                .ok_or(TwoBitGenerationError::Invalid("copy length"))?,
        ),
        &mut out,
    )
    .map_err(TwoBitGenerationError::Io)?;
    if copied != bytes || format!("{:x}", input.digest.finalize()) != digest {
        return Err(TwoBitGenerationError::Invalid("repackage copy identity"));
    }
    out.sync_all().map_err(TwoBitGenerationError::Io)
}

/// Repackage a current v10 flat semantic generation without fitting or reordering.
/// Requires a new generation ID; preserves logical records, unit digests, canonical
/// identity and router bytes, and reseals the SQ8 page manifest and root last.
/// Caller pins and all pack/copy metadata are included in the supplied limits.
pub fn repackage_source_layout(
    current: &Path,
    trusted_root_sha256: &str,
    output: &Path,
    generation: u64,
    layout: SourceLayoutMode,
    limits: TwoBitGenerationLimits,
) -> Result<String> {
    use crate::two_bit_source::{SourcePlaneReceipt, read_authenticated};
    let bad = TwoBitGenerationError::Invalid;
    let available = limits
        .max_memory_bytes
        .checked_sub(limits.already_pinned_bytes)
        .and_then(|n| n.checked_sub(REPACKAGE_METADATA_BYTES as u64))
        .and_then(|n| usize::try_from(n).ok())
        .ok_or(bad("layout repackage memory"))?;
    if output.exists() || available < 131072 {
        return Err(bad("layout repackage admission"));
    }
    let read = |name: &str, digest: &str| -> Result<Vec<u8>> {
        let length = fs::metadata(current.join(name))
            .map_err(TwoBitGenerationError::Io)?
            .len();
        if length == 0 || length > 65536 {
            return Err(bad("layout repackage manifest"));
        }
        read_authenticated(&current.join(name), length as usize, digest)
            .map_err(TwoBitGenerationError::Plane)
    };
    let body = read("manifest.json", trusted_root_sha256)?;
    let mut root: Manifest =
        serde_json::from_slice(&body).map_err(|_| bad("layout repackage root"))?;
    if root.schema != SCHEMA
        || root.source_layout.mode() != SourceLayoutMode::FlatRecords
        || root.discovery.mode() != DiscoveryMode::Semantic
        || generation <= root.generation
    {
        return Err(bad(
            "layout repackage requires current flat semantic generation and new ID",
        ));
    }
    TwoBitGeneration::validate_local_publication(
        current,
        trusted_root_sha256,
        TwoBitGenerationLimits {
            already_pinned_bytes: limits
                .already_pinned_bytes
                .checked_add(REPACKAGE_METADATA_BYTES as u64)
                .ok_or(bad("layout repackage memory"))?,
            ..limits
        },
    )?;
    let plane_body = read("plane/manifest.json", &root.plane_manifest_sha256)?;
    let plane: SourcePlaneReceipt =
        serde_json::from_slice(&plane_body).map_err(|_| bad("plane receipt"))?;
    let page_body = read("page_manifest.json", &root.page_manifest_sha256)?;
    let mut page: serde_json::Value =
        serde_json::from_slice(&page_body).map_err(|_| bad("page receipt"))?;
    let Discovery::Semantic {
        root_sha256,
        root_bytes,
        membership_sha256,
        membership_bytes,
        leaves_sha256,
        leaves_bytes,
        centroids_sha256,
        ..
    } = &root.discovery
    else {
        unreachable!()
    };
    fs::create_dir(output).map_err(TwoBitGenerationError::Io)?;
    fs::create_dir(output.join("plane")).map_err(TwoBitGenerationError::Io)?;
    fs::create_dir(output.join("router")).map_err(TwoBitGenerationError::Io)?;
    for (name, bytes, digest) in [
        (
            "plane/manifest.json",
            plane_body.len() as u64,
            root.plane_manifest_sha256.as_str(),
        ),
        (
            "plane/mean.bin",
            (plane.dimensions * 4) as u64,
            plane.mean_sha256.as_str(),
        ),
        (
            "plane/records.bin",
            (plane.rows * plane.record_bytes) as u64,
            plane.records_sha256.as_str(),
        ),
        (
            "plane/page_digests.bin",
            (plane.rows.div_ceil(32) * 32) as u64,
            plane.page_digest_sha256.as_str(),
        ),
        (
            "page_digests.bin",
            (plane.rows.div_ceil(SQ8_PAGE_ROWS) * 32) as u64,
            page["page_digest_sha256"]
                .as_str()
                .ok_or(bad("page digest"))?,
        ),
        ("router/root.bin", *root_bytes as u64, root_sha256.as_str()),
        (
            "router/membership.bin",
            *membership_bytes as u64,
            membership_sha256.as_str(),
        ),
        (
            "router/leaves.bin",
            *leaves_bytes as u64,
            leaves_sha256.as_str(),
        ),
        (
            "canonical_row_digests.bin",
            root.canonical.row_digests_bytes as u64,
            root.canonical.row_digests_sha256.as_str(),
        ),
        (
            "canonical.bin",
            root.canonical.bytes,
            root.canonical.sha256.as_str(),
        ),
        (
            "centroids.bin",
            fs::metadata(current.join("centroids.bin"))
                .map_err(TwoBitGenerationError::Io)?
                .len(),
            centroids_sha256.as_str(),
        ),
    ] {
        copy_authenticated(current, output, name, bytes, digest)?;
    }
    root.generation = generation;
    page["generation"] = generation.into();
    let page_body = serde_json::to_vec(&page).map_err(|_| bad("page encoding"))?;
    write_new(&output.join("page_manifest.json"), &page_body)?;
    root.page_manifest_sha256 = hash(&page_body);
    root.source_layout = match layout {
        SourceLayoutMode::FlatRecords => SourceLayout::FlatRecords,
        SourceLayoutMode::FramedRecordsV1 => {
            pack_framed_source(output, &plane, generation, available)?
        }
        SourceLayoutMode::LeafCapsulesV2 => {
            pack_source_layout(output, &root.discovery, &plane, generation, available)?
        }
    };
    finish_root(
        output,
        &serde_json::to_vec(&root).map_err(|_| bad("root encoding"))?,
    )
}

/// Shared pre-allocation build admission; caller pins/IDs are separate charges.
pub(crate) fn admit_build_payload(
    rows: usize,
    dimensions: usize,
    ordered: bool,
    mode: DiscoveryMode,
    profile: SemanticProfile,
    max_build_payload_bytes: usize,
) -> Result<usize> {
    admit_build_payload_with_partition(
        rows,
        dimensions,
        ordered,
        mode,
        profile,
        max_build_payload_bytes,
        LeafPartition::Radial64V1,
    )
}

fn admit_build_payload_with_partition(
    rows: usize,
    dimensions: usize,
    ordered: bool,
    mode: DiscoveryMode,
    profile: SemanticProfile,
    max_build_payload_bytes: usize,
    partition: LeafPartition,
) -> Result<usize> {
    let bad = TwoBitGenerationError::Invalid;
    if rows == 0
        || dimensions == 0
        || (mode == DiscoveryMode::Semantic && !profile.valid_geometry(rows, dimensions))
    {
        return Err(bad("build inputs"));
    }
    let units = rows.div_ceil(32);
    let pages = rows.div_ceil(SQ8_PAGE_ROWS);
    let page_bytes = dimensions
        .checked_add(12)
        .and_then(|n| n.checked_mul(SQ8_PAGE_ROWS))
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
        .and_then(|n| n.checked_add(327680))
        .and_then(|n| n.checked_add(if ordered { rows.checked_mul(8)? } else { 0 }))
        .ok_or(bad("build memory"))?;
    let semantic_memory = if mode == DiscoveryMode::Semantic {
        let geometry = crate::semantic_unit_router::Geometry {
            rows,
            dimensions,
            units,
            blob_bytes: units
                .checked_mul(dimensions)
                .and_then(|n| n.checked_mul(2))
                .and_then(|n| n.checked_add(32))
                .ok_or(bad("centroid geometry"))?,
        };
        crate::semantic_unit_router::admit_with_partition(
            geometry,
            max_build_payload_bytes.min(profile.allocation_cap()),
            profile,
            partition,
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
    Ok(semantic_memory)
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
            SourceLayoutMode::FlatRecords,
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
            SourceLayoutMode::FlatRecords,
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
        self.build_with_discovery_and_source_layout(
            order,
            mode,
            SourceLayoutMode::FlatRecords,
            output,
            max_build_payload_bytes,
        )
    }

    /// Explicit physical SOURCE layout with the unchanged discovery builder.
    pub fn build_with_discovery_and_source_layout(
        &self,
        order: Option<&[u64]>,
        mode: DiscoveryMode,
        layout: SourceLayoutMode,
        output: &Path,
        max_build_payload_bytes: usize,
    ) -> Result<String> {
        self.build_inner(
            order,
            mode,
            SemanticProfile::Native100k,
            None,
            layout,
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
            SourceLayoutMode::FlatRecords,
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
            SourceLayoutMode::FlatRecords,
            output,
            max_build_payload_bytes,
        )
    }

    /// Explicit layout construction, packing only after the semantic router is fixed.
    /// An imported router must match the supplied profile; no layout-dependent refit.
    #[allow(clippy::too_many_arguments)]
    pub fn build_with_source_layout(
        &self,
        order: Option<&[u64]>,
        profile: SemanticProfile,
        import: Option<&SemanticRouterImport<'_>>,
        layout: SourceLayoutMode,
        output: &Path,
        max_build_payload_bytes: usize,
    ) -> Result<String> {
        if import.is_some_and(|import| import.input.profile != profile) {
            return Err(TwoBitGenerationError::Invalid("source layout profile"));
        }
        self.build_inner(
            order,
            DiscoveryMode::Semantic,
            profile,
            import,
            layout,
            output,
            max_build_payload_bytes,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build_inner(
        &self,
        order: Option<&[u64]>,
        mode: DiscoveryMode,
        profile: SemanticProfile,
        import: Option<&SemanticRouterImport<'_>>,
        layout: SourceLayoutMode,
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
        let max_build_payload_bytes = max_build_payload_bytes
            .checked_sub(
                import
                    .map(SemanticRouterImport::retained_bytes)
                    .transpose()?
                    .unwrap_or(0),
            )
            .ok_or(bad("router import memory"))?;
        let partition = if let Some(import) = import {
            crate::semantic_unit_router::admit_authenticated_root(
                &import.artifacts.manifest,
                &hash(&import.artifacts.manifest),
                &import.input,
                max_build_payload_bytes,
            )
            .map_err(|e| TwoBitGenerationError::Router(e.to_string()))?
            .0
        } else {
            LeafPartition::Radial64V1
        };
        let semantic_memory = admit_build_payload_with_partition(
            rows,
            dimensions,
            order.is_some(),
            mode,
            profile,
            max_build_payload_bytes,
            partition,
        )?;
        let pages = rows.div_ceil(SQ8_PAGE_ROWS);
        let page_bytes = dimensions
            .checked_add(12)
            .and_then(|n| n.checked_mul(SQ8_PAGE_ROWS))
            .ok_or(bad("page geometry"))?;
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
            let bytes = (rows - page * SQ8_PAGE_ROWS).min(SQ8_PAGE_ROWS) * (dimensions + 12);
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
            "generation":self.generation,"rows":rows,"dimensions":dimensions,"page_rows":SQ8_PAGE_ROWS,
            "object_sha256":self.source.sq8_sha256,"page_digest_sha256":hash(&sidecar)}),
        )
        .map_err(|_| bad("page manifest encoding"))?;
        write_new(&output.join("page_manifest.json"), &page_manifest)?;
        let plane_body =
            fs::read(output.join("plane/manifest.json")).map_err(TwoBitGenerationError::Io)?;
        let source_layout = match layout {
            SourceLayoutMode::FlatRecords => SourceLayout::FlatRecords,
            SourceLayoutMode::FramedRecordsV1 => pack_framed_source(
                output,
                &plane,
                self.generation,
                max_build_payload_bytes
                    .checked_sub(REPACKAGE_METADATA_BYTES)
                    .and_then(|n| n.checked_sub(order.map_or(0, |order| order.len() * 8)))
                    .ok_or(bad("frame build memory"))?,
            )?,
            SourceLayoutMode::LeafCapsulesV2 => pack_source_layout(
                output,
                &discovery,
                &plane,
                self.generation,
                max_build_payload_bytes
                    .checked_sub(REPACKAGE_METADATA_BYTES)
                    .and_then(|n| n.checked_sub(order.map_or(0, |order| order.len() * 8)))
                    .ok_or(bad("capsule build memory"))?,
            )?,
        };
        let body=serde_json::to_vec(&serde_json::json!({"schema":SCHEMA,
            "generation":self.generation,"base_epoch":self.base_epoch,"plane_manifest_sha256":hash(&plane_body),
            "page_manifest_sha256":hash(&page_manifest),"discovery":discovery,"source_layout":source_layout,"sq8_object_sha256":self.source.sq8_sha256,
            "sq8_object_key":self.sq8_object_key,"sq8_etag":self.sq8_etag,"low":self.low,"step":self.step,
            "canonical":canonical}))
            .map_err(|_|bad("root encoding"))?;
        finish_root(output, &body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests observe real file I/O and sync failures, not power-loss recovery.
    fn root_seal_fixture(output: &Path, semantic: bool, capsules: bool) -> Vec<u8> {
        fs::create_dir(output.join("plane")).unwrap();
        write_new(&output.join("plane/manifest.json"), b"plane").unwrap();
        let source_layout = if capsules {
            write_new(&output.join("plane/capsules.bin"), b"capsules").unwrap();
            write_new(&output.join("plane/capsule_directory.bin"), b"directory").unwrap();
            serde_json::json!({"layout":"leaf_capsules_v2", "blob_bytes":8,
                "blob_sha256":hash(b"capsules"), "directory_bytes":9,
                "directory_sha256":hash(b"directory")})
        } else {
            write_new(&output.join("plane/records.bin"), b"records").unwrap();
            serde_json::json!({"layout":"flat_records"})
        };
        let digest = hash(b"fixture");
        let discovery = if semantic {
            fs::create_dir(output.join("router")).unwrap();
            for name in ["root.bin", "membership.bin", "leaves.bin"] {
                write_new(&output.join("router").join(name), b"router").unwrap();
            }
            serde_json::json!({"mode":"semantic", "profile":"native100k",
                "root_sha256":hash(b"router"), "root_bytes":6,
                "membership_sha256":hash(b"router"), "membership_bytes":6,
                "leaves_sha256":hash(b"router"), "leaves_bytes":6,
                "input_schema":SCHEMA, "input_root_sha256":digest,
                "centroids_sha256":digest, "source_sha256":digest,
                "source_order_sha256":digest, "mean_sha256":digest,
                "records_sha256":digest, "sq8_sha256":digest})
        } else {
            serde_json::json!({"mode":"graph", "centroids_sha256":digest,
                "graph_sha256":digest, "graph_resident_bytes":1,
                "diverse_graph_sha256":digest, "diverse_graph_resident_bytes":1})
        };
        serde_json::to_vec(&serde_json::json!({"schema":SCHEMA, "generation":1,
            "base_epoch":0, "plane_manifest_sha256":hash(b"plane"),
            "page_manifest_sha256":digest, "discovery":discovery,
            "source_layout":source_layout, "sq8_object_sha256":digest,
            "sq8_object_key":format!("test/objects/{digest}"), "sq8_etag":"etag",
            "canonical":{"rows":1, "dimensions":1, "bytes":12,
                "row_digests_bytes":32, "row_digests_sha256":digest,
                "sha256":digest, "object_key":format!("test/objects/{digest}")},
            "low":[0.0], "step":[1.0]}))
        .unwrap()
    }

    #[test]
    fn finish_root_syncs_children_pending_root_and_parent_in_order() {
        for (semantic, capsules) in [(false, false), (true, false), (true, true)] {
            let temp = tempfile::tempdir().unwrap();
            let output = temp.path().join("generation");
            fs::create_dir(&output).unwrap();
            let body = root_seal_fixture(&output, semantic, capsules);
            let pending = output.join("manifest.pending");
            let root = output.join("manifest.json");
            let mut synced = Vec::new();
            let root_sha = finish_root_with_sync(&output, &body, |path, file| {
                if path == output.join("plane") || path == output.join("router") {
                    assert!(!pending.exists());
                    assert!(!root.exists());
                    let child = if path == output.join("router") {
                        "root.bin"
                    } else if capsules {
                        "capsules.bin"
                    } else {
                        "records.bin"
                    };
                    assert!(!fs::read(path.join(child)).unwrap().is_empty());
                } else if path == pending {
                    assert_eq!(fs::read(path).unwrap(), body);
                    assert!(!root.exists());
                } else {
                    assert!(!pending.exists());
                    assert_eq!(fs::read(&root).unwrap(), body);
                }
                file.sync_all()?;
                synced.push(path.to_path_buf());
                Ok(())
            })
            .unwrap();
            let mut expected = vec![output.join("plane")];
            if semantic {
                expected.push(output.join("router"));
            }
            expected.extend([pending, output.clone(), temp.path().to_path_buf()]);
            assert_eq!(synced, expected);
            assert_eq!(root_sha, hash(&body));
            assert_eq!(fs::read(root).unwrap(), body);
        }
    }

    #[test]
    fn finish_root_propagates_every_sync_failure_without_removing_artifacts() {
        for (semantic, capsules) in [(false, false), (true, false), (true, true)] {
            for failure in 0..if semantic { 5 } else { 4 } {
                let temp = tempfile::tempdir().unwrap();
                let output = temp.path().join("generation");
                fs::create_dir(&output).unwrap();
                let body = root_seal_fixture(&output, semantic, capsules);
                let pending = output.join("manifest.pending");
                let root = output.join("manifest.json");
                let mut expected = vec![output.join("plane")];
                if semantic {
                    expected.push(output.join("router"));
                }
                let children = expected.len();
                expected.extend([pending.clone(), output.clone(), temp.path().to_path_buf()]);
                let mut attempted = Vec::new();
                let result = finish_root_with_sync(&output, &body, |path, file| {
                    attempted.push(path.to_path_buf());
                    if path == expected[failure] {
                        return Err(std::io::Error::other("injected root sync failure"));
                    }
                    file.sync_all()
                });
                let Err(TwoBitGenerationError::Io(error)) = result else {
                    panic!("sync failure must prevent a successful root receipt");
                };
                assert_eq!(error.kind(), std::io::ErrorKind::Other);
                assert_eq!(error.to_string(), "injected root sync failure");
                assert_eq!(attempted, expected[..=failure]);
                assert_eq!(pending.exists(), failure == children);
                assert_eq!(root.exists(), failure > children);
                if root.exists() {
                    assert_eq!(fs::read(&root).unwrap(), body);
                }
                if pending.exists() {
                    assert_eq!(fs::read(&pending).unwrap(), body);
                }
                assert_eq!(
                    fs::read(output.join("plane/manifest.json")).unwrap(),
                    b"plane"
                );
                if semantic {
                    assert_eq!(fs::read(output.join("router/root.bin")).unwrap(), b"router");
                }
            }
        }
    }

    #[test]
    fn finish_root_syncs_current_directory_for_single_component_output() {
        let temp = tempfile::Builder::new()
            .prefix("borsuk-root-seal-")
            .tempdir_in(".")
            .unwrap();
        let output = Path::new(temp.path().file_name().unwrap());
        assert!(output.parent().unwrap().as_os_str().is_empty());
        let body = root_seal_fixture(output, false, false);
        let mut synced = Vec::new();
        finish_root_with_sync(output, &body, |path, file| {
            file.sync_all()?;
            synced.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(synced.last().unwrap(), Path::new("."));
        assert_eq!(fs::read(output.join("manifest.json")).unwrap(), body);
    }

    #[test]
    fn finish_root_preserves_pending_ownership_and_rename_errors() {
        for block_rename in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let output = temp.path().join("generation");
            fs::create_dir(&output).unwrap();
            let body = root_seal_fixture(&output, false, false);
            let pending = output.join("manifest.pending");
            let root = output.join("manifest.json");
            if block_rename {
                fs::create_dir(&root).unwrap();
            } else {
                fs::write(&pending, b"unowned pending").unwrap();
            }
            let mut synced = Vec::new();
            let result = finish_root_with_sync(&output, &body, |path, file| {
                file.sync_all()?;
                synced.push(path.to_path_buf());
                Ok(())
            });
            assert!(matches!(result, Err(TwoBitGenerationError::Io(_))));
            if block_rename {
                assert!(root.is_dir());
                assert_eq!(fs::read(pending).unwrap(), body);
                assert_eq!(
                    synced,
                    [output.join("plane"), output.join("manifest.pending")]
                );
            } else {
                assert!(!root.exists());
                assert_eq!(fs::read(pending).unwrap(), b"unowned pending");
                assert_eq!(synced, [output.join("plane")]);
            }
        }
    }

    #[test]
    fn finish_root_rejects_invalid_schema_and_size_before_io() {
        let temp = tempfile::tempdir().unwrap();
        for body in [b"{}".to_vec(), vec![b' '; 65537]] {
            let result = finish_root_with_sync(temp.path(), &body, |_, _| {
                panic!("invalid roots must be rejected before sync");
            });
            assert!(matches!(result, Err(TwoBitGenerationError::Invalid(_))));
            assert!(!temp.path().join("manifest.pending").exists());
            assert!(!temp.path().join("manifest.json").exists());
        }
    }
    #[tokio::test]
    async fn source_shortlist_v9_authenticates_32_row_sq8_blocks() {
        use object_store::{ObjectStoreExt, path::Path as ObjectPath};
        let temp = tempfile::tempdir().unwrap();
        let rows = 4097_usize;
        let dimensions = 5;
        let raw = (0..rows)
            .flat_map(|_| {
                [1_f32, 0., 0., 0., 0.]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
            })
            .collect::<Vec<_>>();
        let sq8 = (0..rows)
            .flat_map(|row| {
                let mut bytes = (50_000_i64 - row as i64).to_le_bytes().to_vec();
                bytes.extend(1_f32.to_le_bytes());
                bytes.extend([255, 0, 0, 0, 0]);
                bytes
            })
            .collect::<Vec<_>>();
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let raw_sha = hash(&raw);
        let sq8_sha = hash(&sq8);
        let key = format!("test/objects/{sq8_sha}");
        let store = object_store::memory::InMemory::new();
        let location = ObjectPath::from(key.clone());
        store.put(&location, sq8.clone().into()).await.unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let order = (0..rows as u64).collect::<Vec<_>>();
        let output = temp.path().join("generation");
        let root_sha = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &raw_sha,
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 0,
            generation: 1,
            low: &[0.; 5],
            step: &[1. / 255.; 5],
            sq8_object_key: &key,
            sq8_etag: &etag,
        }
        .build_with_order(&order, &output, 64_000_000)
        .unwrap();
        let root = fs::read(output.join("manifest.json")).unwrap();
        assert_eq!(hash(&root), root_sha);
        let root: serde_json::Value = serde_json::from_slice(&root).unwrap();
        let manifest = fs::read(output.join("page_manifest.json")).unwrap();
        assert_eq!(
            hash(&manifest),
            root["page_manifest_sha256"].as_str().unwrap()
        );
        let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(
            manifest["page_rows"], 32,
            "new SQ8 proofs must authenticate 32-row blocks"
        );
        assert_eq!(root["schema"], "borsuk-two-bit-generation-v13");
        assert_eq!(manifest["object_sha256"], sq8_sha);
        let digests = fs::read(output.join("page_digests.bin")).unwrap();
        assert_eq!(digests.len(), rows.div_ceil(32) * 32);
        assert_eq!(
            hash(&digests),
            manifest["page_digest_sha256"].as_str().unwrap()
        );
        for (block, digest) in sq8
            .chunks(32 * (dimensions + 12))
            .zip(digests.chunks_exact(32))
        {
            assert_eq!(Sha256::digest(block).as_slice(), digest);
        }
        assert_eq!(
            sq8.chunks(32 * (dimensions + 12)).last().unwrap().len(),
            dimensions + 12
        );
        let manifest_bytes = fs::read(output.join("page_manifest.json")).unwrap();
        let authority = crate::sq8_page_authority::PageAuthority::load(
            &manifest_bytes,
            &hash(&manifest_bytes),
            &digests,
        )
        .unwrap();
        for page in 0..rows.div_ceil(32) {
            let range = authority.byte_range(page, page).unwrap();
            authority
                .verify_payload(page, page, &sq8[range.clone()])
                .unwrap();
            let mut corrupt = sq8[range].to_vec();
            let last = corrupt.len() - 1;
            corrupt[last] ^= 1;
            assert!(authority.verify_payload(page, page, &corrupt).is_err());
        }
        // The final one-row proof cannot authenticate a full-width response or
        // an adjacent block, even when those bytes belong to the same object.
        assert!(authority.verify_payload(128, 128, &sq8[..32 * 17]).is_err());
        assert!(
            authority
                .verify_payload(0, 0, &sq8[32 * 17..64 * 17])
                .is_err()
        );
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 128 * 1024 * 1024,
            max_active_queries: 1,
            max_query_bytes: sq8.len(),
            max_query_gets: 30,
            max_parallel_gets: 4,
            max_source_bytes: 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 4,
            max_query_scratch_bytes: 400_000,
            already_pinned_bytes: 4096,
        };
        let generation = TwoBitGeneration::open(&output, &root_sha, limits).unwrap();
        let query = [1., 0., 0., 0., 0.];
        let (result, trace) = generation
            .diagnostic_search_with_store(&store, &query, 10)
            .await
            .unwrap();
        // Independent full-sort oracle over the actual SOURCE visit roster.
        // SOURCE ties select physical ordinals; SQ8 ties rank application IDs.
        let mut visited = trace
            .nomination_evaluated_units
            .iter()
            .flat_map(|&unit| unit * 32..((unit + 1) * 32).min(rows))
            .collect::<Vec<_>>();
        let visit_count = visited.len();
        visited.sort_unstable();
        visited.dedup();
        assert_eq!(visited.len(), visit_count, "SOURCE rows scored once");
        assert!(visited.len() >= 256);
        visited.truncate(256);
        let mut blocks = visited.iter().map(|row| row / 32).collect::<Vec<_>>();
        blocks.dedup();
        let mut ranges = Vec::<std::ops::Range<usize>>::new();
        for &block in &blocks {
            let start = block * 32 * 17;
            let end = ((block + 1) * 32).min(rows) * 17;
            if let Some(last) = ranges.last_mut().filter(|last| last.end == start) {
                last.end = end;
            } else {
                ranges.push(start..end);
            }
        }
        assert_eq!(result.plan.page_rows, 32);
        assert_eq!(result.plan.selected_pages, blocks);
        assert_eq!(result.plan.ranges, ranges);
        assert_eq!(
            result.ranked.stats.verified_bytes,
            ranges.iter().map(|r| r.len()).sum::<usize>()
        );
        // Sequential f32 formula, independent of the production scoring helper.
        let expected_score = 1_f32 - (255_f32 * (1_f32 / 255.)).clamp(-1., 1.);
        let mut expected = ranges
            .iter()
            .flat_map(|r| r.start / 17..r.end / 17)
            .map(|row| (row, 50_000_i64 - row as i64, expected_score.to_bits()))
            .collect::<Vec<_>>();
        expected.sort_unstable_by_key(|entry| entry.1);
        let actual = result
            .ranked
            .candidates
            .iter()
            .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected[..10]);
        let mut excluded = expected[..3]
            .iter()
            .map(|entry| entry.1)
            .collect::<Vec<_>>();
        excluded.sort_unstable();
        let filtered = generation
            .search_with_store(&store, &query, 10, Some(&excluded))
            .await
            .unwrap();
        assert_eq!(
            filtered.plan, result.plan,
            "exclusions do not alter SOURCE selection"
        );
        let actual = filtered
            .ranked
            .candidates
            .iter()
            .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected[3..13]);
        fn budget_refusal(error: &TwoBitGenerationError) -> bool {
            match error {
                TwoBitGenerationError::Query { error, .. }
                | TwoBitGenerationError::SourcePlanning { error, .. }
                | TwoBitGenerationError::RouterCharged { error, .. } => budget_refusal(error),
                TwoBitGenerationError::Budget(
                    crate::budgeted_page_rank::BudgetedPageError::InsufficientBudget,
                ) => true,
                _ => false,
            }
        }
        // A clustered256 shortlist cannot satisfy k257. This is a physical
        // budget refusal before SQ8 I/O, not a scoring/corruption failure.
        let error = generation
            .search_with_store(&store, &query, 257, None)
            .await
            .err()
            .unwrap();
        assert!(budget_refusal(&error));
        assert_eq!(error.read_stats(), Default::default());
        let tiny = TwoBitGeneration::open(
            &output,
            &root_sha,
            TwoBitGenerationLimits {
                max_query_bytes: 17,
                ..limits
            },
        )
        .unwrap();
        let error = tiny
            .search_with_store(&store, &query, 10, None)
            .await
            .err()
            .unwrap();
        assert!(budget_refusal(&error));
        assert_eq!(error.read_stats(), Default::default());
    }
    #[test]
    fn spatial_import_and_repackage_charge_authenticated_mode_and_retained_buffers() {
        let temp = tempfile::tempdir().unwrap();
        let rows = 129_usize * 32;
        let raw = (0..rows)
            .flat_map(|_| [0.5_f32, 0.25].into_iter().flat_map(f32::to_le_bytes))
            .collect::<Vec<_>>();
        let sq8 = (0..rows as i64)
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
        let raw_sha = hash(&raw);
        let sq8_sha = hash(&sq8);
        let key = format!("test/objects/{sq8_sha}");
        let low = [0.; 2];
        let step = [1. / 255.; 2];
        let builder = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &raw_sha,
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions: 2,
            },
            base_epoch: 0,
            generation: 1,
            low: &low,
            step: &step,
            sq8_object_key: &key,
            sq8_etag: "etag",
        };
        let current = temp.path().join("graph");
        let current_sha = builder.build(&current, 64_000_000).unwrap();
        let original_plane = fs::read(current.join("plane/manifest.json")).unwrap();
        let centroid = fs::read(current.join("centroids.bin")).unwrap();
        let centroid_sha = hash(&centroid);
        let original_root = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-two-bit-generation-v4", "centroids_sha256":centroid_sha,
            "plane_manifest_sha256":hash(&original_plane), "sq8_object_sha256":sq8_sha,
            "low":low, "step":step,
        }))
        .unwrap();
        let original_sha = hash(&original_root);
        let input = crate::semantic_unit_router::SourceIdentity {
            profile: SemanticProfile::Native100k,
            schema: "borsuk-two-bit-generation-v4",
            root_sha256: &original_sha,
            centroids_sha256: &centroid_sha,
            rows,
            dimensions: 2,
        };
        let artifacts = crate::semantic_unit_router::build_with_partition(
            &centroid,
            &input,
            64_000_000,
            LeafPartition::WidestAxisQuotaV1,
        )
        .unwrap();
        let import = SemanticRouterImport {
            artifacts: &artifacts,
            input,
            original_root: &original_root,
            original_plane: &original_plane,
        };
        let geometry =
            crate::semantic_unit_router::preflight(&centroid, rows, 2, import.input.profile)
                .unwrap();
        let radial =
            crate::semantic_unit_router::admit(geometry, 64_000_000, import.input.profile).unwrap();
        let spatial = crate::semantic_unit_router::admit_with_partition(
            geometry,
            64_000_000,
            import.input.profile,
            LeafPartition::WidestAxisQuotaV1,
        )
        .unwrap();
        assert!(spatial > radial);
        let gap = (spatial - radial) / 2;
        let pinned = 262144;
        let charge =
            pinned + import.retained_bytes().unwrap() as u64 + REPACKAGE_METADATA_BYTES as u64;
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: charge + spatial as u64,
            already_pinned_bytes: pinned,
            max_active_queries: 1,
            max_query_bytes: 16384,
            max_query_gets: 2,
            max_parallel_gets: 2,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 8192,
        };
        let rejected = temp.path().join("repackage-between");
        let error = repackage_semantic_router(
            &current,
            &current_sha,
            &sq8_path,
            &import,
            &rejected,
            TwoBitGenerationLimits {
                max_memory_bytes: charge + (radial + gap) as u64,
                ..limits
            },
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("modeled allocation admission"));
        assert!(!rejected.exists());
        let exact = temp.path().join("repackage-exact");
        repackage_semantic_router(&current, &current_sha, &sq8_path, &import, &exact, limits)
            .unwrap();
        assert_eq!(
            fs::read(exact.join("router/root.bin")).unwrap(),
            artifacts.manifest
        );
        let modeled =
            2 * 128 + rows.div_ceil(SQ8_PAGE_ROWS) * 32 + SQ8_PAGE_ROWS * (2 + 12) + 327680;
        let retained = import.retained_bytes().unwrap();
        let rejected = temp.path().join("import-between");
        let error = builder
            .build_with_semantic_router(None, &import, &rejected, retained + modeled + radial + gap)
            .err()
            .unwrap();
        assert!(error.to_string().contains("build memory budget"));
        assert!(!rejected.exists());
        let exact = temp.path().join("import-exact");
        builder
            .build_with_semantic_router(None, &import, &exact, retained + modeled + spatial)
            .unwrap();
        assert_eq!(
            fs::read(exact.join("router/root.bin")).unwrap(),
            artifacts.manifest
        );
    }

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
            manifest["schema"], "borsuk-two-bit-generation-v13",
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

/// Offline construction from one explicitly pinned v12 input. This is not a
/// serving compatibility reader. Input contains canonical.bin plus
/// its complete manifest-selected artifacts; output is create-only.
pub fn authenticate_canonical_generation(
    current: &Path,
    trusted_root: &str,
    sq8_path: &Path,
    output: &Path,
    max_memory_bytes: usize,
) -> Result<String> {
    use crate::two_bit_source::SourcePlaneReceipt;
    let bad = TwoBitGenerationError::Invalid;
    let read_small = |name: &str, digest: &str| -> Result<Vec<u8>> {
        let length = fs::metadata(current.join(name))
            .map_err(TwoBitGenerationError::Io)?
            .len();
        if length == 0 || length > 65536 {
            return Err(bad("FP32 prep metadata length"));
        }
        crate::two_bit_source::read_authenticated(&current.join(name), length as usize, digest)
            .map_err(TwoBitGenerationError::Plane)
    };
    if output.exists() {
        return Err(bad("FP32 prep output exists"));
    }
    let body = read_small("manifest.json", trusted_root)?;
    let mut value: serde_json::Value =
        serde_json::from_slice(&body).map_err(|_| bad("FP32 prep root"))?;
    if value["schema"] != "borsuk-two-bit-generation-v12"
        || value["canonical"].get("row_digests_bytes").is_some()
        || value["canonical"].get("row_digests_sha256").is_some()
    {
        return Err(bad("FP32 prep requires pinned v12 input"));
    }
    let rows = value["canonical"]["rows"]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or(bad("FP32 prep rows"))?;
    let digests_bytes = rows
        .checked_mul(32)
        .ok_or(bad("FP32 prep digest geometry"))?;
    value["schema"] = SCHEMA.into();
    value["canonical"]["row_digests_bytes"] = digests_bytes.into();
    value["canonical"]["row_digests_sha256"] = hash(&[]).into();
    let mut root: Manifest =
        serde_json::from_value(value).map_err(|_| bad("FP32 prep strict root"))?;
    if !root.valid_descriptor()
        || root.low.len() != root.canonical.dimensions
        || !root.discovery.valid(rows, root.canonical.dimensions)
        || root.generation == 0
        || !valid_object_key(&root.sq8_object_key, &root.sq8_object_sha256)
    {
        return Err(bad("FP32 prep descriptor"));
    }
    root.generation = root
        .generation
        .checked_add(1)
        .ok_or(bad("FP32 prep generation overflow"))?;
    let dimensions = root.canonical.dimensions;
    let modeled = digests_bytes
        .checked_mul(3)
        .and_then(|n| n.checked_add(rows.checked_mul(8)?))
        .and_then(|n| n.checked_add(dimensions.checked_mul(32)?))
        .and_then(|n| n.checked_add(4 * 1024 * 1024))
        .ok_or(bad("FP32 prep memory"))?;
    if modeled > max_memory_bytes {
        return Err(bad("FP32 prep memory cap"));
    }
    // Admit the sidecar, copies, IDs and streaming buffers before opening either payload.
    let plane_body = read_small("plane/manifest.json", &root.plane_manifest_sha256)?;
    let plane: SourcePlaneReceipt =
        serde_json::from_slice(&plane_body).map_err(|_| bad("FP32 prep plane"))?;
    let page_body = read_small("page_manifest.json", &root.page_manifest_sha256)?;
    let mut page: serde_json::Value =
        serde_json::from_slice(&page_body).map_err(|_| bad("FP32 prep pages"))?;
    if plane.rows != rows
        || plane.dimensions != dimensions
        || plane.sq8_sha256 != root.sq8_object_sha256
        || page["generation"].as_u64() != Some(root.generation - 1)
        || page["rows"].as_u64() != Some(rows as u64)
        || page["dimensions"].as_u64() != Some(dimensions as u64)
        || page["object_sha256"].as_str() != Some(root.sq8_object_sha256.as_str())
        || page["page_rows"].as_u64() != Some(SQ8_PAGE_ROWS as u64)
    {
        return Err(bad("FP32 prep cross-artifact binding"));
    }
    plane.validate_identity(&root.sq8_object_sha256).map_err(TwoBitGenerationError::Plane)?;
    root.capsule_metadata_bytes()?;
    let page_digest_len = rows.div_ceil(SQ8_PAGE_ROWS).checked_mul(32)
        .ok_or(bad("FP32 prep page digest geometry"))?;
    let page_digests = crate::two_bit_source::read_authenticated(
        &current.join("page_digests.bin"), page_digest_len,
        page["page_digest_sha256"].as_str().ok_or(bad("FP32 prep page digest"))?)
        .map_err(TwoBitGenerationError::Plane)?;
    crate::sq8_page_authority::PageAuthority::load(&page_body, &root.page_manifest_sha256, &page_digests)
        .map_err(|_| bad("FP32 prep page authority"))?;
    fs::create_dir(output).map_err(TwoBitGenerationError::Io)?;
    fs::create_dir(output.join("plane")).map_err(TwoBitGenerationError::Io)?;
    if root.discovery.mode() == DiscoveryMode::Semantic {
        fs::create_dir(output.join("router")).map_err(TwoBitGenerationError::Io)?;
    }
    let mut copies = vec![
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
            "plane/page_digests.bin",
            (rows.div_ceil(32) * 32) as u64,
            plane.page_digest_sha256.as_str(),
        ),
        (
            "page_digests.bin",
            (rows.div_ceil(SQ8_PAGE_ROWS) * 32) as u64,
            page["page_digest_sha256"]
                .as_str()
                .ok_or(bad("FP32 prep page digest"))?,
        ),
    ];
    copies.extend(
        crate::two_bit_store::source_artifacts(&root, &plane)
            .map_err(|_| bad("FP32 prep source geometry"))?
            .into_iter()
            .map(|(n, b, h)| (n, b as u64, h)),
    );
    let centroids_sha = match &root.discovery {
        Discovery::Graph {
            centroids_sha256,
            graph_sha256,
            diverse_graph_sha256,
            ..
        } => {
            for (name, sha) in [
                ("graph.bin", graph_sha256),
                ("diverse_graph.bin", diverse_graph_sha256),
            ] {
                copies.push((
                    name,
                    fs::metadata(current.join(name))
                        .map_err(TwoBitGenerationError::Io)?
                        .len(),
                    sha.as_str(),
                ));
            }
            centroids_sha256
        }
        Discovery::Semantic {
            centroids_sha256,
            root_sha256,
            root_bytes,
            membership_sha256,
            membership_bytes,
            leaves_sha256,
            leaves_bytes,
            ..
        } => {
            copies.extend([
                ("router/root.bin", *root_bytes as u64, root_sha256.as_str()),
                (
                    "router/membership.bin",
                    *membership_bytes as u64,
                    membership_sha256.as_str(),
                ),
                (
                    "router/leaves.bin",
                    *leaves_bytes as u64,
                    leaves_sha256.as_str(),
                ),
            ]);
            centroids_sha256
        }
    };
    copies.push((
        "centroids.bin",
        fs::metadata(current.join("centroids.bin"))
            .map_err(TwoBitGenerationError::Io)?
            .len(),
        centroids_sha.as_str(),
    ));
    for (name, bytes, digest) in copies {
        copy_authenticated(current, output, name, bytes, digest)?;
    }
    let width = dimensions
        .checked_mul(4)
        .and_then(|n| n.checked_add(8))
        .ok_or(bad("FP32 prep width"))?;
    let sq8_width = dimensions
        .checked_add(12)
        .ok_or(bad("FP32 prep SQ8 width"))?;
    let canonical = File::open(current.join("canonical.bin")).map_err(TwoBitGenerationError::Io)?;
    let sq8 = File::open(sq8_path).map_err(TwoBitGenerationError::Io)?;
    if canonical
        .metadata()
        .map_err(TwoBitGenerationError::Io)?
        .len()
        != root.canonical.bytes
        || sq8.metadata().map_err(TwoBitGenerationError::Io)?.len()
            != rows
                .checked_mul(sq8_width)
                .ok_or(bad("FP32 prep SQ8 length"))? as u64
    {
        return Err(bad("FP32 prep payload length"));
    }
    let mut canonical = BufReader::with_capacity(65536, canonical);
    let mut sq8 = BufReader::with_capacity(65536, sq8);
    let mut records = std::io::BufWriter::with_capacity(
        65536,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output.join("canonical.bin"))
            .map_err(TwoBitGenerationError::Io)?,
    );
    let mut digests = std::io::BufWriter::with_capacity(
        65536,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output.join("canonical_row_digests.bin"))
            .map_err(TwoBitGenerationError::Io)?,
    );
    let (mut canonical_hash, mut sq8_hash, mut digest_hash) =
        (Sha256::new(), Sha256::new(), Sha256::new());
    let mut row = vec![0; width];
    let mut sq8_row = vec![0; sq8_width];
    let mut ids = Vec::with_capacity(rows);
    for _ in 0..rows {
        canonical
            .read_exact(&mut row)
            .map_err(TwoBitGenerationError::Io)?;
        sq8.read_exact(&mut sq8_row)
            .map_err(TwoBitGenerationError::Io)?;
        if row[..8] != sq8_row[..8] {
            return Err(bad("FP32 prep canonical/SQ8 ID order"));
        }
        let mut norm = 0_f64;
        for value in row[8..].chunks_exact(4) {
            let value = f32::from_le_bytes(value.try_into().unwrap());
            if !value.is_finite() {
                return Err(bad("FP32 prep canonical coordinate"));
            }
            norm += f64::from(value) * f64::from(value);
        }
        if !norm.is_finite() || (norm - 1.).abs() > 1e-6 {
            return Err(bad("FP32 prep canonical norm"));
        }
        ids.push(i64::from_le_bytes(row[..8].try_into().unwrap()));
        canonical_hash.update(&row);
        sq8_hash.update(&sq8_row);
        let row_hash = Sha256::digest(&row);
        digest_hash.update(row_hash);
        records.write_all(&row).map_err(TwoBitGenerationError::Io)?;
        digests
            .write_all(&row_hash)
            .map_err(TwoBitGenerationError::Io)?;
    }
    ids.sort_unstable();
    if ids.windows(2).any(|v| v[0] == v[1])
        || canonical
            .read(&mut [0])
            .map_err(TwoBitGenerationError::Io)?
            != 0
        || sq8.read(&mut [0]).map_err(TwoBitGenerationError::Io)? != 0
        || format!("{:x}", canonical_hash.finalize()) != root.canonical.sha256
        || format!("{:x}", sq8_hash.finalize()) != root.sq8_object_sha256
    {
        return Err(bad("FP32 prep payload identity"));
    }
    for file in [&mut records, &mut digests] {
        file.flush().map_err(TwoBitGenerationError::Io)?;
        file.get_ref()
            .sync_all()
            .map_err(TwoBitGenerationError::Io)?;
    }
    root.canonical.row_digests_sha256 = format!("{:x}", digest_hash.finalize());
    page["generation"] = root.generation.into();
    let page_body = serde_json::to_vec(&page).map_err(|_| bad("FP32 prep pages encoding"))?;
    write_new(&output.join("page_manifest.json"), &page_body)?;
    root.page_manifest_sha256 = hash(&page_body);
    let body = serde_json::to_vec(&root).map_err(|_| bad("FP32 prep root encoding"))?;
    finish_root(output, &body)
}
