//! Bounded offline research construction; no query-quality or latency claim.
//! Accepts only an externally authenticated historical v4 root and its centroids.
//! Membership is one LE u32 leaf ID per original unit. Each leaf record is a LE
//! u32 original unit ID followed by its unchanged D LE FP16 coefficients.
//! Source-row counts are derived from the original rows and 32-row unit geometry.
use borsuk::{VectorMetric, train_logical_cell_centroids, unit_centroid_pages::UnitCentroidPages};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;

const ROOT_CAP: usize = 64 * 1024;
const BLOB_CAP: usize = 8 * 1024 * 1024;
const MANIFEST_CAP: usize = 4 * 1024 * 1024;
const ALLOCATION_CAP: usize = 128 * 1024 * 1024;
const HEADER_BYTES: usize = 32;
const LEAF_UNITS: usize = 64;
const ITERATIONS: usize = 12;
const SCHEMA: &str = "borsuk-semantic-unit-router-research-v1";
// This research adapter accepts the parent's pinned historical input only.
const INPUT_SCHEMA: &str = "borsuk-two-bit-generation-v4";

#[derive(Deserialize)]
struct InputRoot {
    schema: String,
    canonical: InputGeometry,
    centroids_sha256: String,
}

#[derive(Deserialize)]
struct InputGeometry {
    rows: usize,
    dimensions: usize,
}

#[derive(Clone, Copy)]
struct Geometry {
    rows: usize,
    dimensions: usize,
    units: usize,
    blob_bytes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    bytes: usize,
    sha256: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Algorithm {
    trainer: String,
    metric: String,
    iterations: usize,
    requested_centers: usize,
    training_centers: usize,
    max_leaf_units: usize,
    nearest_ties: String,
    group_sort: String,
    root_prototype: String,
    normalization: String,
    payload: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Leaf {
    leaf_id: usize,
    group_ordinal: usize,
    chunk_ordinal: usize,
    offset: usize,
    bytes: usize,
    unit_count: usize,
    source_rows: usize,
    sha256: String,
    prototype: Vec<f32>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: String,
    input_schema: String,
    input_root_sha256: String,
    input_centroids_sha256: String,
    input_centroids_bytes: usize,
    rows: usize,
    dimensions: usize,
    unit_rows: usize,
    page_rows: usize,
    unit_count: usize,
    final_unit_rows: usize,
    algorithm: Algorithm,
    modeled_peak_allocation_bytes: usize,
    modeled_allocation_limit_bytes: usize,
    allocation_model: String,
    membership: Artifact,
    leaf_payload: Artifact,
    leaves: Vec<Leaf>,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn bounded(path: &Path, cap: usize) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    let len = usize::try_from(metadata.len())?;
    if !metadata.is_file() || len > cap {
        return Err("offline artifact byte cap or file type".into());
    }
    // Exact capacity, including when the file changes while being read.
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)?;
    if file.read(&mut [0])? != 0 {
        return Err("artifact length changed while reading".into());
    }
    Ok(bytes)
}

fn product(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(1_usize, |total, value| {
        total
            .checked_mul(*value)
            .ok_or_else(|| "allocation arithmetic overflow".into())
    })
}

fn sum(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| "allocation arithmetic overflow".into())
    })
}

fn preflight(blob: &[u8], rows: usize, dimensions: usize) -> Result<Geometry> {
    if blob.len() < HEADER_BYTES || &blob[..8] != b"BORSUCP1" || blob[28..32] != [0; 4] {
        return Err("centroid header".into());
    }
    let header_rows = usize::try_from(u64::from_le_bytes(blob[8..16].try_into()?))?;
    let header_dimensions = u32::from_le_bytes(blob[16..20].try_into()?) as usize;
    let unit_rows = u32::from_le_bytes(blob[20..24].try_into()?);
    let page_rows = u32::from_le_bytes(blob[24..28].try_into()?);
    if !(1..=100_000).contains(&header_rows)
        || !(1..=768).contains(&header_dimensions)
        || unit_rows != 32
        || page_rows != 256
        || header_rows != rows
        || header_dimensions != dimensions
    {
        return Err("centroid geometry or manifest agreement".into());
    }
    let units = sum(&[rows, 31])? / 32;
    let blob_bytes = sum(&[HEADER_BYTES, product(&[units, dimensions, 2])?])?;
    if units > 3125 || blob_bytes != blob.len() || blob_bytes > BLOB_CAP {
        return Err("centroid exact payload length or unit cap".into());
    }
    Ok(Geometry {
        rows,
        dimensions,
        units,
        blob_bytes,
    })
}

fn admit(geometry: Geometry, cap: usize) -> Result<usize> {
    let Geometry {
        dimensions: d,
        units: u,
        ..
    } = geometry;
    let centers = sum(&[u, LEAF_UNITS - 1])? / LEAF_UNITS;
    let leaves = sum(&[centers, centers])?
        .checked_sub(1)
        .ok_or("allocation geometry")?;
    let decoded = sum(&[product(&[u, d, 4])?, product(&[u, 4])?])?;
    let vectors = sum(&[
        product(&[u, d, 4])?,
        product(&[u, size_of::<Vec<f32>>() + 64])?,
    ])?;
    // The trainer's fanout is <=32. Depth <= requested centers is a deliberately
    // loose bound: charge every level a full sample's indices/groups/assignment
    // scratch plus four center/sum/temporary planes and vector overhead.
    let recursive = product(&[
        centers,
        sum(&[product(&[32, d, 16])?, product(&[u, 64])?, 32 * 128])?,
    ])?;
    let identities = product(&[centers, sum(&[product(&[d, 4])?, 128])?])?;
    let outputs = product(&[3, u, sum(&[product(&[d, 2])?, 8])?])?;
    let prototypes = product(&[3, leaves, sum(&[product(&[d, 4])?, 256])?])?;
    let estimate = sum(&[
        2 * ROOT_CAP,
        BLOB_CAP,
        decoded,
        product(&[2, vectors])?,
        recursive,
        identities,
        outputs,
        prototypes,
        4 * MANIFEST_CAP,
        8 * 1024 * 1024,
    ])?;
    if estimate > cap {
        return Err(format!("modeled allocation admission: {estimate} bytes exceeds {cap}").into());
    }
    Ok(estimate)
}

fn squared_distance(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
        .sum()
}

fn assign_groups(sample: &[Vec<f32>], centers: &[Vec<f32>]) -> Result<Vec<Vec<(f64, usize)>>> {
    if centers.is_empty()
        || centers
            .iter()
            .any(|center| center.len() != sample[0].len() || center.iter().any(|v| !v.is_finite()))
    {
        return Err("trainer returned invalid centers".into());
    }
    let mut groups = vec![Vec::new(); centers.len()];
    for (unit, vector) in sample.iter().enumerate() {
        let mut best = 0;
        let mut distance = squared_distance(vector, &centers[0]);
        for (ordinal, center) in centers.iter().enumerate().skip(1) {
            let candidate = squared_distance(vector, center);
            if candidate < distance {
                best = ordinal;
                distance = candidate;
            }
        }
        if !distance.is_finite() {
            return Err("nonfinite assignment distance".into());
        }
        groups[best].push((distance, unit));
    }
    for group in &mut groups {
        group.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    }
    Ok(groups)
}

fn prototype(scorer: &UnitCentroidPages, units: &[usize]) -> Result<Vec<f32>> {
    let mut sums = vec![0.0_f64; scorer.dimensions()];
    for &unit in units {
        let vector = scorer.unit_centroid(unit).ok_or("prototype unit ID")?;
        for (sum, value) in sums.iter_mut().zip(vector) {
            *sum += f64::from(*value);
        }
    }
    let mean = sums
        .into_iter()
        .map(|sum| (sum / units.len() as f64) as f32)
        .collect::<Vec<_>>();
    if mean.iter().any(|v| !v.is_finite()) {
        return Err("nonfinite root prototype".into());
    }
    Ok(mean)
}

fn unit_row_count(geometry: Geometry, unit: usize) -> usize {
    (geometry.rows - 32 * unit).min(32)
}

fn verify(
    body: &[u8],
    membership: &[u8],
    payload: &[u8],
    root_sha: &str,
    blob: &[u8],
) -> Result<()> {
    if body.len() > MANIFEST_CAP {
        return Err("research manifest cap".into());
    }
    let manifest: Manifest = serde_json::from_slice(body)?;
    let geometry = preflight(blob, manifest.rows, manifest.dimensions)?;
    let estimate = admit(geometry, ALLOCATION_CAP)?;
    let algorithm = &manifest.algorithm;
    let scorer = UnitCentroidPages::decode(blob)?;
    let identical =
        (1..geometry.units).all(|unit| scorer.unit_centroid(unit) == scorer.unit_centroid(0));
    let requested = geometry.units.div_ceil(LEAF_UNITS);
    if manifest.schema != SCHEMA
        || manifest.input_schema != INPUT_SCHEMA
        || manifest.input_root_sha256 != root_sha
        || manifest.input_centroids_sha256 != hash(blob)
        || manifest.input_centroids_bytes != geometry.blob_bytes
        || manifest.unit_rows != 32
        || manifest.page_rows != 256
        || manifest.unit_count != geometry.units
        || manifest.final_unit_rows != unit_row_count(geometry, geometry.units - 1)
        || manifest.modeled_peak_allocation_bytes != estimate
        || manifest.modeled_allocation_limit_bytes != ALLOCATION_CAP
        || manifest.allocation_model
            != "conservative allocation capacities; not RSS or an enforced process limit"
        || algorithm.trainer != "train_logical_cell_centroids"
        || algorithm.metric != "SquaredEuclidean"
        || algorithm.iterations != ITERATIONS
        || algorithm.requested_centers != requested
        || algorithm.training_centers != if identical { 1 } else { requested }
        || algorithm.max_leaf_units != LEAF_UNITS
        || algorithm.nearest_ties != "center ordinal"
        || algorithm.group_sort != "squared distance, original unit ID"
        || algorithm.root_prototype != "unweighted unit mean; f64 accumulation to finite f32"
        || algorithm.normalization != "none"
        || algorithm.payload != "original FP16 little endian"
    {
        return Err("research manifest identities, geometry or algorithm".into());
    }
    let record_bytes = sum(&[4, product(&[geometry.dimensions, 2])?])?;
    if membership.len() != product(&[geometry.units, 4])?
        || payload.len() != product(&[geometry.units, record_bytes])?
        || manifest.membership.bytes != membership.len()
        || manifest.leaf_payload.bytes != payload.len()
        || manifest.membership.sha256 != hash(membership)
        || manifest.leaf_payload.sha256 != hash(payload)
    {
        return Err("research body lengths or hashes".into());
    }
    let mut seen = vec![false; geometry.units];
    let mut offset = 0;
    let mut previous: Option<&Leaf> = None;
    for (id, leaf) in manifest.leaves.iter().enumerate() {
        let bytes = product(&[leaf.unit_count, record_bytes])?;
        let end = sum(&[offset, bytes])?;
        let chunk_order = previous.map_or(leaf.chunk_ordinal == 0, |prev| {
            if prev.group_ordinal == leaf.group_ordinal {
                leaf.chunk_ordinal == prev.chunk_ordinal + 1 && prev.unit_count == LEAF_UNITS
            } else {
                leaf.group_ordinal > prev.group_ordinal && leaf.chunk_ordinal == 0
            }
        });
        if leaf.leaf_id != id
            || !(1..=LEAF_UNITS).contains(&leaf.unit_count)
            || leaf.group_ordinal >= algorithm.training_centers
            || !chunk_order
            || leaf.offset != offset
            || leaf.bytes != bytes
            || end > payload.len()
            || leaf.sha256 != hash(&payload[offset..end])
        {
            return Err("leaf numbering, length, bounds or hash".into());
        }
        let mut units = Vec::with_capacity(leaf.unit_count);
        let mut rows = 0;
        for record in payload[offset..end].chunks_exact(record_bytes) {
            let unit = u32::from_le_bytes(record[..4].try_into()?) as usize;
            if unit >= geometry.units
                || seen[unit]
                || u32::from_le_bytes(membership[unit * 4..unit * 4 + 4].try_into()?) as usize != id
            {
                return Err("complete disjoint unit partition or membership".into());
            }
            let original = HEADER_BYTES + unit * geometry.dimensions * 2;
            if record[4..] != blob[original..original + geometry.dimensions * 2] {
                return Err("original FP16 unit identity".into());
            }
            seen[unit] = true;
            rows += unit_row_count(geometry, unit);
            units.push(unit);
        }
        let expected = prototype(&scorer, &units)?;
        if leaf.source_rows != rows
            || leaf.prototype.len() != geometry.dimensions
            || leaf
                .prototype
                .iter()
                .map(|v| v.to_bits())
                .ne(expected.iter().map(|v| v.to_bits()))
        {
            return Err("leaf row count or root prototype".into());
        }
        offset = end;
        previous = Some(leaf);
    }
    if offset != payload.len() || seen.iter().any(|value| !value) {
        return Err("incomplete unit partition".into());
    }
    Ok(())
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn build(root: &Path, root_sha: &str, output: &Path) -> Result<String> {
    if fs::symlink_metadata(output).is_ok() {
        return Err("output already exists".into());
    }
    let root_body = bounded(&root.join("manifest.json"), ROOT_CAP)?;
    if hash(&root_body) != root_sha {
        return Err("input root SHA256".into());
    }
    let input: InputRoot = serde_json::from_slice(&root_body)?;
    if input.schema != INPUT_SCHEMA {
        return Err("requires pinned borsuk-two-bit-generation-v4 input schema".into());
    }
    let blob = bounded(&root.join("centroids.bin"), BLOB_CAP)?;
    if hash(&blob) != input.centroids_sha256 {
        return Err("input centroid SHA256".into());
    }
    let geometry = preflight(&blob, input.canonical.rows, input.canonical.dimensions)?;
    let estimate = admit(geometry, ALLOCATION_CAP)?;
    let scorer = UnitCentroidPages::decode(&blob)?;
    let sample = (0..geometry.units)
        .map(|unit| scorer.unit_centroid(unit).unwrap().to_vec())
        .collect::<Vec<_>>();
    let requested = geometry.units.div_ceil(LEAF_UNITS);
    let identical = sample.iter().all(|vector| vector == &sample[0]);
    let training_centers = if identical { 1 } else { requested };
    // No retry or fallback geometry: duplicate-center and other trainer errors propagate.
    let centers = train_logical_cell_centroids(
        &sample,
        VectorMetric::SquaredEuclidean,
        training_centers,
        ITERATIONS,
    )?;
    let groups = assign_groups(&sample, &centers)?;
    let record_bytes = 4 + geometry.dimensions * 2;
    let mut membership = vec![0_u8; geometry.units * 4];
    let mut payload = Vec::with_capacity(geometry.units * record_bytes);
    let mut leaves = Vec::with_capacity(2 * requested - 1);
    for (group_ordinal, group) in groups.iter().enumerate() {
        for (chunk_ordinal, chunk) in group.chunks(LEAF_UNITS).enumerate() {
            let leaf_id = leaves.len();
            let offset = payload.len();
            let units = chunk.iter().map(|(_, unit)| *unit).collect::<Vec<_>>();
            let mut source_rows = 0;
            for &unit in &units {
                membership[unit * 4..unit * 4 + 4]
                    .copy_from_slice(&u32::try_from(leaf_id)?.to_le_bytes());
                payload.extend_from_slice(&u32::try_from(unit)?.to_le_bytes());
                let original = HEADER_BYTES + unit * geometry.dimensions * 2;
                payload.extend_from_slice(&blob[original..original + geometry.dimensions * 2]);
                source_rows += unit_row_count(geometry, unit);
            }
            leaves.push(Leaf {
                leaf_id,
                group_ordinal,
                chunk_ordinal,
                offset,
                bytes: payload.len() - offset,
                unit_count: units.len(),
                source_rows,
                sha256: hash(&payload[offset..]),
                prototype: prototype(&scorer, &units)?,
            });
        }
    }
    drop((scorer, sample, centers, groups));
    let manifest = Manifest {
        schema: SCHEMA.into(),
        input_schema: INPUT_SCHEMA.into(),
        input_root_sha256: root_sha.into(),
        input_centroids_sha256: hash(&blob),
        input_centroids_bytes: blob.len(),
        rows: geometry.rows,
        dimensions: geometry.dimensions,
        unit_rows: 32,
        page_rows: 256,
        unit_count: geometry.units,
        final_unit_rows: unit_row_count(geometry, geometry.units - 1),
        algorithm: Algorithm {
            trainer: "train_logical_cell_centroids".into(),
            metric: "SquaredEuclidean".into(),
            iterations: ITERATIONS,
            requested_centers: requested,
            training_centers,
            max_leaf_units: LEAF_UNITS,
            nearest_ties: "center ordinal".into(),
            group_sort: "squared distance, original unit ID".into(),
            root_prototype: "unweighted unit mean; f64 accumulation to finite f32".into(),
            normalization: "none".into(),
            payload: "original FP16 little endian".into(),
        },
        modeled_peak_allocation_bytes: estimate,
        modeled_allocation_limit_bytes: ALLOCATION_CAP,
        allocation_model:
            "conservative allocation capacities; not RSS or an enforced process limit".into(),
        membership: Artifact {
            bytes: membership.len(),
            sha256: hash(&membership),
        },
        leaf_payload: Artifact {
            bytes: payload.len(),
            sha256: hash(&payload),
        },
        leaves,
    };
    let body = serde_json::to_vec(&manifest)?;
    verify(&body, &membership, &payload, root_sha, &blob)?;
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    for (name, bytes) in [
        ("manifest.json", body.as_slice()),
        ("membership.bin", &membership),
        ("leaves.bin", &payload),
    ] {
        write_new(&staging.path().join(name), bytes)?;
    }
    let staged_body = bounded(&staging.path().join("manifest.json"), MANIFEST_CAP)?;
    let staged_membership = bounded(&staging.path().join("membership.bin"), membership.len())?;
    let staged_payload = bounded(&staging.path().join("leaves.bin"), payload.len())?;
    if staged_body != body {
        return Err("staged manifest identity".into());
    }
    verify(
        &staged_body,
        &staged_membership,
        &staged_payload,
        root_sha,
        &blob,
    )?;
    File::open(staging.path())?.sync_all()?;
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staging.path(),
        rustix::fs::CWD,
        output,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    File::open(parent)?.sync_all()?;
    Ok(hash(&body))
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err("usage: build_semantic_unit_router ROOT ROOT_SHA OUTPUT_DIR".into());
    }
    println!(
        "{}",
        build(Path::new(&args[1]), &args[2], Path::new(&args[3]))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use half::f16;
    use sha2::{Digest, Sha256};
    use std::fs;

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn fixture(root: &Path, rows: u64, values: &[[u16; 2]]) -> (String, Vec<u8>) {
        fs::create_dir(root).unwrap();
        let mut blob = b"BORSUCP1".to_vec();
        blob.extend_from_slice(&rows.to_le_bytes());
        for number in [2_u32, 32, 256, 0] {
            blob.extend_from_slice(&number.to_le_bytes());
        }
        for vector in values {
            for bits in vector {
                blob.extend_from_slice(&bits.to_le_bytes());
            }
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-two-bit-generation-v4",
            "canonical":{"rows":rows,"dimensions":2},
            "centroids_sha256":digest(&blob)
        }))
        .unwrap();
        fs::write(root.join("manifest.json"), &body).unwrap();
        fs::write(root.join("centroids.bin"), &blob).unwrap();
        (digest(&body), blob)
    }

    #[test]
    fn synthetic_selfcheck() {
        // Missing splitting, normalization, ID loss, or FP16 rewriting breaks this fixture.
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let values = (0..129)
            .map(|id| [if id % 2 == 0 { 0 } else { 0x8000 }, 0])
            .collect::<Vec<_>>();
        let (root_sha, blob) = fixture(&root, 4097, &values);
        let output = temp.path().join("router");
        let manifest_sha = build(&root, &root_sha, &output).unwrap();
        let body = fs::read(output.join("manifest.json")).unwrap();
        assert_eq!(digest(&body), manifest_sha);
        let manifest: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(manifest["unit_count"], 129);
        assert_eq!(manifest["final_unit_rows"], 1);
        assert_eq!(manifest["algorithm"]["training_centers"], 1);
        let leaves = manifest["leaves"].as_array().unwrap();
        assert_eq!(
            leaves
                .iter()
                .map(|leaf| leaf["unit_count"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [64, 64, 1]
        );
        assert_eq!(leaves[2]["source_rows"], 1);
        let membership = fs::read(output.join("membership.bin")).unwrap();
        let payload = fs::read(output.join("leaves.bin")).unwrap();
        assert_eq!(membership.len(), 129 * 4);
        assert_eq!(payload.len(), 129 * 8);
        for (id, record) in payload.as_chunks::<8>().0.iter().enumerate() {
            assert_eq!(
                u32::from_le_bytes(record[..4].try_into().unwrap()) as usize,
                id
            );
            assert_eq!(&record[4..], &blob[32 + id * 4..36 + id * 4]);
            assert_eq!(
                u32::from_le_bytes(membership[id * 4..id * 4 + 4].try_into().unwrap()) as usize,
                id / 64
            );
        }
        let repeat = temp.path().join("repeat");
        assert_eq!(build(&root, &root_sha, &repeat).unwrap(), manifest_sha);
        for name in ["manifest.json", "membership.bin", "leaves.bin"] {
            assert_eq!(
                fs::read(output.join(name)).unwrap(),
                fs::read(repeat.join(name)).unwrap()
            );
        }
        assert!(build(&root, &root_sha, &output).is_err());
        assert!(build(&root, &"0".repeat(64), &temp.path().join("bad-sha")).is_err());
        let mut tampered = blob.clone();
        tampered[32] ^= 1;
        fs::write(root.join("centroids.bin"), &tampered).unwrap();
        assert!(build(&root, &root_sha, &temp.path().join("bad-blob")).is_err());

        let varied_root = temp.path().join("varied");
        let varied = (0..65)
            .map(|id| {
                [
                    f16::from_f32(id as f32).to_bits(),
                    f16::from_f32(2.0).to_bits(),
                ]
            })
            .collect::<Vec<_>>();
        let (sha, original) = fixture(&varied_root, 2049, &varied);
        let varied_output = temp.path().join("varied-router");
        let varied_sha = build(&varied_root, &sha, &varied_output).unwrap();
        assert_eq!(
            build(&varied_root, &sha, &temp.path().join("varied-repeat")).unwrap(),
            varied_sha
        );
        let records = fs::read(varied_output.join("leaves.bin")).unwrap();
        let mut ids = Vec::new();
        for record in records.as_chunks::<8>().0 {
            let id = u32::from_le_bytes(record[..4].try_into().unwrap()) as usize;
            ids.push(id);
            assert_eq!(&record[4..], &original[32 + id * 4..36 + id * 4]);
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..65).collect::<Vec<_>>());

        // f32 summation loses the small term next to 65504; row weighting also
        // changes this mean because the final unit has only one source row.
        let cancellation_root = temp.path().join("cancellation");
        let cancellation = [65504.0, 0.0009765625, -65504.0, 0.0009765625]
            .map(|value| [f16::from_f32(value).to_bits(), f16::from_f32(2.0).to_bits()]);
        let (sha, _) = fixture(&cancellation_root, 97, &cancellation);
        let cancellation_output = temp.path().join("cancellation-router");
        build(&cancellation_root, &sha, &cancellation_output).unwrap();
        let cancellation_manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(cancellation_output.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(
            cancellation_manifest["leaves"][0]["prototype"],
            serde_json::json!([0.00048828125, 2.0])
        );

        // Refresh hashes to prove partition and original-byte checks go beyond hashing.
        let mut altered = payload.clone();
        altered[8..12].copy_from_slice(&0_u32.to_le_bytes());
        let mut forged = manifest.clone();
        forged["leaf_payload"]["sha256"] = digest(&altered).into();
        forged["leaves"][0]["sha256"] = digest(&altered[..512]).into();
        assert!(
            verify(
                &serde_json::to_vec(&forged).unwrap(),
                &membership,
                &altered,
                &root_sha,
                &blob
            )
            .is_err()
        );
        altered = payload.clone();
        altered[4] ^= 1;
        forged["leaf_payload"]["sha256"] = digest(&altered).into();
        forged["leaves"][0]["sha256"] = digest(&altered[..512]).into();
        assert!(
            verify(
                &serde_json::to_vec(&forged).unwrap(),
                &membership,
                &altered,
                &root_sha,
                &blob
            )
            .is_err()
        );
        let mut wrong_membership = membership.clone();
        wrong_membership[..4].copy_from_slice(&1_u32.to_le_bytes());
        forged = manifest.clone();
        forged["membership"]["sha256"] = digest(&wrong_membership).into();
        assert!(
            verify(
                &serde_json::to_vec(&forged).unwrap(),
                &wrong_membership,
                &payload,
                &root_sha,
                &blob
            )
            .is_err()
        );
        forged = manifest.clone();
        forged["leaves"][0]["sha256"] = "0".repeat(64).into();
        assert!(
            verify(
                &serde_json::to_vec(&forged).unwrap(),
                &membership,
                &payload,
                &root_sha,
                &blob
            )
            .is_err()
        );
        forged = manifest.clone();
        forged["leaves"][0]["prototype"][0] = 1.into();
        assert!(
            verify(
                &serde_json::to_vec(&forged).unwrap(),
                &membership,
                &payload,
                &root_sha,
                &blob
            )
            .is_err()
        );
        assert!(
            verify(
                &body,
                &membership[..membership.len() - 1],
                &payload,
                &root_sha,
                &blob
            )
            .is_err()
        );
        assert!(
            verify(
                &body,
                &membership,
                &payload[..payload.len() - 1],
                &root_sha,
                &blob
            )
            .is_err()
        );
        assert!(verify(&body, &membership, &payload, &"0".repeat(64), &blob).is_err());

        let duplicate_root = temp.path().join("duplicates");
        let duplicates = (0..129)
            .map(|id| [f16::from_f32((id % 2) as f32).to_bits(), 0])
            .collect::<Vec<_>>();
        let (sha, _) = fixture(&duplicate_root, 4097, &duplicates);
        let error = build(&duplicate_root, &sha, &temp.path().join("duplicates-out")).unwrap_err();
        assert!(error.to_string().contains("duplicate centroids"), "{error}");

        for (label, bits) in [("nan", 0x7e00), ("inf", 0x7c00)] {
            let invalid_root = temp.path().join(label);
            let (sha, _) = fixture(&invalid_root, 1, &[[bits, 0]]);
            assert!(
                build(
                    &invalid_root,
                    &sha,
                    &temp.path().join(format!("{label}-out"))
                )
                .is_err()
            );
        }

        // An authenticated v6 root must still fail this pinned v4 research adapter.
        let mut wrong_schema: serde_json::Value =
            serde_json::from_slice(&fs::read(varied_root.join("manifest.json")).unwrap()).unwrap();
        wrong_schema["schema"] = "borsuk-two-bit-generation-v6".into();
        let wrong_body = serde_json::to_vec(&wrong_schema).unwrap();
        fs::write(varied_root.join("manifest.json"), &wrong_body).unwrap();
        assert!(
            build(
                &varied_root,
                &digest(&wrong_body),
                &temp.path().join("wrong-schema")
            )
            .is_err()
        );
        fs::write(root.join("manifest.json"), vec![b' '; ROOT_CAP + 1]).unwrap();
        assert!(build(&root, &root_sha, &temp.path().join("large-root")).is_err());
        let oversized = root.join("oversized.bin");
        File::create(&oversized)
            .unwrap()
            .set_len((BLOB_CAP + 1) as u64)
            .unwrap();
        assert!(bounded(&oversized, BLOB_CAP).is_err());
        assert!(!temp.path().join("duplicates-out").exists());
        assert!(!temp.path().join("bad-blob").exists());
    }

    #[test]
    fn header_and_admission_precede_decode() {
        let temp = tempfile::tempdir().unwrap();
        let (_, blob) = fixture(&temp.path().join("root"), 1, &[[0, 0]]);
        let geometry = preflight(&blob, 1, 2).unwrap();
        let admitted = admit(geometry, ALLOCATION_CAP).unwrap();
        assert_eq!(admit(geometry, admitted).unwrap(), admitted);
        assert!(admit(geometry, admitted - 1).is_err());
        let maximal = Geometry {
            rows: 100_000,
            dimensions: 768,
            units: 3125,
            blob_bytes: 4_800_032,
        };
        assert!(admit(maximal, ALLOCATION_CAP).is_ok());
        assert!(
            admit(
                Geometry {
                    units: usize::MAX,
                    ..geometry
                },
                ALLOCATION_CAP
            )
            .is_err()
        );
        assert!(preflight(&blob[..31], 1, 2).is_err());
        assert!(preflight(&blob[..blob.len() - 1], 1, 2).is_err());
        assert!(preflight(&[blob.as_slice(), &[0]].concat(), 1, 2).is_err());
        assert!(preflight(&blob, 2, 2).is_err());
        assert!(preflight(&blob, 1, 3).is_err());
        for (offset, word) in [
            (16, 0_u32),
            (16, 769),
            (20, 0),
            (20, 16),
            (24, 0),
            (24, 128),
            (28, 1),
        ] {
            let mut invalid = blob.clone();
            invalid[offset..offset + 4].copy_from_slice(&word.to_le_bytes());
            assert!(preflight(&invalid, 1, 2).is_err());
        }
        for rows in [0_u64, 100_001, u64::MAX] {
            let mut invalid = blob.clone();
            invalid[8..16].copy_from_slice(&rows.to_le_bytes());
            assert!(preflight(&invalid, 1, 2).is_err());
        }
        let mut invalid = blob.clone();
        invalid[0] ^= 1;
        assert!(preflight(&invalid, 1, 2).is_err());
    }

    #[test]
    fn nearest_ties_and_distance_then_unit_order() {
        let groups = assign_groups(
            &[vec![1.0], vec![0.0], vec![2.0], vec![1.0]],
            &[vec![0.0], vec![2.0]],
        )
        .unwrap();
        assert_eq!(groups, [vec![(0.0, 1), (1.0, 0), (1.0, 3)], vec![(0.0, 2)]]);
    }
}
