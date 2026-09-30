//! Deterministic semantic grouping and whole-leaf nomination over original FP16 unit means.
//!
//! Construction and complete publication validation read all centroids and leaves.
//! Serving opens only an authenticated root and membership, then validates selected
//! whole leaves. The caller owns generation binding, I/O admission and publication;
//! no generation input schema is hard-coded here. Current geometry is deliberately
//! limited to 100k rows/D768 pending separate scale qualification.
use crate::{VectorMetric, train_logical_cell_centroids, unit_centroid_pages::UnitCentroidPages};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, error::Error};

/// Errors include malformed identities/geometry, admission and native trainer failures.
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const ROOT_CAP: usize = 64 * 1024;
const BLOB_CAP: usize = 8 * 1024 * 1024;
const MANIFEST_CAP: usize = 4 * 1024 * 1024;
/// Maximum admitted construction payload, not an RSS limit.
pub const ALLOCATION_CAP: usize = 128 * 1024 * 1024;
const HEADER_BYTES: usize = 32;
const LEAF_UNITS: usize = 64;
const ITERATIONS: usize = 12;
const SCHEMA: &str = "borsuk-semantic-unit-router-research-v1";
#[allow(missing_docs)]
#[derive(Clone, Copy)]
pub struct Geometry {
    pub rows: usize,
    pub dimensions: usize,
    pub units: usize,
    pub blob_bytes: usize,
}

#[allow(missing_docs)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub bytes: usize,
    pub sha256: String,
}

#[allow(missing_docs)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Algorithm {
    pub trainer: String,
    pub metric: String,
    pub iterations: usize,
    pub requested_centers: usize,
    pub training_centers: usize,
    pub max_leaf_units: usize,
    pub nearest_ties: String,
    pub group_sort: String,
    pub root_prototype: String,
    pub normalization: String,
    pub payload: String,
}

#[allow(missing_docs)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Leaf {
    pub leaf_id: usize,
    pub group_ordinal: usize,
    pub chunk_ordinal: usize,
    pub offset: usize,
    pub bytes: usize,
    pub unit_count: usize,
    pub source_rows: usize,
    pub sha256: String,
    pub prototype: Vec<f32>,
}

#[allow(missing_docs)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema: String,
    pub input_schema: String,
    pub input_root_sha256: String,
    pub input_centroids_sha256: String,
    pub input_centroids_bytes: usize,
    pub rows: usize,
    pub dimensions: usize,
    pub unit_rows: usize,
    pub page_rows: usize,
    pub unit_count: usize,
    pub final_unit_rows: usize,
    pub algorithm: Algorithm,
    pub modeled_peak_allocation_bytes: usize,
    pub modeled_allocation_limit_bytes: usize,
    pub allocation_model: String,
    pub membership: Artifact,
    pub leaf_payload: Artifact,
    pub leaves: Vec<Leaf>,
}

/// Authenticated source binding supplied by the enclosing generation or research CLI.
/// `root_sha256` is an input identity, never the output router's own digest.
pub struct SourceIdentity<'a> {
    /// Nonempty source format (at most 256 bytes); caller validates its meaning.
    pub schema: &'a str,
    /// Authenticated input root or source descriptor digest.
    pub root_sha256: &'a str,
    /// Digest of the original native FP16 unit-centroid bytes.
    pub centroids_sha256: &'a str,
    /// Physical source row count.
    pub rows: usize,
    /// Coordinates per unit mean.
    pub dimensions: usize,
}

/// Complete encoded publication, retaining the frozen research byte encoding.
pub struct RouterArtifacts {
    /// JSON root with exact leaf ranges and identities.
    pub manifest: Vec<u8>,
    /// One little-endian u32 leaf ID per original unit.
    pub membership: Vec<u8>,
    /// Concatenated unit ID and unchanged FP16 records.
    pub leaves: Vec<u8>,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(message.into());
    }
    Ok(())
}
fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
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

/// Check native centroid geometry and exact encoded size before decoding.
pub fn preflight(blob: &[u8], rows: usize, dimensions: usize) -> Result<Geometry> {
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

/// Charge the existing conservative construction model before allocation.
pub fn admit(geometry: Geometry, cap: usize) -> Result<usize> {
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

/// Assign original units to trained centers with the frozen distance/ID ordering.
pub fn assign_groups(sample: &[Vec<f32>], centers: &[Vec<f32>]) -> Result<Vec<Vec<(f64, usize)>>> {
    if sample.is_empty()
        || sample[0].is_empty()
        || sample
            .iter()
            .any(|v| v.len() != sample[0].len() || v.iter().any(|x| !x.is_finite()))
        || centers.is_empty()
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

/// Build using the existing deterministic trainer and an explicit allocation cap.
/// The source descriptor must already be authenticated by the caller.
pub fn build(
    blob: &[u8],
    input: &SourceIdentity<'_>,
    allocation_limit: usize,
) -> Result<RouterArtifacts> {
    require(
        allocation_limit <= ALLOCATION_CAP,
        "construction allocation limit",
    )?;
    let geometry = preflight(blob, input.rows, input.dimensions)?;
    let estimate = admit(geometry, allocation_limit)?;
    require(
        (1..=256).contains(&input.schema.len())
            && valid_sha(input.root_sha256)
            && valid_sha(input.centroids_sha256)
            && hash(blob) == input.centroids_sha256,
        "source identity",
    )?;
    let scorer = UnitCentroidPages::decode(blob)?;
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
        input_schema: input.schema.into(),
        input_root_sha256: input.root_sha256.into(),
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
        modeled_allocation_limit_bytes: allocation_limit,
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
    validate_publication(&body, &membership, &payload, input, blob)?;
    Ok(RouterArtifacts {
        manifest: body,
        membership,
        leaves: payload,
    })
}

/// Root and membership authenticated without loading the centroid or leaf objects.
/// Construction limits and the caller's root byte cap bound retained metadata.
pub struct SemanticUnitRouter {
    manifest: Manifest,
    membership: Vec<usize>,
}

impl SemanticUnitRouter {
    /// Authenticate the exact root and membership before exposing leaf ranges.
    /// The enclosing generation supplies both the router SHA and source binding.
    pub fn open(
        body: &[u8],
        membership: &[u8],
        root_sha256: &str,
        input: &SourceIdentity<'_>,
        root_byte_cap: usize,
    ) -> Result<Self> {
        require(
            body.len() <= root_byte_cap.min(MANIFEST_CAP)
                && valid_sha(root_sha256)
                && hash(body) == root_sha256,
            "router root size or identity",
        )?;
        require(
            (1..=100_000).contains(&input.rows)
                && (1..=768).contains(&input.dimensions)
                && (1..=256).contains(&input.schema.len())
                && valid_sha(input.root_sha256)
                && valid_sha(input.centroids_sha256),
            "source identity or geometry",
        )?;
        let manifest: Manifest = serde_json::from_slice(body)?;
        let geometry = Geometry {
            rows: input.rows,
            dimensions: input.dimensions,
            units: input.rows.div_ceil(32),
            blob_bytes: HEADER_BYTES + input.rows.div_ceil(32) * input.dimensions * 2,
        };
        let requested = geometry.units.div_ceil(LEAF_UNITS);
        let algorithm = &manifest.algorithm;
        let estimate = admit(
            geometry,
            manifest.modeled_allocation_limit_bytes.min(ALLOCATION_CAP),
        )?;
        require(
            manifest.schema == SCHEMA
                && manifest.input_schema == input.schema
                && manifest.input_root_sha256 == input.root_sha256
                && manifest.input_centroids_sha256 == input.centroids_sha256
                && manifest.input_centroids_bytes == geometry.blob_bytes
                && manifest.rows == geometry.rows
                && manifest.dimensions == geometry.dimensions
                && manifest.unit_rows == 32
                && manifest.page_rows == 256
                && manifest.unit_count == geometry.units
                && manifest.final_unit_rows == unit_row_count(geometry, geometry.units - 1)
                && manifest.modeled_peak_allocation_bytes == estimate
                && manifest.modeled_allocation_limit_bytes <= ALLOCATION_CAP
                && manifest.allocation_model
                    == "conservative allocation capacities; not RSS or an enforced process limit"
                && algorithm.trainer == "train_logical_cell_centroids"
                && algorithm.metric == "SquaredEuclidean"
                && algorithm.iterations == ITERATIONS
                && algorithm.requested_centers == requested
                && [1, requested].contains(&algorithm.training_centers)
                && algorithm.max_leaf_units == LEAF_UNITS
                && algorithm.nearest_ties == "center ordinal"
                && algorithm.group_sort == "squared distance, original unit ID"
                && algorithm.root_prototype
                    == "unweighted unit mean; f64 accumulation to finite f32"
                && algorithm.normalization == "none"
                && algorithm.payload == "original FP16 little endian",
            "router identities, geometry or algorithm",
        )?;
        let record_bytes = 4 + geometry.dimensions * 2;
        require(
            membership.len() == geometry.units * 4
                && manifest.membership.bytes == membership.len()
                && manifest.membership.sha256 == hash(membership)
                && manifest.leaf_payload.bytes == geometry.units * record_bytes
                && valid_sha(&manifest.leaf_payload.sha256)
                && (1..=2 * requested - 1).contains(&manifest.leaves.len()),
            "router body descriptors",
        )?;
        let membership = membership
            .chunks_exact(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as usize)
            .collect::<Vec<_>>();
        let mut counts = vec![0; manifest.leaves.len()];
        let mut rows = vec![0; manifest.leaves.len()];
        for (unit, &leaf) in membership.iter().enumerate() {
            require(leaf < manifest.leaves.len(), "membership leaf ID")?;
            counts[leaf] += 1;
            rows[leaf] += unit_row_count(geometry, unit);
        }
        let mut offset = 0;
        let mut previous: Option<&Leaf> = None;
        for (id, leaf) in manifest.leaves.iter().enumerate() {
            let ordered = previous.map_or(leaf.chunk_ordinal == 0, |prev| {
                if prev.group_ordinal == leaf.group_ordinal {
                    prev.chunk_ordinal.checked_add(1) == Some(leaf.chunk_ordinal)
                        && prev.unit_count == LEAF_UNITS
                } else {
                    leaf.group_ordinal > prev.group_ordinal && leaf.chunk_ordinal == 0
                }
            });
            require(
                leaf.leaf_id == id
                    && (1..=LEAF_UNITS).contains(&leaf.unit_count)
                    && leaf.group_ordinal < algorithm.training_centers
                    && ordered
                    && leaf.offset == offset
                    && leaf.bytes == leaf.unit_count * record_bytes
                    && leaf.unit_count == counts[id]
                    && leaf.source_rows == rows[id]
                    && valid_sha(&leaf.sha256)
                    && leaf.prototype.len() == geometry.dimensions
                    && leaf.prototype.iter().all(|v| v.is_finite()),
                "leaf numbering, order, geometry or membership",
            )?;
            offset = sum(&[offset, leaf.bytes])?;
            require(offset <= manifest.leaf_payload.bytes, "leaf bounds")?;
            previous = Some(leaf);
        }
        require(
            offset == manifest.leaf_payload.bytes,
            "incomplete leaf ranges",
        )?;
        Ok(Self {
            manifest,
            membership,
        })
    }

    /// Immutable authenticated descriptors for bounded range reads.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Normalize an original f32 cosine query using the existing native helper,
    /// then nominate leaves. Prepare source scoring from the original query first.
    pub fn nominate(&self, query: &[f32]) -> Result<Vec<usize>> {
        let normalized = crate::sq8_source::cosine_vector(query)?;
        self.select_leaves(&normalized)
    }

    /// Frozen nomination for a query already normalized by the native helper.
    pub fn select_leaves(&self, normalized_query: &[f32]) -> Result<Vec<usize>> {
        rank_leaves(
            normalized_query,
            self.manifest
                .leaves
                .iter()
                .map(|leaf| leaf.prototype.as_slice()),
        )
    }

    /// Validate one exact whole-leaf range. Digest authentication fixes record order.
    /// Original coefficient/prototype identities are checked at publication, so
    /// serving needs only IDs and membership, without decoding centroid vectors.
    pub fn validate_leaf(&self, leaf_id: usize, body: &[u8]) -> Result<Vec<usize>> {
        let leaf = self
            .manifest
            .leaves
            .get(leaf_id)
            .ok_or("selected leaf ID")?;
        require(
            body.len() == leaf.bytes && hash(body) == leaf.sha256,
            "selected leaf size/hash",
        )?;
        let mut units = Vec::with_capacity(leaf.unit_count);
        let mut seen = BTreeSet::new();
        let mut rows = 0;
        for record in body.chunks_exact(4 + self.manifest.dimensions * 2) {
            let unit = u32::from_le_bytes(record[..4].try_into()?) as usize;
            require(
                self.membership.get(unit) == Some(&leaf_id) && seen.insert(unit),
                "selected leaf disjoint membership",
            )?;
            rows += (self.manifest.rows - unit * 32).min(32);
            units.push(unit);
        }
        require(
            units.len() == leaf.unit_count && rows == leaf.source_rows,
            "selected leaf count/rows",
        )?;
        Ok(units)
    }

    /// Authenticate the selected whole leaves and retain every unit, with only
    /// missing seed-page units added. No padding beyond the original page closure.
    pub fn validate_selected(&self, leaf_ids: &[usize], bodies: &[&[u8]]) -> Result<Nomination> {
        require(
            !leaf_ids.is_empty() && leaf_ids.len() <= 16 && leaf_ids.len() == bodies.len(),
            "selected leaf count",
        )?;
        let mut selected = BTreeSet::new();
        let mut units = BTreeSet::new();
        let mut bytes = 0;
        for (&id, body) in leaf_ids.iter().zip(bodies) {
            require(selected.insert(id), "duplicate selected leaf")?;
            bytes = sum(&[bytes, body.len()])?;
            require(bytes <= 2 * 1024 * 1024, "selected leaf byte cap")?;
            for unit in self.validate_leaf(id, body)? {
                require(units.insert(unit), "disjoint selected units")?;
            }
        }
        let (seed_page, walk_units, seed_additions) = seed_walk(&units, self.manifest.rows)?;
        let page_closure = units.iter().map(|u| u / 8).collect();
        Ok(Nomination {
            leaf_ids: leaf_ids.to_vec(),
            units,
            seed_page,
            walk_units,
            seed_additions,
            page_closure,
        })
    }
}

/// Complete query nomination after selected leaf authentication.
pub struct Nomination {
    /// Leaf IDs in nomination order.
    pub leaf_ids: Vec<usize>,
    /// Every authenticated semantic unit; never truncated to nearest 256.
    pub units: BTreeSet<usize>,
    /// Lowest physical page containing a semantic unit.
    pub seed_page: usize,
    /// All semantic units plus only missing seed-page units, ordered by unit ID.
    pub walk_units: Vec<usize>,
    /// Units added to satisfy the shared scorer's complete seed-page invariant.
    pub seed_additions: Vec<usize>,
    /// Distinct original physical pages; seed additions cannot enlarge this set.
    pub page_closure: BTreeSet<usize>,
}

/// Publication-only complete partition/original-FP16/prototype validation.
/// Unlike `open` and `validate_selected`, this intentionally reads all objects.
pub fn validate_publication(
    body: &[u8],
    membership: &[u8],
    payload: &[u8],
    input: &SourceIdentity<'_>,
    blob: &[u8],
) -> Result<()> {
    let geometry = preflight(blob, input.rows, input.dimensions)?;
    admit(geometry, ALLOCATION_CAP)?;
    require(
        hash(blob) == input.centroids_sha256,
        "original centroid identity",
    )?;
    let router = SemanticUnitRouter::open(body, membership, &hash(body), input, MANIFEST_CAP)?;
    let manifest = router.manifest();
    require(
        payload.len() == manifest.leaf_payload.bytes
            && hash(payload) == manifest.leaf_payload.sha256,
        "complete payload size/hash",
    )?;
    let scorer = UnitCentroidPages::decode(blob)?;
    let identical =
        (1..geometry.units).all(|unit| scorer.unit_centroid(unit) == scorer.unit_centroid(0));
    require(
        manifest.algorithm.training_centers
            == if identical {
                1
            } else {
                geometry.units.div_ceil(LEAF_UNITS)
            },
        "training center count",
    )?;
    let mut seen = vec![false; geometry.units];
    for leaf in &manifest.leaves {
        let part = &payload[leaf.offset..leaf.offset + leaf.bytes];
        let units = router.validate_leaf(leaf.leaf_id, part)?;
        let expected = prototype(&scorer, &units)?;
        require(
            leaf.prototype
                .iter()
                .map(|v| v.to_bits())
                .eq(expected.iter().map(|v| v.to_bits())),
            "root prototype identity",
        )?;
        for (unit, record) in units
            .into_iter()
            .zip(part.chunks_exact(4 + geometry.dimensions * 2))
        {
            require(!seen[unit], "complete disjoint unit partition")?;
            seen[unit] = true;
            let original = HEADER_BYTES + unit * geometry.dimensions * 2;
            require(
                record[4..] == blob[original..original + geometry.dimensions * 2],
                "original FP16 unit identity",
            )?;
        }
    }
    require(seen.iter().all(|v| *v), "incomplete unit partition")
}

/// Frozen first-eight/1.15-boundary/max-sixteen rule for normalized coordinates.
pub fn select_leaves(query: &[f32], prototypes: &[Vec<f32>]) -> Result<Vec<usize>> {
    rank_leaves(query, prototypes.iter().map(Vec::as_slice))
}
fn rank_leaves<'a>(
    query: &[f32],
    prototypes: impl ExactSizeIterator<Item = &'a [f32]>,
) -> Result<Vec<usize>> {
    require(
        !query.is_empty() && query.iter().all(|x| x.is_finite()) && prototypes.len() > 0,
        "root query geometry",
    )?;
    let mut ranked = Vec::with_capacity(prototypes.len());
    for (id, prototype) in prototypes.enumerate() {
        require(
            prototype.len() == query.len() && prototype.iter().all(|x| x.is_finite()),
            "root prototype geometry",
        )?;
        let mut distance = 0_f64;
        for (&q, &p) in query.iter().zip(prototype) {
            let delta = f64::from(q) - f64::from(p);
            distance += delta * delta;
        }
        require(distance.is_finite(), "root distance")?;
        ranked.push((distance, id));
    }
    ranked.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut count = ranked.len().min(8);
    if count == 8 {
        let boundary = ranked[7].0;
        for &(distance, _) in ranked.iter().take(16).skip(8) {
            if distance > 1.15 * boundary {
                break;
            }
            count += 1;
        }
    }
    Ok(ranked[..count].iter().map(|&(_, id)| id).collect())
}

/// Complete only the lowest nominated physical page, preserving the page closure.
pub fn seed_walk(units: &BTreeSet<usize>, rows: usize) -> Result<(usize, Vec<usize>, Vec<usize>)> {
    require(
        (1..=100_000).contains(&rows)
            && !units.is_empty()
            && units.last().is_some_and(|&u| u < rows.div_ceil(32)),
        "semantic units",
    )?;
    let seed = units.first().ok_or("no semantic seed")? / 8;
    let additions = (seed * 8..((seed + 1) * 8).min(rows.div_ceil(32)))
        .filter(|u| !units.contains(u))
        .collect::<Vec<_>>();
    let mut walk = units.clone();
    walk.extend(additions.iter().copied());
    require(walk.len() <= 1272, "semantic walk unit cap")?;
    Ok((seed, walk.into_iter().collect(), additions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn blob(rows: usize) -> Vec<u8> {
        let mut bytes = b"BORSUCP1".to_vec();
        bytes.extend_from_slice(&(rows as u64).to_le_bytes());
        for word in [2_u32, 32, 256, 0] {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.resize(32 + rows.div_ceil(32) * 4, 0);
        bytes
    }
    fn source(blob: &[u8]) -> SourceIdentity<'static> {
        SourceIdentity {
            schema: "native-generation-test",
            root_sha256: "1111111111111111111111111111111111111111111111111111111111111111",
            centroids_sha256: "",
            rows: u64::from_le_bytes(blob[8..16].try_into().unwrap()) as usize,
            dimensions: 2,
        }
    }
    fn open(a: &RouterArtifacts, source: &SourceIdentity<'_>) -> SemanticUnitRouter {
        SemanticUnitRouter::open(
            &a.manifest,
            &a.membership,
            &hash(&a.manifest),
            source,
            1 << 20,
        )
        .unwrap()
    }
    fn parts<'a>(router: &SemanticUnitRouter, payload: &'a [u8], ids: &[usize]) -> Vec<&'a [u8]> {
        ids.iter()
            .map(|&id| {
                let leaf = &router.manifest().leaves[id];
                &payload[leaf.offset..leaf.offset + leaf.bytes]
            })
            .collect()
    }

    #[test]
    fn schema_marker_is_bounded_before_construction() {
        let blob = blob(1);
        let digest = hash(&blob);
        let marker = "x".repeat(257);
        let source = SourceIdentity {
            schema: &marker,
            centroids_sha256: &digest,
            ..source(&blob)
        };
        assert!(build(&blob, &source, ALLOCATION_CAP).is_err());
    }

    #[test]
    fn selected_validation_is_bounded_and_retains_every_unit_and_partial_tail() {
        let blob = blob(32_769);
        let digest = hash(&blob);
        let source = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let artifacts = build(&blob, &source, ALLOCATION_CAP).unwrap();
        validate_publication(
            &artifacts.manifest,
            &artifacts.membership,
            &artifacts.leaves,
            &source,
            &blob,
        )
        .unwrap();
        let router = open(&artifacts, &source);
        assert_eq!(router.manifest().input_schema, "native-generation-test");
        let ids = router.nominate(&[3.0, 4.0]).unwrap();
        assert_eq!(ids, (0..16).collect::<Vec<_>>());
        assert_eq!(
            ids,
            router
                .select_leaves(&crate::sq8_source::cosine_vector(&[3.0, 4.0]).unwrap())
                .unwrap()
        );
        let selected = parts(&router, &artifacts.leaves, &ids);
        let nomination = router.validate_selected(&ids, &selected).unwrap();
        assert_eq!(nomination.leaf_ids, ids);
        assert_eq!(nomination.units, (0..1024).collect());
        assert_eq!(nomination.walk_units, (0..1024).collect::<Vec<_>>());
        assert!(nomination.seed_additions.is_empty());
        assert_eq!(nomination.page_closure, (0..128).collect());
        let last = router
            .validate_selected(&[16], &parts(&router, &artifacts.leaves, &[16]))
            .unwrap();
        assert_eq!(last.units, [1024].into_iter().collect());
        assert_eq!(last.seed_page, 128);
        assert_eq!(last.page_closure, [128].into_iter().collect());
        assert_eq!(last.walk_units, [1024]);
        assert!(last.seed_additions.is_empty());

        // A bad unselected leaf cannot affect bounded serving validation; complete
        // publication must still reject it. No full payload is passed to serving.
        let mut corrupt = artifacts.leaves.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        router
            .validate_selected(&ids, &parts(&router, &corrupt, &ids))
            .unwrap();
        assert!(
            validate_publication(
                &artifacts.manifest,
                &artifacts.membership,
                &corrupt,
                &source,
                &blob
            )
            .is_err()
        );
        assert!(
            router
                .validate_selected(&[16], &parts(&router, &corrupt, &[16]))
                .is_err()
        );
        assert!(
            router
                .validate_selected(&[0, 0], &[selected[0], selected[0]])
                .is_err()
        );
        assert!(
            router
                .validate_selected(&[0, 1], &[selected[1], selected[0]])
                .is_err()
        );
        assert!(router.validate_selected(&ids, &selected[..15]).is_err());
        assert!(router.validate_selected(&[], &[]).is_err());
        assert!(router.validate_leaf(17, &[]).is_err());
        assert!(
            router
                .validate_leaf(0, &selected[0][..selected[0].len() - 1])
                .is_err()
        );
        assert!(
            router
                .validate_leaf(0, &[selected[0], &[0]].concat())
                .is_err()
        );
        assert!(router.nominate(&[0., 0.]).is_err());
        assert!(router.nominate(&[f32::NAN, 1.]).is_err());
        assert!(router.nominate(&[1.]).is_err());
        assert!(
            SemanticUnitRouter::open(
                &artifacts.manifest,
                &artifacts.membership,
                &hash(&artifacts.manifest),
                &source,
                artifacts.manifest.len() - 1
            )
            .is_err()
        );
        assert!(
            SemanticUnitRouter::open(
                &artifacts.manifest,
                &artifacts.membership,
                &"0".repeat(64),
                &source,
                1 << 20
            )
            .is_err()
        );
        let mut membership = artifacts.membership.clone();
        membership[0] ^= 1;
        assert!(
            SemanticUnitRouter::open(
                &artifacts.manifest,
                &membership,
                &hash(&artifacts.manifest),
                &source,
                1 << 20
            )
            .is_err()
        );
    }

    #[test]
    fn refreshed_hashes_cannot_hide_bad_membership_partition_or_nonfinite_values() {
        let blob = blob(4097);
        let digest = hash(&blob);
        let source = SourceIdentity {
            centroids_sha256: &digest,
            ..source(&blob)
        };
        let artifacts = build(&blob, &source, ALLOCATION_CAP).unwrap();
        let manifest: Value = serde_json::from_slice(&artifacts.manifest).unwrap();
        for (field, value) in [
            ("leaf_id", json!(1)),
            ("unit_count", json!(63)),
            ("offset", json!(1)),
            ("source_rows", json!(1)),
            ("group_ordinal", json!(1)),
            ("chunk_ordinal", json!(1)),
            ("prototype", json!([null, 0.])),
        ] {
            let mut bad = manifest.clone();
            bad["leaves"][0][field] = value;
            let body = serde_json::to_vec(&bad).unwrap();
            assert!(
                SemanticUnitRouter::open(
                    &body,
                    &artifacts.membership,
                    &hash(&body),
                    &source,
                    1 << 20
                )
                .is_err(),
                "{field}"
            );
        }
        for field in ["algorithm", "membership", "rows", "leaves"] {
            let mut bad = manifest.clone();
            bad[field] = Value::Null;
            let body = serde_json::to_vec(&bad).unwrap();
            assert!(
                SemanticUnitRouter::open(
                    &body,
                    &artifacts.membership,
                    &hash(&body),
                    &source,
                    1 << 20
                )
                .is_err()
            );
        }
        for id in [1_u32, u32::MAX] {
            let mut membership = artifacts.membership.clone();
            membership[..4].copy_from_slice(&id.to_le_bytes());
            let mut bad = manifest.clone();
            bad["membership"]["sha256"] = hash(&membership).into();
            let body = serde_json::to_vec(&bad).unwrap();
            assert!(
                SemanticUnitRouter::open(&body, &membership, &hash(&body), &source, 1 << 20)
                    .is_err()
            );
        }
        let first_bytes = manifest["leaves"][0]["bytes"].as_u64().unwrap() as usize;
        for (offset, bytes) in [
            (0, 1_u32.to_le_bytes().to_vec()),
            (0, 64_u32.to_le_bytes().to_vec()),
            (4, 0x7e00_u16.to_le_bytes().to_vec()),
        ] {
            let mut payload = artifacts.leaves.clone();
            payload[offset..offset + bytes.len()].copy_from_slice(&bytes);
            let mut bad = manifest.clone();
            bad["leaves"][0]["sha256"] = hash(&payload[..first_bytes]).into();
            bad["leaf_payload"]["sha256"] = hash(&payload).into();
            let body = serde_json::to_vec(&bad).unwrap();
            let router = SemanticUnitRouter::open(
                &body,
                &artifacts.membership,
                &hash(&body),
                &source,
                1 << 20,
            )
            .unwrap();
            if offset == 0 {
                assert!(
                    router
                        .validate_selected(&[0], &[&payload[..first_bytes]])
                        .is_err()
                );
            }
            assert!(
                validate_publication(&body, &artifacts.membership, &payload, &source, &blob)
                    .is_err()
            );
        }
        // Signed-zero tampering preserves the prototype. Only full publication
        // has the original centroid authority needed to reject rewritten FP16.
        let mut payload = artifacts.leaves.clone();
        payload[5] = 0x80;
        let mut bad = manifest.clone();
        bad["leaves"][0]["sha256"] = hash(&payload[..first_bytes]).into();
        bad["leaf_payload"]["sha256"] = hash(&payload).into();
        let body = serde_json::to_vec(&bad).unwrap();
        let router =
            SemanticUnitRouter::open(&body, &artifacts.membership, &hash(&body), &source, 1 << 20)
                .unwrap();
        router
            .validate_selected(&[0], &[&payload[..first_bytes]])
            .unwrap();
        assert!(
            validate_publication(&body, &artifacts.membership, &payload, &source, &blob).is_err()
        );
        assert!(build(&blob, &source, 1).is_err());
        assert!(build(&blob, &source, ALLOCATION_CAP + 1).is_err());
    }
}
