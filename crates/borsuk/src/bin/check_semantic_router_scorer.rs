//! Bounded offline paired scorer. Logical range charges are not physical S3 I/O.
use borsuk::{
    budgeted_page_rank::BudgetedPagePlan,
    exact_sq8_nominee::Sq8Geometry,
    returned_sq8::{ReturnedRange, rank_returned_ranges},
    rotated_two_bit::RotatedTwoBitCodec,
    two_bit_generation::{
        TwoBitDiscoveryTrace, TwoBitPlanTrace, normalize_two_bit_diagnostic_query,
        plan_two_bit_source_cover, plan_two_bit_source_walks,
    },
    unit_centroid_graph::UnitCentroidGraph,
    unit_centroid_pages::UnitCentroidPages,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    error::Error,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Write},
    ops::Range,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn require(ok: bool, message: &str) -> Result<()> {
    if !ok {
        return Err(message.into());
    }
    Ok(())
}
fn hash(body: &[u8]) -> String {
    format!("{:x}", Sha256::digest(body))
}
fn select_leaves(query: &[f32], prototypes: &[Vec<f32>]) -> Result<Vec<usize>> {
    require(
        !query.is_empty() && query.iter().all(|x| x.is_finite()) && !prototypes.is_empty(),
        "root query geometry",
    )?;
    let mut ranked = Vec::with_capacity(prototypes.len());
    for (id, prototype) in prototypes.iter().enumerate() {
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
fn seed_walk(units: &BTreeSet<usize>, rows: usize) -> Result<(usize, Vec<usize>, Vec<usize>)> {
    require(
        rows > 0 && !units.is_empty() && units.last().is_some_and(|&u| u < rows.div_ceil(32)),
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
fn inverse_order(order: &[u64]) -> Result<Vec<usize>> {
    require(!order.is_empty(), "empty row order")?;
    let mut inverse = vec![usize::MAX; order.len()];
    for (physical, &logical) in order.iter().enumerate() {
        let logical = usize::try_from(logical)?;
        require(
            logical < order.len() && inverse[logical] == usize::MAX,
            "row order must be a permutation",
        )?;
        inverse[logical] = physical;
    }
    Ok(inverse)
}
fn guarded_record(
    row: usize,
    record_bytes: usize,
    ranges: &[(Range<usize>, Vec<u8>)],
) -> Option<&[u8]> {
    let start = row.checked_mul(record_bytes)?;
    let end = start.checked_add(record_bytes)?;
    ranges.iter().find_map(|(range, body)| {
        if start >= range.start && end <= range.end {
            body.get(start - range.start..end - range.start)
        } else {
            None
        }
    })
}
fn parity(actual: &Value, expected: &Value, fields: &[&str]) -> Result<()> {
    for &field in fields {
        require(
            actual.get(field).is_some_and(|v| !v.is_null())
                && actual.get(field) == expected.get(field),
            &format!("control parity: {field}"),
        )?;
    }
    Ok(())
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: PathBuf,
    bytes: usize,
    sha256: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Inputs {
    root: Artifact,
    plane_manifest: Artifact,
    plane_mean: Artifact,
    plane_records: Artifact,
    sq8: Artifact,
    centroids: Artifact,
    graph: Artifact,
    diverse_graph: Artifact,
    router_manifest: Artifact,
    router_membership: Artifact,
    router_leaves: Artifact,
    requests: Artifact,
    truth: Artifact,
    order: Artifact,
    binding: Artifact,
    control_plan: Artifact,
    control_returned: Artifact,
}
impl Inputs {
    fn all(&self) -> [(&str, &Artifact, usize); 17] {
        [
            ("root", &self.root, 65536),
            ("plane_manifest", &self.plane_manifest, 65536),
            ("plane_mean", &self.plane_mean, 3072),
            ("plane_records", &self.plane_records, 20_000_000),
            ("sq8", &self.sq8, 78_000_000),
            ("centroids", &self.centroids, 4_800_032),
            ("graph", &self.graph, 8 << 20),
            ("diverse_graph", &self.diverse_graph, 8 << 20),
            ("router_manifest", &self.router_manifest, 1 << 20),
            ("router_membership", &self.router_membership, 12500),
            ("router_leaves", &self.router_leaves, 4_812_500),
            ("requests", &self.requests, 32 << 20),
            ("truth", &self.truth, 400_000),
            ("order", &self.order, 800_000),
            ("binding", &self.binding, 65536),
            ("control_plan", &self.control_plan, 8 << 20),
            ("control_returned", &self.control_returned, 1 << 20),
        ]
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    combined_bytes: usize,
    combined_gets: usize,
    router_leaf_bytes: usize,
    router_leaf_gets: usize,
    router_root_bytes: usize,
    source_bytes: usize,
    source_gets: usize,
    source_parallel: usize,
    sq8_bytes: usize,
    sq8_gets: usize,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    dataset: String,
    rows: usize,
    dimensions: usize,
    first: usize,
    count: usize,
    inputs: Inputs,
    limits: Limits,
}
fn validate_config(c: &Config) -> Result<()> {
    require(
        c.schema == "borsuk-semantic-router-scorer-config-v1"
            && matches!(c.dataset.as_str(), "ReLAION" | "CoHere")
            && (c.rows, c.dimensions, c.first, c.count) == (100000, 768, 0, 64),
        "frozen dataset/panel geometry",
    )?;
    let l = &c.limits;
    require(
        (
            l.combined_bytes,
            l.combined_gets,
            l.router_leaf_bytes,
            l.router_leaf_gets,
            l.router_root_bytes,
            l.source_bytes,
            l.source_gets,
            l.source_parallel,
            l.sq8_bytes,
            l.sq8_gets,
        ) == (
            83881984, 160, 2097152, 16, 1048576, 67108864, 128, 16, 16773120, 32,
        ),
        "frozen physical limits",
    )?;
    let mut paths = BTreeSet::new();
    for (_, a, cap) in c.inputs.all() {
        require(
            a.path.is_absolute()
                && paths.insert(&a.path)
                && a.bytes > 0
                && a.bytes <= cap
                && a.sha256.len() == 64
                && a.sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "artifact path/bytes/SHA cap",
        )?;
    }
    for (a, size) in [
        (&c.inputs.plane_mean, 3072),
        (&c.inputs.plane_records, 20000000),
        (&c.inputs.sq8, 78000000),
        (&c.inputs.centroids, 4800032),
        (&c.inputs.order, 800000),
        (&c.inputs.truth, 400000),
        (&c.inputs.router_membership, 12500),
        (&c.inputs.router_leaves, 4812500),
    ] {
        require(a.bytes == size, "fixed artifact geometry")?;
    }
    Ok(())
}
fn open_regular(path: &Path, bytes: usize, cap: usize) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NONBLOCK | rustix::fs::OFlags::NOFOLLOW).bits() as i32)
        .open(path)?;
    let metadata = file.metadata()?;
    require(
        metadata.is_file() && bytes <= cap && metadata.len() == bytes as u64,
        "regular artifact length/cap",
    )?;
    Ok(file)
}
fn authenticate(a: &Artifact, cap: usize) -> Result<File> {
    let file = open_regular(&a.path, a.bytes, cap)?;
    let mut reader = BufReader::with_capacity(65536, &file);
    let mut digest = Sha256::new();
    let mut block = [0_u8; 65536];
    let mut count = 0;
    loop {
        let n = reader.read(&mut block)?;
        if n == 0 {
            break;
        }
        count += n;
        require(count <= a.bytes, "artifact grew during authentication")?;
        digest.update(&block[..n]);
    }
    require(
        count == a.bytes && format!("{digest:x}", digest = digest.finalize()) == a.sha256,
        &format!("artifact identity: {}", a.path.display()),
    )?;
    Ok(file)
}
fn read_at(file: &File, range: Range<usize>) -> Result<Vec<u8>> {
    require(range.start < range.end, "empty or reversed read range")?;
    let mut body = vec![0; range.end - range.start];
    file.read_exact_at(&mut body, range.start as u64)?;
    Ok(body)
}
fn checked(a: &Artifact, cap: usize) -> Result<Vec<u8>> {
    read_at(&authenticate(a, cap)?, 0..a.bytes)
}
fn read_ranges(file: &File, ranges: &[Range<usize>]) -> Result<Vec<(Range<usize>, Vec<u8>)>> {
    ranges
        .iter()
        .map(|r| Ok((r.clone(), read_at(file, r.clone())?)))
        .collect()
}

#[derive(Deserialize)]
struct Geometry {
    rows: usize,
    dimensions: usize,
}
#[derive(Deserialize)]
struct Root {
    schema: String,
    generation: u64,
    base_epoch: u64,
    canonical: Geometry,
    plane_manifest_sha256: String,
    centroids_sha256: String,
    graph_sha256: String,
    diverse_graph_sha256: String,
    sq8_object_sha256: String,
    sq8_object_key: String,
    sq8_etag: String,
    low: Vec<f32>,
    step: Vec<f32>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plane {
    schema: String,
    rows: usize,
    dimensions: usize,
    seed: u32,
    record_bytes: usize,
    source_sha256: String,
    sq8_sha256: String,
    source_order_sha256: String,
    mean_sha256: String,
    records_sha256: String,
    query_or_truth_used: bool,
}
#[derive(Deserialize)]
struct Binding {
    candidate_root_sha256: String,
    control_root_sha256: String,
    source_order_sha256: String,
    coefficient_f32_bits_exact: bool,
    per_id_sq8_payload_exact: bool,
    source_precision_bits: Precision,
    sq8_object_key: String,
    sq8_etag: String,
}
#[derive(Deserialize)]
struct Precision {
    candidate: usize,
    control: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BodyIdentity {
    bytes: usize,
    sha256: String,
}
#[derive(Deserialize)]
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Router {
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
    algorithm: Value,
    modeled_peak_allocation_bytes: usize,
    modeled_allocation_limit_bytes: usize,
    allocation_model: String,
    membership: BodyIdentity,
    leaf_payload: BodyIdentity,
    leaves: Vec<Leaf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    query_ordinal: usize,
    query: Vec<f32>,
    #[serde(default, deserialize_with = "present_ignored")]
    nominees: Option<serde::de::IgnoredAny>,
    #[serde(default, deserialize_with = "present_ignored")]
    primary_count: Option<serde::de::IgnoredAny>,
}
fn present_ignored<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<serde::de::IgnoredAny>, D::Error> {
    serde::de::IgnoredAny::deserialize(deserializer).map(Some)
}

struct Context {
    root: Root,
    codec: RotatedTwoBitCodec,
    centroids: UnitCentroidPages,
    graphs: Vec<UnitCentroidGraph>,
    router: Router,
    membership: Vec<usize>,
    prototypes: Vec<Vec<f32>>,
    leaf_pages: Vec<usize>,
    leaves: File,
    records: File,
    sq8: File,
    inverse: Vec<usize>,
    requests: Vec<Vec<f32>>,
    historical_plans: Vec<Value>,
    historical_ids: Vec<Value>,
}
fn json_lines(body: &[u8]) -> Result<Vec<Value>> {
    std::str::from_utf8(body)?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}
fn validate_router(
    c: &Config,
    router: &Router,
    membership: &[usize],
    payload: &[u8],
    blob: &[u8],
    centroids: &UnitCentroidPages,
) -> Result<Vec<usize>> {
    require(
        router.schema == "borsuk-semantic-unit-router-research-v1"
            && router.input_schema == "borsuk-two-bit-generation-v4"
            && router.input_root_sha256 == c.inputs.root.sha256
            && router.input_centroids_sha256 == c.inputs.centroids.sha256
            && (
                router.input_centroids_bytes,
                router.rows,
                router.dimensions,
                router.unit_rows,
                router.page_rows,
                router.unit_count,
                router.final_unit_rows,
            ) == (4800032, c.rows, c.dimensions, 32, 256, 3125, 32)
            && router.membership.bytes == c.inputs.router_membership.bytes
            && router.membership.sha256 == c.inputs.router_membership.sha256
            && router.leaf_payload.bytes == payload.len()
            && router.leaf_payload.sha256 == c.inputs.router_leaves.sha256
            && membership.len() == 3125
            && (1..=97).contains(&router.leaves.len())
            && router.modeled_peak_allocation_bytes <= 128 << 20
            && router.modeled_allocation_limit_bytes == 128 << 20
            && router.allocation_model
                == "conservative allocation capacities; not RSS or an enforced process limit",
        "router authority/geometry",
    )?;
    let identical = (1..3125).all(|u| centroids.unit_centroid(u) == centroids.unit_centroid(0));
    require(
        router.algorithm
            == json!({"trainer":"train_logical_cell_centroids","metric":"SquaredEuclidean",
        "iterations":12,"requested_centers":49,"training_centers":if identical {1} else {49},"max_leaf_units":64,
        "nearest_ties":"center ordinal","group_sort":"squared distance, original unit ID",
        "root_prototype":"unweighted unit mean; f64 accumulation to finite f32","normalization":"none","payload":"original FP16 little endian"}),
        "frozen construction algorithm",
    )?;
    let mut seen = vec![false; 3125];
    let mut offset = 0_usize;
    let mut previous: Option<&Leaf> = None;
    let mut leaf_pages = Vec::new();
    for (id, leaf) in router.leaves.iter().enumerate() {
        let bytes = leaf
            .unit_count
            .checked_mul(1540)
            .ok_or("leaf length overflow")?;
        let end = offset.checked_add(bytes).ok_or("leaf offset overflow")?;
        let ordered = previous.map_or(leaf.chunk_ordinal == 0, |p| {
            if p.group_ordinal == leaf.group_ordinal {
                leaf.chunk_ordinal == p.chunk_ordinal + 1 && p.unit_count == 64
            } else {
                leaf.group_ordinal > p.group_ordinal && leaf.chunk_ordinal == 0
            }
        });
        require(
            leaf.leaf_id == id
                && (1..=64).contains(&leaf.unit_count)
                && leaf.offset == offset
                && leaf.bytes == bytes
                && end <= payload.len()
                && ordered
                && leaf.group_ordinal < if identical { 1 } else { 49 }
                && leaf.prototype.len() == 768
                && hash(&payload[offset..end]) == leaf.sha256,
            "leaf identity/shape/order",
        )?;
        let mut sums = vec![0_f64; 768];
        let mut rows = 0;
        let mut pages = BTreeSet::new();
        for record in payload[offset..end].chunks_exact(1540) {
            let unit = u32::from_le_bytes(record[..4].try_into()?) as usize;
            require(
                unit < 3125 && !seen[unit] && membership[unit] == id,
                "leaf disjoint membership",
            )?;
            seen[unit] = true;
            let original = 32 + unit * 1536;
            require(
                record[4..] == blob[original..original + 1536],
                "original FP16 centroid identity",
            )?;
            for (sum, &value) in sums
                .iter_mut()
                .zip(centroids.unit_centroid(unit).ok_or("centroid unit")?)
            {
                *sum += f64::from(value);
            }
            rows += (c.rows - unit * 32).min(32);
            pages.insert(unit / 8);
        }
        require(
            leaf.source_rows == rows
                && leaf.prototype.iter().zip(sums).all(|(&p, sum)| {
                    p.is_finite()
                        && p.to_bits() == ((sum / leaf.unit_count as f64) as f32).to_bits()
                }),
            "root prototype identity",
        )?;
        leaf_pages.push(pages.len());
        previous = Some(leaf);
        offset = end;
    }
    require(
        offset == payload.len() && seen.iter().all(|&x| x),
        "complete leaf partition",
    )?;
    Ok(leaf_pages)
}

fn load(c: &Config, stage: &mut String) -> Result<Context> {
    *stage = "authenticate root/plane/scorer identities".into();
    let i = &c.inputs;
    let root: Root = serde_json::from_slice(&checked(&i.root, 65536)?)?;
    let plane: Plane = serde_json::from_slice(&checked(&i.plane_manifest, 65536)?)?;
    let binding: Binding = serde_json::from_slice(&checked(&i.binding, 65536)?)?;
    require(
        root.schema == "borsuk-two-bit-generation-v4"
            && root.canonical.rows == c.rows
            && root.canonical.dimensions == c.dimensions
            && root.plane_manifest_sha256 == i.plane_manifest.sha256
            && root.centroids_sha256 == i.centroids.sha256
            && root.graph_sha256 == i.graph.sha256
            && root.diverse_graph_sha256 == i.diverse_graph.sha256
            && root.sq8_object_sha256 == i.sq8.sha256
            && root.low.len() == c.dimensions
            && root.step.len() == c.dimensions
            && root.low.iter().all(|v| v.is_finite())
            && root.step.iter().all(|v| v.is_finite() && *v > 0.),
        "historical root/scorer binding",
    )?;
    require(
        plane.schema == "borsuk-two-bit-plane-v2"
            && (plane.rows, plane.dimensions, plane.seed, plane.record_bytes)
                == (c.rows, c.dimensions, 20260923, 200)
            && !plane.query_or_truth_used
            && plane.source_sha256.len() == 64
            && plane.sq8_sha256 == i.sq8.sha256
            && plane.source_order_sha256 == i.order.sha256
            && plane.mean_sha256 == i.plane_mean.sha256
            && plane.records_sha256 == i.plane_records.sha256,
        "source plane/scorer binding",
    )?;
    require(
        binding.candidate_root_sha256 == i.root.sha256
            && binding.control_root_sha256 == i.root.sha256
            && binding.source_order_sha256 == i.order.sha256
            && binding.coefficient_f32_bits_exact
            && binding.per_id_sq8_payload_exact
            && (
                binding.source_precision_bits.candidate,
                binding.source_precision_bits.control,
            ) == (2, 2)
            && binding.sq8_object_key == root.sq8_object_key
            && binding.sq8_etag == root.sq8_etag,
        "logical/physical binding",
    )?;
    let mean = checked(&i.plane_mean, 3072)?
        .chunks_exact(4)
        .map(|word| f32::from_le_bytes(word.try_into().unwrap()))
        .collect::<Vec<_>>();
    let codec = RotatedTwoBitCodec::new(&mean, plane.seed)?;
    require(codec.record_bytes() == 200, "codec record geometry")?;
    *stage = "authenticate original centroids/graphs/router".into();
    let blob = checked(&i.centroids, 4800032)?;
    let centroids = UnitCentroidPages::decode(&blob)?;
    require(
        (
            centroids.rows(),
            centroids.dimensions(),
            centroids.unit_rows(),
            centroids.page_rows(),
            centroids.unit_count(),
        ) == (c.rows, c.dimensions, 32, 256, 3125),
        "original centroid geometry",
    )?;
    let mut graphs = Vec::new();
    for a in [&i.graph, &i.diverse_graph] {
        let bytes = checked(a, 8 << 20)?;
        let graph = UnitCentroidGraph::decode(&bytes, &blob, &centroids)?;
        require(graph.node_count() == 3125, "graph nodes")?;
        graphs.push(graph);
    }
    if i.graph.sha256 == i.diverse_graph.sha256 {
        graphs.truncate(1);
    }
    let router: Router =
        serde_json::from_slice(&checked(&i.router_manifest, c.limits.router_root_bytes)?)?;
    let membership = checked(&i.router_membership, 12500)?
        .chunks_exact(4)
        .map(|word| u32::from_le_bytes(word.try_into().unwrap()) as usize)
        .collect::<Vec<_>>();
    let leaves = authenticate(&i.router_leaves, 4812500)?;
    let payload = read_at(&leaves, 0..i.router_leaves.bytes)?;
    let leaf_pages = validate_router(c, &router, &membership, &payload, &blob, &centroids)?;
    let prototypes = router
        .leaves
        .iter()
        .map(|leaf| leaf.prototype.clone())
        .collect();
    drop(payload);
    drop(blob);
    *stage = "authenticate physical order/source/SQ8".into();
    let order = checked(&i.order, 800000)?
        .chunks_exact(8)
        .map(|word| u64::from_le_bytes(word.try_into().unwrap()))
        .collect::<Vec<_>>();
    let inverse = inverse_order(&order)?;
    let records = authenticate(&i.plane_records, 20_000_000)?;
    let sq8 = authenticate(&i.sq8, 78_000_000)?;
    // Startup identity validation only; these scans are never charged as serving reads.
    for first in (0..c.rows).step_by(256) {
        let end = (first + 256).min(c.rows);
        let body = read_at(&sq8, first * 780..end * 780)?;
        for (physical, row) in (first..end).zip(body.chunks_exact(780)) {
            require(
                i64::from_le_bytes(row[..8].try_into()?) == order[physical] as i64,
                "SQ8 logical ID/order mismatch",
            )?;
        }
    }
    *stage = "authenticate requests/historical parity authorities".into();
    let body = checked(&i.requests, 32 << 20)?;
    let mut requests = Vec::with_capacity(c.count);
    let mut request_count = 0;
    for (ordinal, line) in std::str::from_utf8(&body)?.lines().enumerate() {
        let request: Request = serde_json::from_str(line)?;
        require(
            request.query_ordinal == ordinal
                && request.query.len() == c.dimensions
                && request.nominees.is_some() == request.primary_count.is_some(),
            "request ordinal/shape",
        )?;
        // Historical nominee fields are deliberately ignored. They cannot enter either arm.
        normalize_two_bit_diagnostic_query(&request.query)?;
        if ordinal < c.count {
            requests.push(request.query);
        }
        request_count += 1;
    }
    require(
        request_count == 1000 && requests.len() == 64,
        "consumed request panel",
    )?;
    let historical_plans = json_lines(&checked(&i.control_plan, 8 << 20)?)?;
    require(
        historical_plans.len() == c.count
            && historical_plans
                .iter()
                .enumerate()
                .all(|(q, v)| v["query_ordinal"].as_u64() == Some(q as u64)),
        "historical plan ordinals",
    )?;
    let native = json_lines(&checked(&i.control_returned, 1 << 20)?)?;
    require(
        native.len() == c.count + 2
            && native[0]["phase"] == "startup"
            && native[0]["root_sha256"] == i.root.sha256
            && native[0]["generation"].as_u64() == Some(root.generation)
            && native[0]["control_epoch"].as_u64() == root.base_epoch.checked_add(1)
            && native.last().is_some_and(|v| {
                v["phase"] == "summary" && v["count"].as_u64() == Some(c.count as u64)
            }),
        "historical native startup/terminal binding",
    )?;
    let historical_ids = native.into_iter().skip(1).take(c.count).collect::<Vec<_>>();
    for (q, v) in historical_ids.iter().enumerate() {
        require(
            v["phase"] == "query"
                && v["query_ordinal"].as_u64() == Some(q as u64)
                && v["failed_gets"].as_u64() == Some(0)
                && v["ids"].as_array().is_some_and(|ids| ids.len() == 100),
            "historical successful native query",
        )?;
        parity(
            v,
            &historical_plans[q],
            &["query_ordinal", "ranges", "planned_bytes"],
        )?;
    }
    Ok(Context {
        root,
        codec,
        centroids,
        graphs,
        router,
        membership,
        prototypes,
        leaf_pages,
        leaves,
        records,
        sq8,
        inverse,
        requests,
        historical_plans,
        historical_ids,
    })
}

fn discover(
    context: &Context,
    query: &[f32],
    trace: &mut TwoBitPlanTrace,
) -> Result<Vec<(usize, Vec<usize>)>> {
    // EXACT TwoBitGeneration::discover_walks sequence; original FP16 graph authority.
    let mut walks = Vec::with_capacity(2);
    for graph in &context.graphs {
        let seed = graph.search(&context.centroids, query, 1, 128)?;
        let seed_page = seed.units.first().ok_or("no control seed")?.0 / 8;
        let found =
            graph.search_pages_seeded(&context.centroids, query, &[seed_page], 158, 1272)?;
        trace.discoveries.push(TwoBitDiscoveryTrace {
            seed_page,
            seed_evaluated_units: seed.evaluated_units,
            seed_work_exhausted: seed.work_exhausted,
            walk_evaluated_units: found.evaluated_units.clone(),
            walk_work_exhausted: found.work_exhausted,
        });
        walks.push((seed_page, found.evaluated_units));
    }
    Ok(walks)
}
fn cpu_ns() -> i128 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    i128::from(t.tv_sec) * 1_000_000_000 + i128::from(t.tv_nsec)
}
struct Timer {
    wall: Instant,
    cpu: i128,
}
impl Timer {
    fn start() -> Self {
        Self {
            wall: Instant::now(),
            cpu: cpu_ns(),
        }
    }
    fn finish(&self) -> Value {
        json!({"wall_ns":self.wall.elapsed().as_nanos(),"process_cpu_ns":cpu_ns()-self.cpu})
    }
}
struct Events {
    file: File,
    first: bool,
}
impl Events {
    fn emit(&mut self, event: &Value) -> Result<()> {
        if !self.first {
            self.file.write_all(b",\n")?;
        }
        self.first = false;
        serde_json::to_writer(&mut self.file, event)?;
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }
}
struct Arm {
    units: BTreeSet<usize>,
    walk_units: BTreeSet<usize>,
    scored_units: BTreeSet<usize>,
    closure: BTreeSet<usize>,
    source_cover: Vec<Range<usize>>,
    plan: BudgetedPagePlan,
    ids: Vec<i64>,
    report: Value,
}
fn plan_value(q: usize, plan: &BudgetedPagePlan, trace: &TwoBitPlanTrace) -> Value {
    json!({"query_ordinal":q,"ranges":plan.ranges.iter().map(|r| [r.start,r.end]).collect::<Vec<_>>(),
        "planned_bytes":plan.planned_bytes,"ranked_candidate_pages":trace.ranked_candidate_pages,
        "selected_pages":plan.selected_pages,"primary_page":trace.primary_page,
        "nomination_evaluated_units":trace.nomination_evaluated_units,"discoveries":trace.discoveries})
}
fn run_arm(
    c: &Config,
    context: &Context,
    q: usize,
    candidate: bool,
    events: &mut Events,
    stage: &mut String,
) -> Result<Arm> {
    let arm = if candidate { "candidate" } else { "control" };
    *stage = format!("query {q} {arm}: prepare/discovery");
    let total = Timer::start();
    let timer = Timer::start();
    // Source scoring MUST prepare from the original f32 request, before cosine normalization.
    let original = &context.requests[q];
    let prepared = context.codec.prepare_query(original, 400000)?;
    let query = normalize_two_bit_diagnostic_query(original)?;
    let prepare_timing = timer.finish();
    let timer = Timer::start();
    let mut trace = TwoBitPlanTrace::default();
    let mut selected_leaves = Vec::new();
    let mut leaf_bytes = 0;
    let mut seed_additions = Vec::new();
    let units;
    let walks;
    if candidate {
        selected_leaves = select_leaves(&query, &context.prototypes)?;
        leaf_bytes = selected_leaves
            .iter()
            .map(|&id| context.router.leaves[id].bytes)
            .sum();
        require(
            selected_leaves.len() <= c.limits.router_leaf_gets
                && leaf_bytes <= c.limits.router_leaf_bytes,
            "router selected-leaf budget",
        )?;
        let mut nominated = BTreeSet::new();
        for &id in &selected_leaves {
            let leaf = &context.router.leaves[id];
            let body = read_at(&context.leaves, leaf.offset..leaf.offset + leaf.bytes)?;
            require(hash(&body) == leaf.sha256, "query selected-leaf identity")?;
            for record in body.chunks_exact(1540) {
                let unit = u32::from_le_bytes(record[..4].try_into()?) as usize;
                require(
                    context.membership.get(unit) == Some(&id) && nominated.insert(unit),
                    "query leaf unit membership",
                )?;
            }
        }
        units = nominated;
        let (seed, walked, additions) = seed_walk(&units, c.rows)?;
        seed_additions = additions;
        walks = vec![(seed, walked)];
    } else {
        walks = discover(context, &query, &mut trace)?;
        units = walks.iter().flat_map(|(_, u)| u.iter().copied()).collect();
    }
    let discovery_timing = timer.finish();
    let closure = units.iter().map(|u| u / 8).collect::<BTreeSet<_>>();
    require(
        !closure.is_empty()
            && (!candidate || closure.len() <= 159)
            && walks
                .iter()
                .flat_map(|(_, u)| u)
                .all(|u| closure.contains(&(u / 8))),
        "nomination page closure",
    )?;
    *stage = format!("query {q} {arm}: source physical admission/read");
    let timer = Timer::start();
    let (source_cover, source_bytes) = plan_two_bit_source_cover(
        &closure,
        c.rows,
        200,
        c.limits.source_gets,
        c.limits.source_bytes,
    )?;
    let source_data = read_ranges(&context.records, &source_cover)?;
    let source_timing = timer.finish();
    *stage = format!("query {q} {arm}: shared source selection/SQ8 admission");
    let timer = Timer::start();
    let plan = plan_two_bit_source_walks(
        c.rows,
        c.dimensions,
        if candidate { closure.len() } else { 159 },
        &walks,
        &prepared,
        |row| guarded_record(row, 200, &source_data),
        c.limits.sq8_gets,
        c.limits.sq8_bytes,
        Some(&mut trace),
    )?;
    require(
        plan.ranges.len() <= c.limits.sq8_gets
            && plan.planned_bytes <= c.limits.sq8_bytes
            && source_cover.len() + plan.ranges.len() <= c.limits.combined_gets
            && source_bytes + plan.planned_bytes <= c.limits.combined_bytes
            && trace
                .nomination_evaluated_units
                .iter()
                .all(|u| closure.contains(&(u / 8))),
        "combined/scored-closure budgets",
    )?;
    let source_selection_timing = timer.finish();
    let planned = plan_value(q, &plan, &trace);
    events.emit(&json!({"phase":"source_plan_frozen","arm":arm,"plan":planned,"source_ranges":source_cover.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>() }))?;
    if !candidate {
        *stage = format!("query {q} control: historical discovery/source-plan parity");
        parity(
            &planned,
            &context.historical_plans[q],
            &[
                "query_ordinal",
                "ranges",
                "planned_bytes",
                "ranked_candidate_pages",
                "selected_pages",
                "primary_page",
                "nomination_evaluated_units",
                "discoveries",
            ],
        )?;
    }
    drop(source_data);
    *stage = format!("query {q} {arm}: SQ8 admitted range read/ranking");
    let timer = Timer::start();
    let sq8_data = read_ranges(&context.sq8, &plan.ranges)?;
    let sq8_read_timing = timer.finish();
    let timer = Timer::start();
    let ranges = sq8_data
        .iter()
        .map(|(r, body)| ReturnedRange {
            start: r.start,
            bytes: body,
        })
        .collect::<Vec<_>>();
    let scores = rank_returned_ranges(
        Sq8Geometry {
            rows: c.rows,
            dimensions: c.dimensions,
        },
        &ranges,
        &query,
        &context.root.low,
        &context.root.step,
        100,
        c.limits.sq8_bytes,
    )
    .map_err(|error| format!("admitted SQ8 ranking: {error:?}"))?;
    let ids = scores.iter().map(|score| score.id).collect::<Vec<_>>();
    let sq8_rank_timing = timer.finish();
    let mut returned = json!({"query_ordinal":q,"ids":ids,"ranges":planned["ranges"],"planned_bytes":plan.planned_bytes,
        "submitted_gets":plan.ranges.len(),"verified_bytes":plan.planned_bytes});
    events.emit(&json!({"phase":"returned_ids_frozen","arm":arm,"returned":returned}))?;
    if !candidate {
        *stage = format!("query {q} control: historical ordered top100 parity");
        parity(
            &returned,
            &context.historical_ids[q],
            &[
                "query_ordinal",
                "ids",
                "ranges",
                "planned_bytes",
                "submitted_gets",
                "verified_bytes",
            ],
        )?;
    }
    returned["control_parity_passed"] = json!(!candidate);
    let total_timing = total.finish();
    let source_rows = source_bytes / 200;
    let closure_rows = closure
        .iter()
        .map(|p| (c.rows - p * 256).min(256))
        .sum::<usize>();
    let walked_units = walks
        .iter()
        .flat_map(|(_, u)| u)
        .copied()
        .collect::<BTreeSet<_>>();
    let report = json!({"query_ordinal":q,"arm":arm,"success":true,"source_plan":planned,"returned":returned,
        "discovery_units":units,"page_closure":closure,"walk_units":walked_units,"seed_added_units":seed_additions,
        "page_completion_units":trace.nomination_evaluated_units.iter().filter(|u| !walked_units.contains(u)).collect::<Vec<_>>(),
        "selected_leaf_ids":selected_leaves,"selected_leaf_distinct_pages":selected_leaves.iter().map(|&id|context.leaf_pages[id]).collect::<Vec<_>>(),
        "modeled_io":{"source_gets":source_cover.len(),"source_bytes":source_bytes,"source_parallel_limit":c.limits.source_parallel,
            "source_rows":source_rows,"source_bridge_rows":source_rows-closure_rows,
            "sq8_gets":plan.ranges.len(),"sq8_bytes":plan.planned_bytes,"combined_gets":source_cover.len()+plan.ranges.len(),
            "combined_bytes":source_bytes+plan.planned_bytes,"router_root_gets":usize::from(candidate),
            "router_root_bytes":if candidate {c.inputs.router_manifest.bytes} else {0},"router_leaf_gets":selected_leaves.len(),"router_leaf_bytes":leaf_bytes,
            "total_query_gets":source_cover.len()+plan.ranges.len()+usize::from(candidate)+selected_leaves.len(),
            "total_query_bytes":source_bytes+plan.planned_bytes+if candidate {c.inputs.router_manifest.bytes+leaf_bytes} else {0}},
        "offline_timings":{"prepare":prepare_timing,"discovery_router_read":discovery_timing,"source_admission_read":source_timing,
            "source_selection_sq8_admission":source_selection_timing,"sq8_read":sq8_read_timing,"sq8_rank":sq8_rank_timing,"total":total_timing,
            "total_includes":"synchronous source-plan/returned-ID evidence writes and control parity; arm-frozen evidence write excluded; use per-stage timings for CPU conclusions"},
        "physical_s3_measured":false,"serving_latency_claim":false});
    events.emit(&json!({"phase":"arm_frozen","report":report}))?;
    Ok(Arm {
        units,
        walk_units: walked_units,
        scored_units: trace.nomination_evaluated_units.iter().copied().collect(),
        closure,
        source_cover,
        plan,
        ids,
        report,
    })
}

fn hits(ids: &[i64], truth: &[i64], k: usize) -> usize {
    ids.iter()
        .take(k)
        .filter(|id| truth[..k].contains(id))
        .count()
}
fn coverage(gold: &[i64], mut contains: impl FnMut(i64) -> bool) -> Value {
    let hit_ids = gold
        .iter()
        .copied()
        .filter(|&id| contains(id))
        .collect::<Vec<_>>();
    let hit10 = gold[..10].iter().filter(|id| hit_ids.contains(id)).count();
    json!({"hits10":hit10,"hits100":hit_ids.len(),"r10":hit10 as f64/10.,"r100":hit_ids.len() as f64/100.,"truth100_hit_ids":hit_ids})
}
fn in_ranges(physical: usize, row_bytes: usize, ranges: &[Range<usize>]) -> bool {
    let offset = physical * row_bytes;
    ranges.iter().any(|r| r.contains(&offset))
}
fn arm_metrics(arm: &Arm, gold: &[i64], reference: &[i64], inverse: &[usize]) -> Value {
    let physical = |id: i64| inverse[id as usize];
    let discovery = coverage(gold, |id| arm.units.contains(&(physical(id) / 32)));
    let seed = coverage(gold, |id| arm.walk_units.contains(&(physical(id) / 32)));
    let closure = coverage(gold, |id| arm.closure.contains(&(physical(id) / 256)));
    let source_fetched = coverage(gold, |id| in_ranges(physical(id), 200, &arm.source_cover));
    let source_scored = coverage(gold, |id| arm.scored_units.contains(&(physical(id) / 32)));
    let selected = coverage(gold, |id| {
        arm.plan.selected_pages.contains(&(physical(id) / 256))
    });
    let fetched = coverage(gold, |id| in_ranges(physical(id), 780, &arm.plan.ranges));
    let returned = json!({"hits10":hits(&arm.ids,gold,10),"hits100":hits(&arm.ids,gold,100),
        "r10":hits(&arm.ids,gold,10) as f64/10.,"r100":hits(&arm.ids,gold,100) as f64/100.});
    let loss = |a: &Value, b: &Value| {
        json!({
        "hits10_delta":a["hits10"].as_i64().unwrap()-b["hits10"].as_i64().unwrap(),
        "hits100_delta":a["hits100"].as_i64().unwrap()-b["hits100"].as_i64().unwrap()})
    };
    let source_bridge = gold
        .iter()
        .copied()
        .filter(|&id| {
            in_ranges(physical(id), 200, &arm.source_cover)
                && !arm.closure.contains(&(physical(id) / 256))
        })
        .collect::<Vec<_>>();
    let sq8_bridge = gold
        .iter()
        .copied()
        .filter(|&id| {
            in_ranges(physical(id), 780, &arm.plan.ranges)
                && !arm.plan.selected_pages.contains(&(physical(id) / 256))
        })
        .collect::<Vec<_>>();
    let reference_selected = coverage(reference, |id| {
        arm.plan.selected_pages.contains(&(physical(id) / 256))
    });
    let reference_fetched = coverage(reference, |id| {
        in_ranges(physical(id), 780, &arm.plan.ranges)
    });
    json!({"stages":{"discovery_units":discovery,"seed_augmented_units":seed,"page_closure":closure,
        "source_fetched":source_fetched,"source_scored_units":source_scored,"source_selected_pages":selected,
        "sq8_fetched":fetched,"returned":returned},
        "losses":{"discovery":json!({"hits10":10-discovery["hits10"].as_u64().unwrap(),"hits100":100-discovery["hits100"].as_u64().unwrap()}),
            "closure_gain":loss(&closure,&discovery),"source_selection":loss(&closure,&selected),
            "fetched_gain":loss(&fetched,&selected),"returned_ranking":loss(&fetched,&returned)},
        "bridge_coverage_gains":{"source_truth100_ids":source_bridge,"sq8_truth100_ids":sq8_bridge,
            "source_truth10_hits":source_bridge.iter().filter(|id|gold[..10].contains(id)).count(),
            "sq8_truth10_hits":sq8_bridge.iter().filter(|id|gold[..10].contains(id)).count()},
        "selection_against_exhaustive_native_sq8":{"selected":reference_selected,"fetched":reference_fetched,
            "returned_r10":hits(&arm.ids,reference,10) as f64/10.,"returned_r100":hits(&arm.ids,reference,100) as f64/100.}})
}
fn distribution(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    // Nearest-rank tails, matching the frozen coverage evaluator.
    let percentile = |p: f64| values[((values.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    json!({"mean":mean,"min":values[0],"p05":percentile(0.05),"p50":percentile(0.5),"p95":percentile(0.95),"max":values[values.len()-1]})
}
fn evaluate(
    c: &Config,
    context: &Context,
    pairs: &[[Arm; 2]],
    reference_ids: &[Vec<i64>],
    events: &mut Events,
) -> Result<Value> {
    // Truth is opened for the FIRST time here, after every plan/ID and reference is frozen.
    let body = checked(&c.inputs.truth, 400000)?;
    let mut gold = Vec::with_capacity(64);
    for (q, row) in body.chunks_exact(400).enumerate() {
        let ids = row
            .chunks_exact(4)
            .map(|w| i64::from(u32::from_le_bytes(w.try_into().unwrap())))
            .collect::<Vec<_>>();
        require(
            ids.iter().all(|&id| id < c.rows as i64)
                && ids.iter().collect::<BTreeSet<_>>().len() == 100,
            "truth logical IDs",
        )?;
        if q < 64 {
            gold.push(ids);
        }
    }
    let mut metrics: [Vec<Value>; 2] = [Vec::new(), Vec::new()];
    let mut reference_metrics = Vec::new();
    for q in 0..64 {
        for arm in 0..2 {
            let m = arm_metrics(
                &pairs[q][arm],
                &gold[q],
                &reference_ids[q],
                &context.inverse,
            );
            events.emit(
                &json!({"phase":"evaluation","query_ordinal":q,"truth_ordinal":q,
                "arm":if arm == 0 {"control"} else {"candidate"},"metrics":m}),
            )?;
            metrics[arm].push(m);
        }
        let r = json!({"query_ordinal":q,"truth_ordinal":q,"hits10":hits(&reference_ids[q],&gold[q],10),
            "hits100":hits(&reference_ids[q],&gold[q],100),"r10":hits(&reference_ids[q],&gold[q],10) as f64/10.,
            "r100":hits(&reference_ids[q],&gold[q],100) as f64/100.});
        events.emit(&json!({"phase":"exhaustive_sq8_quality","metrics":r}))?;
        reference_metrics.push(r);
    }
    let summary = |arm: usize| {
        let total10 = metrics[arm]
            .iter()
            .map(|m| m["stages"]["returned"]["hits10"].as_u64().unwrap())
            .sum::<u64>();
        let total100 = metrics[arm]
            .iter()
            .map(|m| m["stages"]["returned"]["hits100"].as_u64().unwrap())
            .sum::<u64>();
        let mut stages = serde_json::Map::new();
        for stage in [
            "discovery_units",
            "seed_augmented_units",
            "page_closure",
            "source_fetched",
            "source_scored_units",
            "source_selected_pages",
            "sq8_fetched",
            "returned",
        ] {
            stages.insert(stage.into(),json!({"r10":distribution(metrics[arm].iter().map(|m|m["stages"][stage]["r10"].as_f64().unwrap()).collect()),
                "r100":distribution(metrics[arm].iter().map(|m|m["stages"][stage]["r100"].as_f64().unwrap()).collect())}));
        }
        let mut io = serde_json::Map::new();
        for field in [
            "source_gets",
            "source_bytes",
            "source_rows",
            "source_bridge_rows",
            "sq8_gets",
            "sq8_bytes",
            "combined_gets",
            "combined_bytes",
            "router_root_gets",
            "router_root_bytes",
            "router_leaf_gets",
            "router_leaf_bytes",
            "total_query_gets",
            "total_query_bytes",
        ] {
            io.insert(
                field.into(),
                distribution(
                    pairs
                        .iter()
                        .map(|p| p[arm].report["modeled_io"][field].as_f64().unwrap())
                        .collect(),
                ),
            );
        }
        json!({"successful_queries":64,"total_returned_r10_hits":total10,"total_returned_r100_hits":total100,
            "mean_r10":total10 as f64/640.,"mean_r100":total100 as f64/6400.,"stage_quality":stages,"modeled_io":io,
            "offline_total_process_cpu_ns":distribution(pairs.iter().map(|p|p[arm].report["offline_timings"]["total"]["process_cpu_ns"].as_f64().unwrap()).collect()),
            "offline_total_wall_ns":distribution(pairs.iter().map(|p|p[arm].report["offline_timings"]["total"]["wall_ns"].as_f64().unwrap()).collect())})
    };
    let candidate = summary(1);
    let passed = candidate["total_returned_r10_hits"].as_u64().unwrap() >= 608;
    Ok(
        json!({"status":if passed {"PASS"} else {"FAIL"},"failure_stage":if passed {Value::Null} else {json!("candidate returned_quality")},
        "gate":{"required_integer_hits":608,"denominator":640,"all_64_paired_queries_successful":true,"historical_control_parity":true,
            "all_identity_and_budget_gates_passed":true,"candidate_returned_r10_passed":passed},
        "control":summary(0),"candidate":candidate,
        "exhaustive_native_sq8":{"r10":distribution(reference_metrics.iter().map(|m|m["r10"].as_f64().unwrap()).collect()),
            "r100":distribution(reference_metrics.iter().map(|m|m["r100"].as_f64().unwrap()).collect()),
            "reference_bytes_per_query":78000000,"total_reference_bytes":78000000_u64*64,
            "purpose":"quantization/native-kernel rounding loss against exact truth; reference IDs never enter nomination"},
        "scope":"consumed development panel; modeled offline IO and CPU only; no physical S3/cold HTTP/serving/QPS claim"}),
    )
}
fn run(
    config_path: &Path,
    config_sha: &str,
    events: &mut Events,
    stage: &mut String,
) -> Result<Value> {
    *stage = "authenticate strict config".into();
    let metadata = std::fs::symlink_metadata(config_path)?;
    require(
        metadata.is_file() && metadata.len() <= 65536,
        "config regular-file/cap",
    )?;
    let a = Artifact {
        path: config_path.into(),
        bytes: usize::try_from(metadata.len())?,
        sha256: config_sha.into(),
    };
    let c: Config = serde_json::from_slice(&checked(&a, 65536)?)?;
    validate_config(&c)?;
    events.emit(&json!({"phase":"configuration","config":c,"query_space":"original f32 prepare_query; cosine_vector for discovery and SQ8; ordered f64 root squared distances over f32 prototypes"}))?;
    let context = load(&c, stage)?;
    events.emit(&json!({"phase":"authenticated_startup","truth_opened":false,
        "authenticated_object_bytes_excluding_truth":c.inputs.all().iter().filter(|(name,_,_)|*name!="truth").map(|(_,a,_)|a.bytes).sum::<usize>(),
        "preloads":{"membership_bytes":c.inputs.router_membership.bytes,"order_bytes":c.inputs.order.bytes,"inverse_mapping_payload_bytes":context.inverse.len()*std::mem::size_of::<usize>(),
            "control_centroid_object_bytes":c.inputs.centroids.bytes,"control_centroid_resident_array_bytes":context.centroids.resident_array_bytes(),
            "control_graph_object_bytes":c.inputs.graph.bytes+c.inputs.diverse_graph.bytes,"requests_bytes":c.inputs.requests.bytes,
            "router_root_bytes":c.inputs.router_manifest.bytes},"full_object_authentication_is_validation_only":true}))?;
    let mut pairs = Vec::with_capacity(64);
    for q in 0..64 {
        let (control, candidate) = if q % 2 == 0 {
            let control = run_arm(&c, &context, q, false, events, stage)?;
            (control, run_arm(&c, &context, q, true, events, stage)?)
        } else {
            let candidate = run_arm(&c, &context, q, true, events, stage)?;
            (run_arm(&c, &context, q, false, events, stage)?, candidate)
        };
        pairs.push([control, candidate]);
    }
    events.emit(&json!({"phase":"all_paired_plans_and_returned_ids_frozen","count":64,"truth_opened":false}))?;
    let mut reference_ids = Vec::with_capacity(64);
    let reference_total = Timer::start();
    for q in 0..64 {
        *stage = format!("query {q}: separate exhaustive native SQ8 reference");
        let timer = Timer::start();
        let body = read_at(&context.sq8, 0..c.inputs.sq8.bytes)?;
        let read_timing = timer.finish();
        let timer = Timer::start();
        let query = normalize_two_bit_diagnostic_query(&context.requests[q])?;
        let scores = rank_returned_ranges(
            Sq8Geometry {
                rows: c.rows,
                dimensions: c.dimensions,
            },
            &[ReturnedRange {
                start: 0,
                bytes: &body,
            }],
            &query,
            &context.root.low,
            &context.root.step,
            100,
            body.len(),
        )
        .map_err(|error| format!("exhaustive SQ8 reference: {error:?}"))?;
        let ids = scores.iter().map(|s| s.id).collect::<Vec<_>>();
        events.emit(&json!({"phase":"exhaustive_sq8_reference_frozen","query_ordinal":q,"ids":ids,"bytes":body.len(),
            "offline_read_timing":read_timing,"offline_rank_timing":timer.finish(),"evaluator_work_excluded_from_serving":true,"truth_opened":false}))?;
        reference_ids.push(ids);
    }
    let reference_timing = reference_total.finish();
    *stage = "truth authentication/ordinal binding and evaluation".into();
    let mut summary = evaluate(&c, &context, &pairs, &reference_ids, events)?;
    summary["exhaustive_native_sq8"]["offline_total_timing"] = reference_timing;
    Ok(summary)
}
fn execute() -> Result<bool> {
    let args = std::env::args().collect::<Vec<_>>();
    require(
        args.len() == 4,
        "usage: check_semantic_router_scorer CONFIG CONFIG_SHA OUTPUT",
    )?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[3])?;
    let mut events = Events { file, first: true };
    let executable = std::env::current_exe()?;
    let binary = Artifact {
        path: executable.clone(),
        bytes: usize::try_from(executable.metadata()?.len())?,
        sha256: String::new(),
    };
    // Hash the exact running executable through its admitted descriptor.
    let mut digest = Sha256::new();
    let mut reader = BufReader::new(open_regular(&binary.path, binary.bytes, binary.bytes)?);
    let mut buffer = [0_u8; 65536];
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    let header = json!({"schema":"borsuk-semantic-router-scorer-result-v1","command":args,
        "config_sha256":args[2],"source_sha256":hash(include_bytes!("check_semantic_router_scorer.rs")),
        "binary_path":executable,"binary_sha256":format!("{:x}",digest.finalize()),
        "physical_s3_measured":false,"serving_claim":false});
    // Append-only valid JSON on terminal success/failure; partial bytes survive external termination.
    let header = serde_json::to_string(&header)?;
    events
        .file
        .write_all(header.trim_end_matches('}').as_bytes())?;
    events.file.write_all(b",\"events\":[\n")?;
    let mut stage = "startup".to_owned();
    let timer = Timer::start();
    let result = run(Path::new(&args[1]), &args[2], &mut events, &mut stage);
    let mut summary = match result {
        Ok(summary) => summary,
        Err(error) => {
            json!({"status":"FAIL","failure_stage":stage,"error":error.to_string(),"preserved":true,"retry":false})
        }
    };
    summary["whole_evaluator_offline_timing"] = timer.finish();
    summary["whole_evaluator_timing_includes"] = json!(
        "authentication, evaluator work, synchronous evidence writes and control parity; use per-stage timings for CPU conclusions"
    );
    let passed = summary["status"] == "PASS";
    events.file.write_all(b"\n],\"summary\":")?;
    serde_json::to_writer(&mut events.file, &summary)?;
    events.file.write_all(b"}\n")?;
    events.file.sync_all()?;
    println!(
        "{}",
        json!({"status":summary["status"],"failure_stage":summary["failure_stage"],"output":args[3]})
    );
    Ok(passed)
}
fn main() {
    match execute() {
        Ok(true) => (),
        Ok(false) => std::process::exit(2),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roots_keep_distance_ties_and_zero_boundary_without_truncating_units() {
        let prototypes = vec![vec![0.]; 20];
        assert_eq!(
            select_leaves(&[1.], &prototypes).unwrap(),
            (0..16).collect::<Vec<_>>()
        );
        let mut prototypes = vec![vec![0.]; 8];
        prototypes.extend([vec![0.], vec![1.]]);
        assert_eq!(
            select_leaves(&[0.], &prototypes).unwrap(),
            (0..9).collect::<Vec<_>>()
        );
        assert!(select_leaves(&[f32::NAN], &prototypes).is_err());
        let mut prototypes = vec![vec![1.]; 8];
        prototypes.extend([vec![1.06], vec![1.08]]);
        assert_eq!(
            select_leaves(&[0.], &prototypes).unwrap(),
            (0..9).collect::<Vec<_>>()
        );
        let all_units = (0..1024).collect();
        assert_eq!(seed_walk(&all_units, 32768).unwrap().1.len(), 1024);
        let units = [10, 17].into_iter().collect();
        let (seed, walk, additions) = seed_walk(&units, 600).unwrap();
        assert_eq!(seed, 1);
        assert_eq!(walk, vec![8, 9, 10, 11, 12, 13, 14, 15, 17]);
        assert_eq!(additions, vec![8, 9, 11, 12, 13, 14, 15]);
        assert!(walk.iter().all(|u| [1, 2].contains(&(u / 8))));
        assert!(seed_walk(&BTreeSet::new(), 600).is_err());
    }

    #[test]
    fn callback_rejects_uncharged_rows_and_mapping_rejects_aliases() {
        let ranges = vec![(4..8, vec![1, 2, 3, 4])];
        assert_eq!(guarded_record(2, 2, &ranges), Some(&[1, 2][..]));
        assert_eq!(guarded_record(3, 2, &ranges), Some(&[3, 4][..]));
        assert_eq!(guarded_record(1, 2, &ranges), None);
        assert_eq!(guarded_record(4, 2, &ranges), None);
        assert_eq!(inverse_order(&[2, 0, 1]).unwrap(), vec![1, 2, 0]);
        assert!(inverse_order(&[0, 0]).is_err());
        assert!(inverse_order(&[0, 2]).is_err());
        let reverse = (0..64).rev().collect::<Vec<u64>>();
        assert_eq!(inverse_order(&reverse).unwrap()[0] / 32, 1);
    }

    #[test]
    fn parity_requires_present_fields_and_ordered_ids() {
        let expected = json!({"ids":[7,3],"ranges":[[0,26]]});
        assert!(parity(&expected, &expected, &["ids", "ranges"]).is_ok());
        assert!(parity(&json!({"ids":[3,7],"ranges":[[0,26]]}), &expected, &["ids"]).is_err());
        assert!(parity(&expected, &expected, &["missing"]).is_err());
    }

    #[test]
    fn ignored_nomination_values_including_null_do_not_change_request_shape() {
        for (nominees, count) in [
            (json!([17]), json!(1)),
            (Value::Null, json!(100)),
            (json!({"changed":"ignored"}), Value::Null),
            (Value::Null, Value::Null),
        ] {
            let request: Request =
                serde_json::from_value(json!({"query_ordinal":0,"query":[1.,0.],
                "nominees":nominees,"primary_count":count}))
                .unwrap();
            assert!(request.nominees.is_some() && request.primary_count.is_some());
            assert_eq!(request.query, vec![1., 0.]);
        }
        let request: Request =
            serde_json::from_value(json!({"query_ordinal":0,"query":[1.,0.]})).unwrap();
        assert!(request.nominees.is_none() && request.primary_count.is_none());
    }

    #[test]
    fn strict_config_and_descriptor_checks_reject_unbounded_inputs() {
        let body = include_bytes!(
            "../../../../docs/research/performance-architecture-20260930/semantic-router-scorer-relaion-config.json"
        );
        let config: Config = serde_json::from_slice(body).unwrap();
        validate_config(&config).unwrap();
        assert_eq!(config.inputs.all().len(), 17);
        let mut value: Value = serde_json::from_slice(body).unwrap();
        value["inputs"]["extra"] = json!({});
        assert!(serde_json::from_value::<Config>(value).is_err());
        let mut value: Value = serde_json::from_slice(body).unwrap();
        value["limits"]["sq8_bytes"] = json!(16773121);
        assert!(validate_config(&serde_json::from_value::<Config>(value).unwrap()).is_err());
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("object");
        std::fs::write(&path, b"abc").unwrap();
        let mut artifact = Artifact {
            path: path.clone(),
            bytes: 3,
            sha256: hash(b"abc"),
        };
        assert_eq!(checked(&artifact, 3).unwrap(), b"abc");
        assert!(checked(&artifact, 2).is_err());
        artifact.bytes = 2;
        assert!(checked(&artifact, 3).is_err());
        artifact.bytes = 3;
        artifact.sha256 = "0".repeat(64);
        assert!(checked(&artifact, 3).is_err());
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(open_regular(&link, 3, 3).is_err());
        let fifo = directory.path().join("fifo");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        assert!(open_regular(&fifo, 0, 3).is_err());
    }

    #[test]
    fn shared_scorer_cannot_score_rows_outside_charged_clipped_cover() {
        let codec = RotatedTwoBitCodec::new(&[0., 0.], 20260923).unwrap();
        let prepared = codec.prepare_query(&[1., 0.], 400000).unwrap();
        let record = codec.encode(&[1., 0.]).unwrap();
        let closure = [0, 1].into_iter().collect();
        let (cover, bytes) =
            plan_two_bit_source_cover(&closure, 257, record.len(), 1, 257 * record.len()).unwrap();
        assert_eq!(bytes, 257 * record.len());
        assert_eq!(cover, vec![0..257 * record.len()]);
        let walks = [(0, (0..9).collect())];
        let data = vec![(0..256 * record.len(), record.repeat(256))];
        assert!(
            plan_two_bit_source_walks(
                257,
                2,
                2,
                &walks,
                &prepared,
                |r| guarded_record(r, record.len(), &data),
                2,
                257 * 14,
                None
            )
            .is_err()
        );
        let data = vec![(cover[0].clone(), record.repeat(257))];
        let mut trace = TwoBitPlanTrace::default();
        let plan = plan_two_bit_source_walks(
            257,
            2,
            2,
            &walks,
            &prepared,
            |r| guarded_record(r, record.len(), &data),
            2,
            257 * 14,
            Some(&mut trace),
        )
        .unwrap();
        assert_eq!(plan.planned_bytes, 257 * 14);
        assert_eq!(trace.ranked_candidate_pages, vec![0, 1]);
    }
}
