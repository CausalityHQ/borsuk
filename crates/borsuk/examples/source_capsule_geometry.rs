//! Counterfactual SOURCE page-capsule geometry from pinned router metadata only.
use borsuk::{
    rotated_two_bit::RotatedTwoBitCodec,
    semantic_unit_router::{SemanticProfile, SemanticUnitRouter, SourceIdentity},
    two_bit_source::SourcePlaneReceipt,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    error::Error,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const JSON_CAP: usize = 64 * 1024;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FilePin {
    path: PathBuf,
    bytes: usize,
    sha256: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    generation_manifest: FilePin,
    plane_manifest: FilePin,
    router_root: FilePin,
    membership: FilePin,
}

// Generation descriptor types are private; mirror their exact v9 shape without opening payloads.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Generation {
    schema: String,
    generation: u64,
    #[serde(rename = "base_epoch")]
    _base_epoch: u64,
    plane_manifest_sha256: String,
    page_manifest_sha256: String,
    discovery: Discovery,
    sq8_object_sha256: String,
    sq8_object_key: String,
    sq8_etag: String,
    canonical: Canonical,
    low: Vec<f32>,
    step: Vec<f32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Canonical {
    rows: usize,
    dimensions: usize,
    bytes: u64,
    sha256: String,
    object_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Discovery {
    mode: String,
    profile: SemanticProfile,
    root_sha256: String,
    root_bytes: usize,
    membership_sha256: String,
    membership_bytes: usize,
    leaves_sha256: String,
    leaves_bytes: usize,
    input_schema: String,
    input_root_sha256: String,
    centroids_sha256: String,
    source_sha256: String,
    source_order_sha256: String,
    mean_sha256: String,
    records_sha256: String,
    sq8_sha256: String,
}

fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn object_key(key: &str, sha: &str) -> bool {
    let parts: Vec<_> = key.split('/').collect();
    parts.len() >= 2
        && parts[parts.len() - 2] == "objects"
        && parts[parts.len() - 1] == sha
        && parts.iter().all(|part| {
            !part.is_empty()
                && *part != "."
                && *part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        })
}

fn read_capped(path: &Path, cap: usize) -> Result<Vec<u8>> {
    require(
        std::fs::symlink_metadata(path)?.file_type().is_file(),
        "regular metadata file required",
    )?;
    let file = File::open(path)?;
    let size = file.metadata()?.len();
    require(size > 0 && size <= u64::try_from(cap)?, "metadata byte cap")?;
    let mut body = Vec::with_capacity(usize::try_from(size)?);
    let limit = u64::try_from(cap)?
        .checked_add(1)
        .ok_or("metadata cap overflow")?;
    file.take(limit).read_to_end(&mut body)?;
    require(
        body.len() == usize::try_from(size)?,
        "metadata length changed",
    )?;
    Ok(body)
}

fn read_pin(pin: &FilePin, cap: usize) -> Result<Vec<u8>> {
    require(
        pin.bytes > 0 && pin.bytes <= cap && valid_sha(&pin.sha256),
        "metadata pin",
    )?;
    let body = read_capped(&pin.path, pin.bytes)?;
    require(
        body.len() == pin.bytes && hash(&body) == pin.sha256,
        "metadata length or SHA256",
    )?;
    Ok(body)
}

fn encoded_record_bytes(dimensions: usize) -> Result<usize> {
    require((1..=1024).contains(&dimensions), "source dimensions")?;
    // Width depends only on dimensions; no stored source mean or record is loaded.
    Ok(RotatedTwoBitCodec::new(&vec![0.0; dimensions], 20260923)?.record_bytes())
}

fn open_metadata(c: &Config) -> Result<(SemanticUnitRouter, SourcePlaneReceipt)> {
    let g: Generation = serde_json::from_slice(&read_pin(&c.generation_manifest, JSON_CAP)?)?;
    require(
        g.schema == "borsuk-two-bit-generation-v9"
            && g.generation > 0
            && g.plane_manifest_sha256 == c.plane_manifest.sha256,
        "generation identity",
    )?;
    let p: SourcePlaneReceipt = serde_json::from_slice(&read_pin(&c.plane_manifest, JSON_CAP)?)?;
    let d = &g.discovery;
    require(
        d.mode == "semantic"
            && d.profile.valid_geometry(p.rows, p.dimensions)
            && p.schema == "borsuk-two-bit-plane-v3"
            && p.seed == 20260923
            && p.page_rows == 32
            && !p.query_or_truth_used
            && p.record_bytes == encoded_record_bytes(p.dimensions)?
            && g.canonical.rows == p.rows
            && g.canonical.dimensions == p.dimensions,
        "source profile/plane/canonical geometry",
    )?;
    let canonical_bytes = u64::try_from(p.dimensions)?
        .checked_mul(4)
        .and_then(|n| n.checked_add(8))
        .and_then(|n| n.checked_mul(p.rows as u64))
        .ok_or("canonical bytes overflow")?;
    require(
        g.canonical.bytes == canonical_bytes
            && valid_sha(&g.canonical.sha256)
            && object_key(&g.canonical.object_key, &g.canonical.sha256)
            && g.sq8_object_sha256 == p.sq8_sha256
            && object_key(&g.sq8_object_key, &g.sq8_object_sha256)
            && !g.sq8_etag.is_empty()
            && !g.sq8_etag.starts_with("W/")
            && !g.sq8_etag.chars().any(char::is_control)
            && valid_sha(&g.page_manifest_sha256)
            && valid_sha(&p.page_digest_sha256)
            && g.low.len() == p.dimensions
            && g.step.len() == p.dimensions
            && g.low.iter().all(|n| n.is_finite())
            && g.step.iter().all(|n| n.is_finite() && *n > 0.0),
        "generation descriptors/calibration",
    )?;
    for (source, bound) in [
        (&p.source_sha256, &d.source_sha256),
        (&p.sq8_sha256, &d.sq8_sha256),
        (&p.source_order_sha256, &d.source_order_sha256),
        (&p.mean_sha256, &d.mean_sha256),
        (&p.records_sha256, &d.records_sha256),
    ] {
        require(
            valid_sha(source) && source == bound,
            "discovery/plane source binding",
        )?;
    }
    let units = p.rows.div_ceil(32);
    let membership_bytes = units.checked_mul(4).ok_or("membership bytes overflow")?;
    let leaf_bytes = p
        .dimensions
        .checked_mul(2)
        .and_then(|n| n.checked_add(4))
        .and_then(|n| n.checked_mul(units))
        .ok_or("leaf bytes overflow")?;
    let directory_stride = 64 + 4 * p.dimensions;
    require(
        (512..=d.profile.root_cap()).contains(&d.root_bytes)
            && (d.root_bytes - 512) % directory_stride == 0
            && (1..=2 * units.div_ceil(64) - 1)
                .contains(&((d.root_bytes - 512) / directory_stride))
            && d.root_sha256 == c.router_root.sha256
            && d.root_bytes == c.router_root.bytes
            && d.membership_sha256 == c.membership.sha256
            && d.membership_bytes == c.membership.bytes
            && d.membership_bytes == membership_bytes
            && d.leaves_bytes == leaf_bytes
            && valid_sha(&d.leaves_sha256)
            && valid_sha(&d.input_root_sha256)
            && valid_sha(&d.centroids_sha256)
            && (1..=256).contains(&d.input_schema.len()),
        "semantic discovery descriptors/bounds",
    )?;
    let input = SourceIdentity {
        profile: d.profile,
        schema: &d.input_schema,
        root_sha256: &d.input_root_sha256,
        centroids_sha256: &d.centroids_sha256,
        rows: p.rows,
        dimensions: p.dimensions,
    };
    let root = read_pin(&c.router_root, d.profile.root_cap())?;
    let membership = read_pin(&c.membership, membership_bytes)?;
    let router = SemanticUnitRouter::open(
        &root,
        &membership,
        &c.router_root.sha256,
        &input,
        d.profile.root_cap(),
    )?;
    require(
        router.manifest().leaf_payload.bytes == d.leaves_bytes
            && router.manifest().leaf_payload.sha256 == d.leaves_sha256,
        "router leaf descriptor binding",
    )?;
    Ok((router, p))
}

fn plus(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| "geometry addition overflow".into())
}

fn times(left: u64, right: u64) -> Result<u64> {
    left.checked_mul(right)
        .ok_or_else(|| "geometry multiplication overflow".into())
}

fn size_quantiles(values: &[u64]) -> Value {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = |percent: usize| sorted[(sorted.len() * percent).div_ceil(100).max(1) - 1];
    json!({"min":sorted[0], "p50":rank(50), "p90":rank(90),
        "p95":rank(95), "p99":rank(99), "max":sorted[sorted.len() - 1]})
}

fn geometry(
    rows: usize,
    record_bytes: usize,
    membership: &[usize],
    leaf_count: usize,
    largest: usize,
) -> Result<Value> {
    let units = rows.div_ceil(32);
    require(
        rows > 0
            && record_bytes > 0
            && membership.len() == units
            && leaf_count > 0
            && leaf_count <= units
            && (1..=leaf_count).contains(&largest),
        "geometry/membership/largest-capsules cap",
    )?;
    let baseline = times(rows as u64, record_bytes as u64)?;
    let page_count = rows.div_ceil(256);
    let mut closures = vec![Vec::new(); leaf_count];
    for (unit, &leaf) in membership.iter().enumerate() {
        require(leaf < leaf_count, "membership leaf out of range")?;
        let page = unit / 8;
        u32::try_from(page)?;
        // Units arrive in physical order, so adjacent equality removes all page duplicates.
        if closures[leaf].last() != Some(&page) {
            closures[leaf].push(page);
        }
    }
    require(
        closures.iter().all(|pages| !pages.is_empty()),
        "empty semantic leaf",
    )?;
    let mut multiplicities = vec![0_usize; page_count];
    let mut capsules = Vec::with_capacity(leaf_count);
    let mut payload_sizes = Vec::with_capacity(leaf_count);
    let mut header_sizes = Vec::with_capacity(leaf_count);
    let mut total_sizes = Vec::with_capacity(leaf_count);
    let mut payload_total = 0;
    let mut header_total = 0;
    let mut page_incidences = 0;
    for (leaf_id, pages) in closures.into_iter().enumerate() {
        let mut payload = 0;
        let mut closure_rows = 0;
        for &page in &pages {
            let actual_rows = (rows - page * 256).min(256) as u64;
            closure_rows = plus(closure_rows, actual_rows)?;
            payload = plus(payload, times(actual_rows, record_bytes as u64)?)?;
            multiplicities[page] += 1;
        }
        let header = plus(32, times(4, pages.len() as u64)?)?;
        let total = plus(payload, header)?;
        payload_total = plus(payload_total, payload)?;
        header_total = plus(header_total, header)?;
        page_incidences = plus(page_incidences, pages.len() as u64)?;
        payload_sizes.push(payload);
        header_sizes.push(header);
        total_sizes.push(total);
        capsules.push(json!({"leaf_id":leaf_id, "pages":pages,
            "closure_rows":closure_rows, "payload_record_bytes":payload,
            "header_bytes":header, "total_bytes":total}));
    }
    let mut histogram = BTreeMap::<usize, usize>::new();
    for count in multiplicities {
        require(count > 0, "uncovered source page")?;
        *histogram.entry(count).or_default() += 1;
    }
    let mut largest_sizes = payload_sizes.clone();
    largest_sizes.sort_unstable_by(|a, b| b.cmp(a));
    let largest_sum = largest_sizes[..largest]
        .iter()
        .try_fold(0, |n, &value| plus(n, value))?;
    Ok(json!({
        "rows":rows, "source_record_bytes":record_bytes, "unit_rows":32, "source_page_rows":256,
        "last_source_page_rows":rows - (page_count - 1) * 256,
        "semantic_leaves":leaf_count, "distinct_source_pages":page_count,
        "total_page_incidences":page_incidences, "page_multiplicity_histogram":histogram,
        "unreplicated_source_record_bytes":baseline, "replicated_source_record_bytes":payload_total,
        "weighted_total_replication":payload_total as f64 / baseline as f64,
        "capsule_fixed_header_bytes":32, "capsule_page_id_bytes":4,
        "replicated_header_bytes":header_total, "replicated_total_bytes":plus(payload_total, header_total)?,
        "quantile_method":"nearest rank",
        "capsule_sizes":{"payload_record_bytes":size_quantiles(&payload_sizes),
            "header_bytes":size_quantiles(&header_sizes), "total_bytes":size_quantiles(&total_sizes)},
        "largest_capsules":largest, "largest_capsules_payload_record_bytes_sum":largest_sum,
        "capsules":capsules
    }))
}

fn main() -> Result<()> {
    let usage = "usage: source_capsule_geometry CONFIG.json LARGEST_CAPSULES";
    let mut args = std::env::args_os().skip(1);
    let path = PathBuf::from(args.next().ok_or(usage)?);
    let largest: usize = args
        .next()
        .ok_or(usage)?
        .into_string()
        .map_err(|_| usage)?
        .parse()?;
    require(args.next().is_none(), usage)?;
    let c: Config = serde_json::from_slice(&read_capped(&path, JSON_CAP)?)?;
    let (router, plane) = open_metadata(&c)?;
    let report = geometry(
        plane.rows,
        plane.record_bytes,
        router.unit_membership(),
        router.manifest().leaves.len(),
        largest,
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema":"borsuk-source-capsule-geometry-v1",
            "scope":"counterfactual physical geometry only; no latency or quality measurement",
            "admission":"pinned generation/plane descriptors and native authenticated router/membership; no payload verification",
            "dimensions":plane.dimensions, "inputs":c, "geometry":report
        }))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsuk::semantic_unit_router::build;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    fn put(dir: &Path, name: &str, body: &[u8]) -> FilePin {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        FilePin {
            path,
            bytes: body.len(),
            sha256: hash(body),
        }
    }

    fn fixture(dimensions: usize) -> (TempDir, Config) {
        let dir = tempfile::tempdir().unwrap();
        let rows = 257_usize;
        let mut blob = b"BORSUCP1".to_vec();
        blob.extend_from_slice(&(rows as u64).to_le_bytes());
        for word in [dimensions as u32, 32, 256, 0] {
            blob.extend_from_slice(&word.to_le_bytes());
        }
        for _ in 0..rows.div_ceil(32) * dimensions {
            blob.extend_from_slice(&half::f16::from_f32(1.0).to_bits().to_le_bytes());
        }
        let centroid_sha = hash(&blob);
        let input_root = "1".repeat(64);
        let input = SourceIdentity {
            profile: SemanticProfile::Scale1m,
            schema: "borsuk-two-bit-plane-v3",
            root_sha256: &input_root,
            centroids_sha256: &centroid_sha,
            rows,
            dimensions,
        };
        let artifacts = build(&blob, &input, 64 * 1024 * 1024).unwrap();
        let router_root = put(dir.path(), "root.bin", &artifacts.manifest);
        let membership = put(dir.path(), "membership.bin", &artifacts.membership);
        let record_bytes = encoded_record_bytes(dimensions).unwrap();
        let plane = json!({
            "schema":"borsuk-two-bit-plane-v3", "rows":rows, "dimensions":dimensions,
            "seed":20260923, "record_bytes":record_bytes, "page_rows":32,
            "source_sha256":"2".repeat(64), "sq8_sha256":"3".repeat(64),
            "source_order_sha256":"4".repeat(64), "mean_sha256":"5".repeat(64),
            "records_sha256":"6".repeat(64), "page_digest_sha256":"7".repeat(64),
            "query_or_truth_used":false
        });
        let plane_manifest = put(
            dir.path(),
            "plane.json",
            &serde_json::to_vec(&plane).unwrap(),
        );
        let generation = json!({
            "schema":"borsuk-two-bit-generation-v9", "generation":1, "base_epoch":0,
            "plane_manifest_sha256":plane_manifest.sha256,
            "page_manifest_sha256":"8".repeat(64),
            "sq8_object_sha256":plane["sq8_sha256"],
            "sq8_object_key":format!("semantic/objects/{}", "3".repeat(64)),
            "sq8_etag":"fixture",
            "canonical":{"rows":rows, "dimensions":dimensions,
                "bytes":rows * (8 + 4 * dimensions), "sha256":"9".repeat(64),
                "object_key":format!("semantic/objects/{}", "9".repeat(64))},
            "low":vec![-1.0_f32; dimensions], "step":vec![0.25_f32; dimensions],
            "discovery":{
                "mode":"semantic", "profile":"scale1m",
                "root_sha256":router_root.sha256, "root_bytes":router_root.bytes,
                "membership_sha256":membership.sha256, "membership_bytes":membership.bytes,
                "leaves_sha256":hash(&artifacts.leaves), "leaves_bytes":artifacts.leaves.len(),
                "input_schema":input.schema, "input_root_sha256":input.root_sha256,
                "centroids_sha256":centroid_sha, "source_sha256":plane["source_sha256"],
                "source_order_sha256":plane["source_order_sha256"],
                "mean_sha256":plane["mean_sha256"], "records_sha256":plane["records_sha256"],
                "sq8_sha256":plane["sq8_sha256"]
            }
        });
        let generation_manifest = put(
            dir.path(),
            "generation.json",
            &serde_json::to_vec(&generation).unwrap(),
        );
        (
            dir,
            Config {
                generation_manifest,
                plane_manifest,
                router_root,
                membership,
            },
        )
    }

    fn edit_generation(c: &mut Config, edit: impl FnOnce(&mut Value)) {
        let mut g: Value =
            serde_json::from_slice(&std::fs::read(&c.generation_manifest.path).unwrap()).unwrap();
        edit(&mut g);
        let body = serde_json::to_vec(&g).unwrap();
        std::fs::write(&c.generation_manifest.path, &body).unwrap();
        c.generation_manifest.bytes = body.len();
        c.generation_manifest.sha256 = hash(&body);
    }

    #[test]
    fn overlapping_source_pages_use_weighted_partial_tail_and_separate_headers() {
        let r = geometry(257, 9, &[0, 1, 0, 1, 0, 1, 0, 1, 0], 2, 1).unwrap();
        assert_eq!(r["distinct_source_pages"], 2);
        assert_eq!(r["total_page_incidences"], 3);
        assert_eq!(r["page_multiplicity_histogram"], json!({"1":1, "2":1}));
        assert_eq!(r["unreplicated_source_record_bytes"], 257 * 9);
        assert_eq!(r["replicated_source_record_bytes"], 513 * 9);
        assert_eq!(r["replicated_header_bytes"], 76);
        assert_eq!(
            r["capsule_sizes"]["payload_record_bytes"],
            json!({
            "min":256 * 9, "p50":256 * 9, "p90":257 * 9,
            "p95":257 * 9, "p99":257 * 9, "max":257 * 9})
        );
        let all = geometry(257, 9, &[0, 1, 0, 1, 0, 1, 0, 1, 0], 2, 2).unwrap();
        assert_eq!(all["largest_capsules_payload_record_bytes_sum"], 513 * 9);
        assert_eq!(r["largest_capsules_payload_record_bytes_sum"], 257 * 9);
        assert_eq!(r["capsules"][0]["pages"], json!([0, 1]));
        assert_eq!(r["capsules"][0]["header_bytes"], 40);
        assert_eq!(r["capsules"][1]["pages"], json!([0]));
        assert_eq!(r["capsules"][1]["header_bytes"], 36);
        let ratio = r["weighted_total_replication"].as_f64().unwrap();
        assert!((ratio - 513.0 / 257.0).abs() < 1e-12);
    }

    #[test]
    fn one_partial_source_page_is_not_charged_as_256_rows() {
        let r = geometry(33, 73, &[0, 0], 1, 1).unwrap();
        assert_eq!(r["replicated_source_record_bytes"], 33 * 73);
        assert_eq!(r["replicated_header_bytes"], 36);
        assert_eq!(r["capsule_sizes"]["payload_record_bytes"]["max"], 33 * 73);
        assert_eq!(r["weighted_total_replication"], 1.0);
    }

    #[test]
    fn authenticated_native_metadata_supports_d1_d257_d1024() {
        for (dimensions, width) in [(1, 9), (257, 73), (1024, 264)] {
            let (_dir, c) = fixture(dimensions);
            let (router, plane) = open_metadata(&c).unwrap();
            assert_eq!(plane.record_bytes, width);
            assert_eq!(router.manifest().dimensions, dimensions);
            let r = geometry(
                plane.rows,
                plane.record_bytes,
                router.unit_membership(),
                router.manifest().leaves.len(),
                1,
            )
            .unwrap();
            assert_eq!(r["unreplicated_source_record_bytes"], 257 * width);
            assert_eq!(r["replicated_source_record_bytes"], 257 * width);
            assert_eq!(r["capsules"][0]["pages"], json!([0, 1]));
            assert_eq!(r["replicated_header_bytes"], 40);
        }
    }

    #[test]
    fn metadata_rejects_wrong_hash_unknown_fields_and_plane_width() {
        let (_dir, mut c) = fixture(1);
        let original = c.clone();
        c.generation_manifest.sha256 = "0".repeat(64);
        assert!(open_metadata(&c).is_err());
        c = original.clone();
        edit_generation(&mut c, |g| g["unexpected"] = json!(true));
        assert!(open_metadata(&c).is_err());

        let (_dir2, mut c) = fixture(257);
        let mut p: Value =
            serde_json::from_slice(&std::fs::read(&c.plane_manifest.path).unwrap()).unwrap();
        p["record_bytes"] = json!(72);
        let body = serde_json::to_vec(&p).unwrap();
        std::fs::write(&c.plane_manifest.path, &body).unwrap();
        c.plane_manifest.bytes = body.len();
        c.plane_manifest.sha256 = hash(&body);
        let sha = c.plane_manifest.sha256.clone();
        edit_generation(&mut c, |g| g["plane_manifest_sha256"] = json!(sha));
        assert!(open_metadata(&c).is_err());
    }

    #[test]
    fn native_decoder_rejects_rebound_corrupt_root_and_membership() {
        let (_dir, mut c) = fixture(1);
        let mut body = std::fs::read(&c.router_root.path).unwrap();
        body[0] ^= 1;
        std::fs::write(&c.router_root.path, &body).unwrap();
        c.router_root.sha256 = hash(&body);
        let sha = c.router_root.sha256.clone();
        edit_generation(&mut c, |g| g["discovery"]["root_sha256"] = json!(sha));
        assert!(open_metadata(&c).is_err());

        let (_dir2, mut c) = fixture(1);
        let mut body = std::fs::read(&c.membership.path).unwrap();
        body[..4].copy_from_slice(&99_u32.to_le_bytes());
        std::fs::write(&c.membership.path, &body).unwrap();
        c.membership.sha256 = hash(&body);
        let sha = c.membership.sha256.clone();
        edit_generation(&mut c, |g| g["discovery"]["membership_sha256"] = json!(sha));
        assert!(open_metadata(&c).is_err());
    }

    #[test]
    fn geometry_rejects_overflow_invalid_membership_and_caps() {
        assert!(geometry(257, usize::MAX, &[0; 9], 1, 1).is_err());
        assert!(geometry(33, 9, &[0, 1], 1, 1).is_err());
        assert!(geometry(33, 9, &[0], 1, 1).is_err());
        assert!(geometry(33, 9, &[0, 0], 2, 1).is_err());
        assert!(geometry(33, 9, &[0, 0], 1, 0).is_err());
        assert!(geometry(33, 9, &[0, 0], 1, 2).is_err());
        assert!(encoded_record_bytes(0).is_err());
        assert!(encoded_record_bytes(1025).is_err());
    }
}
