//! Query-free construction evidence adapter, never a historical serving reader.
//! Copies authenticated SOURCE/router bytes; only SQ8 proof geometry is rebuilt.
use super::{CompletedInput, HeaderSnapshot, Result, UniqueJson, fields, require};
use borsuk::{
    rotated_two_bit::RotatedTwoBitCodec, semantic_unit_router::SemanticProfile,
    sq8_page_authority::PageAuthority, two_bit_source::SourcePlaneReceipt,
};
use rustix::fs::{Mode, OFlags, openat};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::Path,
};

pub(super) const REPORT_SCHEMA: &str = "borsuk-source-preserving-v9-fixture-v1";
const BASE_PAYLOAD: usize = 2 * 1024 * 1024;
const SMALL_CAP: u64 = 65_536;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    root: CompletedInput,
    sq8: CompletedInput,
    max_payload_bytes: usize,
}

#[derive(Deserialize)]
struct Calibration {
    low: Vec<f32>,
    step: Vec<f32>,
}

fn hash(body: &[u8]) -> String {
    format!("{:x}", Sha256::digest(body))
}

fn positive(value: &Value) -> Result<usize> {
    let n = usize::try_from(value.as_u64().ok_or("fixture positive integer")?)?;
    require(n > 0, "fixture positive integer")?;
    Ok(n)
}

fn digest(value: &Value) -> Result<String> {
    let value = value.as_str().ok_or("fixture digest string")?;
    require(super::valid_sha(value), "fixture SHA256")?;
    Ok(value.to_owned())
}

fn object_key(value: &Value, sha: &str) -> Result<()> {
    let key = value.as_str().ok_or("fixture object key")?;
    let parts = key.split('/').collect::<Vec<_>>();
    require(
        parts.len() >= 2
            && parts[parts.len() - 2] == "objects"
            && parts.last().copied() == Some(sha)
            && parts.iter().all(|part| {
                !part.is_empty()
                    && *part != "."
                    && *part != ".."
                    && part
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            }),
        "fixture object identity",
    )
}

fn small(path: &Path, sha: &str) -> Result<(Vec<u8>, Value, HeaderSnapshot)> {
    let bytes = fs::symlink_metadata(path)?.len();
    require((1..=SMALL_CAP).contains(&bytes), "fixture small metadata")?;
    let pin = CompletedInput {
        path: path.to_owned(),
        bytes,
        sha256: sha.to_owned(),
    };
    let (body, snapshot) = super::loss_binary(&pin, bytes)?;
    let UniqueJson(value) = serde_json::from_slice(&body)?;
    Ok((body, value, snapshot))
}

fn new_file(path: &Path) -> Result<File> {
    let parent = super::parent(path)?;
    Ok(File::from(openat(
        &parent,
        path.file_name().ok_or("fixture output filename")?,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?))
}

fn finish_file(file: File, path: &Path) -> Result<HeaderSnapshot> {
    file.sync_all()?;
    super::parent(path)?.sync_all()?;
    let snapshot = HeaderSnapshot {
        identity: super::file_identity(&file)?,
        file,
        path: path.to_owned(),
    };
    snapshot.recheck()?;
    Ok(snapshot)
}

fn write_new(path: &Path, body: &[u8]) -> Result<HeaderSnapshot> {
    let mut file = new_file(path)?;
    file.write_all(body)?;
    finish_file(file, path)
}

fn copy_authenticated(pin: CompletedInput, destination: &Path) -> Result<[HeaderSnapshot; 2]> {
    super::header_pin(&pin, pin.bytes)?;
    let mut input = super::header_file(&pin.path)?;
    let identity = super::file_identity(&input)?;
    require(identity.len == pin.bytes, "fixture exact source length")?;
    let mut output = new_file(destination)?;
    let mut buffer = vec![0_u8; 65_536];
    let mut remaining = pin.bytes;
    let mut whole = Sha256::new();
    while remaining > 0 {
        let take = usize::try_from(remaining.min(buffer.len() as u64))?;
        input.read_exact(&mut buffer[..take])?;
        whole.update(&buffer[..take]);
        output.write_all(&buffer[..take])?;
        remaining -= take as u64;
    }
    require(
        input.read(&mut [0])? == 0 && format!("{:x}", whole.finalize()) == pin.sha256,
        "fixture complete source SHA/EOF",
    )?;
    let source = HeaderSnapshot {
        file: input,
        path: pin.path,
        identity,
    };
    source.recheck()?;
    Ok([source, finish_file(output, destination)?])
}

// Caller supplies immutable snapshots and a NEW sibling directory. Partial
// files may remain on failure; there is no success receipt or publication head.
fn prepare(
    original: &Path,
    root_sha256: &str,
    sq8: &Path,
    output: &Path,
    payload_cap: usize,
) -> Result<String> {
    require(
        payload_cap >= BASE_PAYLOAD,
        "fixture base payload admission",
    )?;
    super::header_path(&output.join("manifest.json"))?;
    let parent = super::parent(output)?;
    require(
        !output.starts_with(original)
            && !original.starts_with(output)
            && matches!(fs::symlink_metadata(output), Err(e) if e.kind()==std::io::ErrorKind::NotFound),
        "fixture NEW output separate from inputs",
    )?;
    let (root_body, mut root, root_snapshot) = small(&original.join("manifest.json"), root_sha256)?;
    fields(
        &root,
        "schema generation base_epoch plane_manifest_sha256 page_manifest_sha256 discovery sq8_object_sha256 sq8_object_key sq8_etag canonical low step",
    )?;
    require(
        root["schema"] == "borsuk-two-bit-generation-v8",
        "fixture historical construction schema",
    )?;
    let generation = root["generation"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or("fixture generation")?;
    require(root["base_epoch"].as_u64().is_some(), "fixture base epoch")?;
    let canonical = &root["canonical"];
    fields(canonical, "rows dimensions bytes sha256 object_key")?;
    let rows = positive(&canonical["rows"])?;
    let dimensions = positive(&canonical["dimensions"])?;
    let discovery = &root["discovery"];
    fields(
        discovery,
        "mode profile root_sha256 root_bytes membership_sha256 membership_bytes leaves_sha256 leaves_bytes input_schema input_root_sha256 centroids_sha256 source_sha256 source_order_sha256 mean_sha256 records_sha256 sq8_sha256",
    )?;
    let profile: SemanticProfile = serde_json::from_value(discovery["profile"].clone())?;
    require(
        discovery["mode"] == "semantic" && profile.valid_geometry(rows, dimensions),
        "fixture semantic geometry",
    )?;
    require(
        canonical["bytes"].as_u64()
            == rows
                .checked_mul(
                    dimensions
                        .checked_mul(4)
                        .and_then(|n| n.checked_add(8))
                        .ok_or("fixture canonical width")?,
                )
                .map(|n| n as u64),
        "fixture canonical geometry",
    )?;
    object_key(&canonical["object_key"], &digest(&canonical["sha256"])?)?;
    let sq8_sha = digest(&root["sq8_object_sha256"])?;
    object_key(&root["sq8_object_key"], &sq8_sha)?;
    require(
        root["sq8_etag"].as_str().is_some_and(|s| {
            !s.is_empty() && !s.starts_with("W/") && !s.chars().any(char::is_control)
        }),
        "fixture strong SQ8 ETag",
    )?;
    let calibration: Calibration = serde_json::from_slice(&root_body)?;
    require(
        calibration.low.len() == dimensions
            && calibration.step.len() == dimensions
            && calibration.low.iter().all(|v| v.is_finite())
            && calibration.step.iter().all(|v| v.is_finite() && *v > 0.),
        "fixture calibration",
    )?;
    let units = rows.div_ceil(32);
    let sidecar_bytes = units.checked_mul(32).ok_or("fixture sidecar overflow")?;
    let width = dimensions.checked_add(12).ok_or("fixture SQ8 width")?;
    // Loading the final authority temporarily retains a second digest table.
    require(
        BASE_PAYLOAD
            .checked_add(
                sidecar_bytes
                    .checked_mul(2)
                    .ok_or("fixture proof workspace")?,
            )
            .and_then(|n| n.checked_add(width.checked_mul(32)?))
            .is_some_and(|n| n <= payload_cap),
        "fixture simultaneous payload",
    )?;
    let sq8_pin = CompletedInput {
        path: sq8.to_owned(),
        bytes: rows.checked_mul(width).ok_or("fixture SQ8 length")? as u64,
        sha256: sq8_sha.clone(),
    };
    super::header_pin(&sq8_pin, sq8_pin.bytes)?;
    let sq8_file = super::header_file(sq8)?;
    let sq8_identity = super::file_identity(&sq8_file)?;
    require(
        sq8_identity.len == sq8_pin.bytes,
        "fixture SQ8 exact file length",
    )?;
    let early_sq8 = HeaderSnapshot {
        file: sq8_file,
        path: sq8.to_owned(),
        identity: sq8_identity,
    };
    let plane_sha = digest(&root["plane_manifest_sha256"])?;
    let (plane_body, plane_value, plane_snapshot) =
        small(&original.join("plane/manifest.json"), &plane_sha)?;
    let plane: SourcePlaneReceipt = serde_json::from_value(plane_value)?;
    // Record width depends only on the admitted dimension. Use the public codec
    // constructor rather than its crate-private padding helper; this zero-mean
    // geometry probe does not encode or replace any authenticated SOURCE bytes.
    // Profile geometry bounds its small temporary allocation within BASE_PAYLOAD.
    let record_bytes = RotatedTwoBitCodec::new(&vec![0.; dimensions], plane.seed)?.record_bytes();
    require(
        plane.schema == "borsuk-two-bit-plane-v3"
            && plane.rows == rows
            && plane.dimensions == dimensions
            && plane.page_rows == 32
            && plane.seed == 20260923
            && !plane.query_or_truth_used
            && plane.sq8_sha256 == sq8_sha
            && plane.record_bytes == record_bytes,
        "fixture plane binding",
    )?;
    for (name, expected) in [
        ("source_sha256", &plane.source_sha256),
        ("source_order_sha256", &plane.source_order_sha256),
        ("mean_sha256", &plane.mean_sha256),
        ("records_sha256", &plane.records_sha256),
        ("sq8_sha256", &sq8_sha),
    ] {
        require(
            super::valid_sha(expected) && discovery[name] == *expected,
            "fixture discovery/plane identity",
        )?;
    }
    require(
        super::valid_sha(&plane.page_digest_sha256),
        "fixture SOURCE proof digest",
    )?;
    digest(&discovery["input_root_sha256"])?;
    digest(&discovery["centroids_sha256"])?;
    require(
        discovery["input_schema"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "fixture original input schema",
    )?;
    let old_page_sha = digest(&root["page_manifest_sha256"])?;
    let (old_page_body, old_page, old_page_snapshot) =
        small(&original.join("page_manifest.json"), &old_page_sha)?;
    fields(
        &old_page,
        "schema generation rows dimensions page_rows object_sha256 page_digest_sha256",
    )?;
    require(
        old_page["generation"] == generation
            && old_page["rows"] == rows
            && old_page["dimensions"] == dimensions
            && old_page["object_sha256"] == sq8_sha
            && old_page["page_rows"] == 256,
        "fixture original SQ8 geometry",
    )?;
    let old_pin = CompletedInput {
        path: original.join("page_digests.bin"),
        bytes: rows
            .div_ceil(256)
            .checked_mul(32)
            .ok_or("fixture old sidecar overflow")? as u64,
        sha256: digest(&old_page["page_digest_sha256"])?,
    };
    let (old_sidecar, old_sidecar_snapshot) = super::loss_binary(&old_pin, old_pin.bytes)?;
    PageAuthority::load(&old_page_body, &old_page_sha, &old_sidecar)
        .map_err(|e| format!("fixture old proof authority: {e:?}"))?;
    drop(old_sidecar);
    let root_bytes = positive(&discovery["root_bytes"])?;
    require(
        root_bytes <= profile.root_cap()
            && discovery["membership_bytes"].as_u64() == units.checked_mul(4).map(|n| n as u64)
            && discovery["leaves_bytes"].as_u64()
                == units
                    .checked_mul(
                        dimensions
                            .checked_mul(2)
                            .and_then(|n| n.checked_add(4))
                            .ok_or("fixture leaf width")?,
                    )
                    .map(|n| n as u64),
        "fixture router body geometry",
    )?;
    let roster = [
        ("plane/manifest.json", plane_body.len(), plane_sha),
        (
            "plane/mean.bin",
            dimensions.checked_mul(4).ok_or("fixture mean width")?,
            plane.mean_sha256,
        ),
        (
            "plane/records.bin",
            rows.checked_mul(plane.record_bytes)
                .ok_or("fixture SOURCE width")?,
            plane.records_sha256,
        ),
        (
            "plane/page_digests.bin",
            sidecar_bytes,
            plane.page_digest_sha256,
        ),
        (
            "router/root.bin",
            root_bytes,
            digest(&discovery["root_sha256"])?,
        ),
        (
            "router/membership.bin",
            units.checked_mul(4).ok_or("fixture membership width")?,
            digest(&discovery["membership_sha256"])?,
        ),
        (
            "router/leaves.bin",
            positive(&discovery["leaves_bytes"])?,
            digest(&discovery["leaves_sha256"])?,
        ),
    ];
    fs::DirBuilder::new().mode(0o700).create(output)?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(output.join("plane"))?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(output.join("router"))?;
    let directory = super::directory(output)?;
    let directory_identity = directory.metadata()?;
    let parent_identity = parent.metadata()?;
    let mut snapshots = vec![
        root_snapshot,
        plane_snapshot,
        old_page_snapshot,
        old_sidecar_snapshot,
        early_sq8,
    ];
    for (name, bytes, sha256) in roster {
        snapshots.extend(copy_authenticated(
            CompletedInput {
                path: original.join(name),
                bytes: bytes as u64,
                sha256,
            },
            &output.join(name),
        )?);
    }
    let (sidecar, sq8_snapshot) = super::sq8_subpage_shortlist::authenticated_sq8_digests(
        &sq8_pin,
        rows,
        dimensions,
        payload_cap - BASE_PAYLOAD,
    )?;
    snapshots.push(sq8_snapshot);
    let page_body = serde_json::to_vec(
        &json!({"schema":"borsuk-v115-sq8-page-authority-v2","generation":generation,"rows":rows,"dimensions":dimensions,"page_rows":32,"object_sha256":sq8_sha,"page_digest_sha256":hash(&sidecar)}),
    )?;
    PageAuthority::load(&page_body, &hash(&page_body), &sidecar)
        .map_err(|e| format!("fixture new proof authority: {e:?}"))?;
    snapshots.push(write_new(&output.join("page_digests.bin"), &sidecar)?);
    snapshots.push(write_new(&output.join("page_manifest.json"), &page_body)?);
    root["schema"] = json!("borsuk-two-bit-generation-v9");
    root["page_manifest_sha256"] = json!(hash(&page_body));
    let new_root = serde_json::to_vec(&root)?;
    require(
        new_root.len() <= SMALL_CAP as usize,
        "fixture output root cap",
    )?;
    let after: Calibration = serde_json::from_slice(&new_root)?;
    require(
        calibration
            .low
            .iter()
            .map(|v| v.to_bits())
            .eq(after.low.iter().map(|v| v.to_bits()))
            && calibration
                .step
                .iter()
                .map(|v| v.to_bits())
                .eq(after.step.iter().map(|v| v.to_bits())),
        "fixture unchanged calibration bits",
    )?;
    for snapshot in &snapshots {
        snapshot.recheck()?;
    }
    let current = super::directory(output)?.metadata()?;
    let current_parent = super::parent(output)?.metadata()?;
    require(
        (current.dev(), current.ino()) == (directory_identity.dev(), directory_identity.ino())
            && (current_parent.dev(), current_parent.ino())
                == (parent_identity.dev(), parent_identity.ino()),
        "fixture output directory identity",
    )?;
    write_new(&output.join("manifest.json"), &new_root)?.recheck()?;
    directory.sync_all()?;
    parent.sync_all()?;
    Ok(hash(&new_root))
}

pub(super) fn execute(config: &Path, sha: &str, output: &Path, report: &Path) -> Result<bool> {
    let (c, config_bytes): (Config, _) = super::read_config(config, sha)?;
    require(
        c.schema == "borsuk-source-preserving-v9-fixture-config-v1",
        "fixture config schema",
    )?;
    let caller = usize::try_from(config_bytes)?
        .checked_mul(8)
        .and_then(|n| n.checked_add(65_536))
        .ok_or("fixture caller payload")?;
    let cap = c
        .max_payload_bytes
        .checked_sub(caller)
        .ok_or("fixture caller admission")?;
    require(cap >= BASE_PAYLOAD, "fixture metadata admission")?;
    super::header_pin(&c.root, SMALL_CAP)?;
    super::header_pin(&c.sq8, c.sq8.bytes)?;
    let original = c.root.path.parent().ok_or("fixture input directory")?;
    require(
        !report.starts_with(original) && !report.starts_with(output),
        "fixture receipt separate from metadata trees",
    )?;
    super::execute_report(report, REPORT_SCHEMA, || {
        require(
            c.root.path.file_name() == Some(std::ffi::OsStr::new("manifest.json")),
            "fixture root filename",
        )?;
        let (root, snapshot) = super::header_json(&c.root)?;
        let rows = positive(&root["canonical"]["rows"])?;
        let dimensions = positive(&root["canonical"]["dimensions"])?;
        require(
            c.sq8.sha256 == root["sq8_object_sha256"]
                && c.sq8.bytes
                    == rows
                        .checked_mul(dimensions.checked_add(12).ok_or("fixture config width")?)
                        .map(|n| n as u64)
                        .ok_or("fixture config length")?,
            "fixture configured SQ8 binding",
        )?;
        snapshot.recheck()?;
        drop(root);
        let new_root = prepare(
            c.root.path.parent().ok_or("fixture input directory")?,
            &c.root.sha256,
            &c.sq8.path,
            output,
            cap,
        )?;
        snapshot.recheck()?;
        Ok(
            json!({"schema":REPORT_SCHEMA,"status":"IDENTITY_REBOUND_PROOFS_ONLY","complete":true,"config_sha256":sha,"original_root":c.root,"sq8":c.sq8,"output":output,"generation_root_sha256":new_root,"rows":rows,"dimensions":dimensions,"sq8_proof_rows":32,"max_payload_bytes":c.max_payload_bytes,"caller_payload_bytes":caller,"payload_model":"2 MiB metadata/copy/snapshot allowance + two SQ8 digest tables + one 32-record buffer; allocator/runtime and OS cache require external measurement","root_fields_changed":["schema","page_manifest_sha256"],"source_and_router_refitted":false,"canonical_body_authenticated":false,"serving_qualified":false,"query_or_truth_opened":false,"performance_claim":false,"vendor_or_scientific_win_claim":false}),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::prepare;
    use borsuk::{
        semantic_unit_router::SemanticProfile, sq8_page_authority::PageAuthority,
        two_bit_build::TwoBitGenerationBuilder, two_bit_source::TwoBitSource,
    };
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use std::fs;

    fn hash(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn v9_fixture_preserves_real_source_router_and_sq8_while_reblocking_proofs() {
        let temp = tempfile::tempdir().unwrap();
        let rows = 257_usize;
        let dimensions = 5_usize;
        let raw = (0..rows)
            .flat_map(|_| {
                [1_f32, 0., 0., 0., 0.]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
            })
            .collect::<Vec<_>>();
        let sq8 = (0..rows)
            .flat_map(|row| {
                let mut record = (90_000_i64 - row as i64).to_le_bytes().to_vec();
                record.extend(1_f32.to_le_bytes());
                record.extend([255, 0, 0, 0, 0]);
                record
            })
            .collect::<Vec<_>>();
        let raw_path = temp.path().join("raw");
        let sq8_path = temp.path().join("sq8");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let raw_sha = hash(&raw);
        let sq8_sha = hash(&sq8);
        let key = format!("fixture/objects/{sq8_sha}");
        let order = (0..rows as u64).collect::<Vec<_>>();
        let original = temp.path().join("original");
        TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &raw_sha,
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            base_epoch: 7,
            generation: 3,
            low: &[0.; 5],
            step: &[1. / 255.; 5],
            sq8_object_key: &key,
            sq8_etag: "\"immutable-fixture\"",
        }
        .build_with_semantic_profile(
            Some(&order),
            SemanticProfile::Native100k,
            &original,
            64_000_000,
        )
        .unwrap();

        // Construct an explicitly historical proof envelope from real builder
        // artifacts. All SOURCE/router bytes and their identities stay intact.
        let mut old_root: Value =
            serde_json::from_slice(&fs::read(original.join("manifest.json")).unwrap()).unwrap();
        let old_sidecar = sq8
            .chunks(256 * (dimensions + 12))
            .flat_map(|block| Sha256::digest(block).to_vec())
            .collect::<Vec<_>>();
        let old_page = serde_json::to_vec(&json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":3,
            "rows":rows, "dimensions":dimensions, "page_rows":256,
            "object_sha256":sq8_sha, "page_digest_sha256":hash(&old_sidecar)
        }))
        .unwrap();
        fs::write(original.join("page_digests.bin"), &old_sidecar).unwrap();
        fs::write(original.join("page_manifest.json"), &old_page).unwrap();
        old_root["schema"] = json!("borsuk-two-bit-generation-v8");
        old_root["page_manifest_sha256"] = json!(hash(&old_page));
        let old_bytes = serde_json::to_vec(&old_root).unwrap();
        fs::write(original.join("manifest.json"), &old_bytes).unwrap();
        let output = temp.path().join("converted");
        let new_sha = prepare(
            &original,
            &hash(&old_bytes),
            &sq8_path,
            &output,
            4 * 1024 * 1024,
        )
        .expect("convert authenticated historical construction evidence without refitting");
        let new_bytes = fs::read(output.join("manifest.json")).unwrap();
        assert_eq!(hash(&new_bytes), new_sha);
        let new_root: Value = serde_json::from_slice(&new_bytes).unwrap();
        assert_eq!(new_root["schema"], "borsuk-two-bit-generation-v9");
        for (name, value) in old_root.as_object().unwrap() {
            if name != "schema" && name != "page_manifest_sha256" {
                assert_eq!(&new_root[name], value, "unchanged root field {name}");
            }
        }
        for name in [
            "plane/manifest.json",
            "plane/mean.bin",
            "plane/records.bin",
            "plane/page_digests.bin",
            "router/root.bin",
            "router/membership.bin",
            "router/leaves.bin",
        ] {
            assert_eq!(
                fs::read(original.join(name)).unwrap(),
                fs::read(output.join(name)).unwrap(),
                "unchanged {name}"
            );
        }
        assert!(
            !output.join("canonical.bin").exists(),
            "no canonical ownership claim"
        );
        assert_eq!(fs::read(&sq8_path).unwrap(), sq8);
        let page = fs::read(output.join("page_manifest.json")).unwrap();
        assert_eq!(hash(&page), new_root["page_manifest_sha256"]);
        let sidecar = fs::read(output.join("page_digests.bin")).unwrap();
        let authority = PageAuthority::load(&page, &hash(&page), &sidecar).unwrap();
        assert_eq!(authority.page_rows(), 32);
        assert_eq!(sidecar.len(), 9 * 32);
        for (block, bytes) in sq8.chunks(32 * (dimensions + 12)).enumerate() {
            authority.verify_payload(block, block, bytes).unwrap();
            let mut changed = bytes.to_vec();
            *changed.last_mut().unwrap() ^= 1;
            assert!(authority.verify_payload(block, block, &changed).is_err());
        }
        assert!(
            prepare(
                &original,
                &hash(&old_bytes),
                &sq8_path,
                &output,
                4 * 1024 * 1024
            )
            .is_err(),
            "no overwrite"
        );
        let refused = temp.path().join("wrong-root");
        assert!(
            prepare(
                &original,
                &"0".repeat(64),
                &sq8_path,
                &refused,
                4 * 1024 * 1024
            )
            .is_err()
        );
        assert!(!refused.join("manifest.json").exists());
        let refused = temp.path().join("insufficient-payload");
        assert!(prepare(&original, &hash(&old_bytes), &sq8_path, &refused, 1).is_err());
        assert!(!refused.join("manifest.json").exists());

        // Each authenticated body is an independent refusal. Restoring bytes
        // permits the next mutation to test its own authority, not stale damage.
        for (index, path) in [
            original.join("page_manifest.json"),
            original.join("page_digests.bin"),
            original.join("plane/manifest.json"),
            original.join("plane/mean.bin"),
            original.join("plane/records.bin"),
            original.join("plane/page_digests.bin"),
            original.join("router/root.bin"),
            original.join("router/membership.bin"),
            original.join("router/leaves.bin"),
            sq8_path.clone(),
        ]
        .into_iter()
        .enumerate()
        {
            let before = fs::read(&path).unwrap();
            let mut changed = before.clone();
            *changed.last_mut().unwrap() ^= 1;
            fs::write(&path, &changed).unwrap();
            let refused = temp.path().join(format!("corrupt-{index}"));
            assert!(
                prepare(
                    &original,
                    &hash(&old_bytes),
                    &sq8_path,
                    &refused,
                    4 * 1024 * 1024
                )
                .is_err(),
                "corrupted {}",
                path.display()
            );
            assert!(
                !refused.join("manifest.json").exists(),
                "no accepted root after corruption"
            );
            fs::write(&path, &before).unwrap();
        }
        for (name, bad_sq8) in [
            ("short-sq8", sq8[..sq8.len() - 1].to_vec()),
            ("long-sq8", [sq8.clone(), vec![0]].concat()),
        ] {
            fs::write(&sq8_path, &bad_sq8).unwrap();
            let refused = temp.path().join(name);
            assert!(
                prepare(
                    &original,
                    &hash(&old_bytes),
                    &sq8_path,
                    &refused,
                    4 * 1024 * 1024
                )
                .is_err()
            );
            assert!(!refused.join("manifest.json").exists());
        }
        fs::write(&sq8_path, &sq8).unwrap();
        assert_eq!(fs::read(original.join("manifest.json")).unwrap(), old_bytes);

        let config = json!({
            "schema":"borsuk-source-preserving-v9-fixture-config-v1",
            "root":{"path":original.join("manifest.json"),"bytes":old_bytes.len(),"sha256":hash(&old_bytes)},
            "sq8":{"path":sq8_path,"bytes":sq8.len(),"sha256":hash(&sq8)},
            "max_payload_bytes":8*1024*1024
        });
        let config_path = temp.path().join("fixture-config.json");
        let config_body = serde_json::to_vec(&config).unwrap();
        fs::write(&config_path, &config_body).unwrap();
        let cli_output = temp.path().join("cli-fixture");
        let report = temp.path().join("fixture-report.json");
        assert!(super::execute(&config_path, &hash(&config_body), &cli_output, &report).unwrap());
        let report_body = fs::read(&report).unwrap();
        let receipt: Value = serde_json::from_slice(&report_body).unwrap();
        assert_eq!(receipt["status"], "IDENTITY_REBOUND_PROOFS_ONLY");
        assert_eq!(receipt["serving_qualified"], false);
        assert_eq!(receipt["canonical_body_authenticated"], false);
        assert_eq!(receipt["query_or_truth_opened"], false);
        assert_eq!(
            receipt["generation_root_sha256"],
            hash(&fs::read(cli_output.join("manifest.json")).unwrap())
        );
        let untouched = temp.path().join("report-collision-output");
        assert!(super::execute(&config_path, &hash(&config_body), &untouched, &report).is_err());
        assert!(!untouched.exists());
        assert_eq!(fs::read(&report).unwrap(), report_body);
        let forbidden_report = original.join("must-not-write.json");
        assert!(
            super::execute(
                &config_path,
                &hash(&config_body),
                &untouched,
                &forbidden_report
            )
            .is_err()
        );
        assert!(!forbidden_report.exists());

        for kind in 0..7 {
            let mut bad = config.clone();
            match kind {
                0 => bad["unexpected"] = json!(true),
                1 => bad["schema"] = json!("wrong"),
                2 => bad["root"]["sha256"] = json!("0".repeat(64)),
                3 => bad["sq8"]["sha256"] = json!("0".repeat(64)),
                4 => bad["sq8"]["bytes"] = json!(sq8.len() - 1),
                5 => bad["max_payload_bytes"] = json!(1),
                6 => bad["root"]["bytes"] = json!(old_bytes.len() + 1),
                _ => unreachable!(),
            }
            let bytes = serde_json::to_vec(&bad).unwrap();
            fs::write(&config_path, &bytes).unwrap();
            let bad_output = temp.path().join(format!("bad-config-{kind}"));
            let bad_report = temp.path().join(format!("bad-config-{kind}.json"));
            let outcome = super::execute(&config_path, &hash(&bytes), &bad_output, &bad_report);
            assert!(!matches!(outcome, Ok(true)), "bad config {kind}");
            assert!(!bad_output.join("manifest.json").exists());
        }
    }
}
