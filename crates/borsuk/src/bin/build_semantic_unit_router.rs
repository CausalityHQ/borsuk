//! Bounded offline research construction; no query-quality or latency claim.
//! Accepts only an externally authenticated historical v4 root and its centroids.
//! Membership is one LE u32 leaf ID per original unit. Each leaf record is a LE
//! u32 original unit ID followed by its unchanged D LE FP16 coefficients.
//! Source-row counts are derived from the original rows and 32-row unit geometry.
use borsuk::semantic_unit_router::{self, ALLOCATION_CAP, SemanticProfile, SourceIdentity};
#[cfg(test)]
use borsuk::semantic_unit_router::{Geometry, admit, assign_groups, preflight};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const ROOT_CAP: usize = 64 * 1024;
const BLOB_CAP: usize = 8 * 1024 * 1024;
const MANIFEST_CAP: usize = 4 * 1024 * 1024;
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

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn bounded(path: &Path, cap: usize) -> Result<Vec<u8>> {
    // Nonblocking open lets descriptor metadata reject FIFOs before any read.
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
        .open(path)?;
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
    if blob.len() < 32 {
        return Err("centroid header".into());
    }
    let geometry = semantic_unit_router::preflight(
        blob,
        u64::from_le_bytes(blob[8..16].try_into()?) as usize,
        u32::from_le_bytes(blob[16..20].try_into()?) as usize,
        SemanticProfile::Native100k,
    )?;
    semantic_unit_router::validate_publication(
        body,
        membership,
        payload,
        &SourceIdentity {
            profile: SemanticProfile::Native100k,
            schema: INPUT_SCHEMA,
            root_sha256: root_sha,
            centroids_sha256: &hash(blob),
            rows: geometry.rows,
            dimensions: geometry.dimensions,
        },
        blob,
    )
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
    let artifacts = semantic_unit_router::build(
        &blob,
        &SourceIdentity {
            profile: SemanticProfile::Native100k,
            schema: &input.schema,
            root_sha256: root_sha,
            centroids_sha256: &input.centroids_sha256,
            rows: input.canonical.rows,
            dimensions: input.canonical.dimensions,
        },
        ALLOCATION_CAP,
    )?;
    let body = artifacts.manifest;
    let membership = artifacts.membership;
    let payload = artifacts.leaves;
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    for (name, bytes) in [
        ("root.bin", body.as_slice()),
        ("membership.bin", &membership),
        ("leaves.bin", &payload),
    ] {
        write_new(&staging.path().join(name), bytes)?;
    }
    let staged_body = bounded(&staging.path().join("root.bin"), MANIFEST_CAP)?;
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

    fn root_sha_or_sha(root: &Path) -> String {
        digest(&fs::read(root.join("manifest.json")).unwrap())
    }
    fn manifest_value(output: &Path, sha: &str, blob: &[u8]) -> serde_json::Value {
        let body = fs::read(output.join("root.bin")).unwrap();
        let membership = fs::read(output.join("membership.bin")).unwrap();
        let input = SourceIdentity {
            profile: SemanticProfile::Native100k,
            schema: INPUT_SCHEMA,
            root_sha256: sha,
            centroids_sha256: &hash(blob),
            rows: u64::from_le_bytes(blob[8..16].try_into().unwrap()) as usize,
            dimensions: u32::from_le_bytes(blob[16..20].try_into().unwrap()) as usize,
        };
        let router = semantic_unit_router::SemanticUnitRouter::open(
            &body,
            &membership,
            &hash(&body),
            &input,
            1 << 20,
        )
        .unwrap();
        serde_json::to_value(router.manifest()).unwrap()
    }
    fn encode_value(value: &serde_json::Value) -> Vec<u8> {
        serde_json::from_value::<semantic_unit_router::Manifest>(value.clone())
            .unwrap()
            .encode()
            .unwrap()
    }

    #[test]
    fn frozen_small_artifact_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let values = (0..129)
            .map(|id| {
                [
                    f16::from_f32(id as f32).to_bits(),
                    f16::from_f32(2.0).to_bits(),
                ]
            })
            .collect::<Vec<_>>();
        let root = temp.path().join("root");
        let (sha, _) = fixture(&root, 4097, &values);
        let output = temp.path().join("router");
        build(&root, &sha, &output).unwrap();
        // Captured from the pre-extraction implementation at ae772138.
        for (name, expected) in [
            (
                "membership.bin",
                "919207b3fc34a3d945d3310331767bec4033cb7873859bad8d43f72a06f510b0",
            ),
            (
                "leaves.bin",
                "a308a30fec67ef28269b8c7826438dd8dbcff1fe0f692de7f0685c2a34ee8906",
            ),
        ] {
            assert_eq!(
                digest(&fs::read(output.join(name)).unwrap()),
                expected,
                "{name}"
            );
        }
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
        let body = fs::read(output.join("root.bin")).unwrap();
        assert_eq!(digest(&body), manifest_sha);
        let manifest = manifest_value(
            &output,
            &root_sha_or_sha(&root),
            &fs::read(root.join("centroids.bin")).unwrap(),
        );
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
        for name in ["root.bin", "membership.bin", "leaves.bin"] {
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
        let cancellation_manifest = manifest_value(
            &cancellation_output,
            &sha,
            &fs::read(cancellation_root.join("centroids.bin")).unwrap(),
        );
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
                &encode_value(&forged),
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
                &encode_value(&forged),
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
                &encode_value(&forged),
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
                &encode_value(&forged),
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
                &encode_value(&forged),
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
        let geometry = preflight(&blob, 1, 2, SemanticProfile::Native100k).unwrap();
        let admitted = admit(geometry, ALLOCATION_CAP, SemanticProfile::Native100k).unwrap();
        assert_eq!(
            admit(geometry, admitted, SemanticProfile::Native100k).unwrap(),
            admitted
        );
        assert!(admit(geometry, admitted - 1, SemanticProfile::Native100k).is_err());
        let maximal = Geometry {
            rows: 100_000,
            dimensions: 768,
            units: 3125,
            blob_bytes: 4_800_032,
        };
        assert!(admit(maximal, ALLOCATION_CAP, SemanticProfile::Native100k).is_ok());
        assert!(
            admit(
                Geometry {
                    units: usize::MAX,
                    ..geometry
                },
                ALLOCATION_CAP,
                SemanticProfile::Native100k
            )
            .is_err()
        );
        assert!(preflight(&blob[..31], 1, 2, SemanticProfile::Native100k).is_err());
        assert!(preflight(&blob[..blob.len() - 1], 1, 2, SemanticProfile::Native100k).is_err());
        assert!(
            preflight(
                &[blob.as_slice(), &[0]].concat(),
                1,
                2,
                SemanticProfile::Native100k
            )
            .is_err()
        );
        assert!(preflight(&blob, 2, 2, SemanticProfile::Native100k).is_err());
        assert!(preflight(&blob, 1, 3, SemanticProfile::Native100k).is_err());
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
            assert!(preflight(&invalid, 1, 2, SemanticProfile::Native100k).is_err());
        }
        for rows in [0_u64, 100_001, u64::MAX] {
            let mut invalid = blob.clone();
            invalid[8..16].copy_from_slice(&rows.to_le_bytes());
            assert!(preflight(&invalid, 1, 2, SemanticProfile::Native100k).is_err());
        }
        let mut invalid = blob.clone();
        invalid[0] ^= 1;
        assert!(preflight(&invalid, 1, 2, SemanticProfile::Native100k).is_err());
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

    #[test]
    fn fifo_is_rejected_without_waiting_for_a_writer() {
        let temp = tempfile::tempdir().unwrap();
        let fifo = temp.path().join("manifest.json");
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker =
            std::thread::spawn(move || sender.send(bounded(&fifo, ROOT_CAP).is_err()).unwrap());
        // A regression fails within one second instead of hanging the test process.
        assert!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("FIFO open blocked")
        );
        worker.join().unwrap();
    }

    #[test]
    fn recursive_trainer_partition_is_lossless_and_repeatable() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let values = (0..2049)
            .map(|id| {
                [
                    f16::from_f32(id as f32).to_bits(),
                    f16::from_f32(2.0).to_bits(),
                ]
            })
            .collect::<Vec<_>>();
        let (sha, blob) = fixture(&root, 65_537, &values);
        let output = temp.path().join("router");
        let repeat = temp.path().join("repeat");
        let manifest_sha = build(&root, &sha, &output).unwrap();
        assert_eq!(build(&root, &sha, &repeat).unwrap(), manifest_sha);
        for name in ["root.bin", "membership.bin", "leaves.bin"] {
            assert_eq!(
                fs::read(output.join(name)).unwrap(),
                fs::read(repeat.join(name)).unwrap()
            );
        }
        let body = fs::read(output.join("root.bin")).unwrap();
        let manifest = manifest_value(
            &output,
            &root_sha_or_sha(&root),
            &fs::read(root.join("centroids.bin")).unwrap(),
        );
        assert_eq!(manifest["algorithm"]["requested_centers"], 33);
        assert_eq!(manifest["algorithm"]["training_centers"], 33);
        assert_eq!(manifest["final_unit_rows"], 1);
        for leaf in manifest["leaves"].as_array().unwrap() {
            assert!((1..=64).contains(&leaf["unit_count"].as_u64().unwrap()));
        }
        let payload = fs::read(output.join("leaves.bin")).unwrap();
        let membership = fs::read(output.join("membership.bin")).unwrap();
        assert_eq!(membership.len(), 2049 * 4);
        assert_eq!(payload.len(), 2049 * 8);
        let mut units = Vec::new();
        for record in payload.as_chunks::<8>().0 {
            let id = u32::from_le_bytes(record[..4].try_into().unwrap()) as usize;
            assert!(id < 2049);
            assert_eq!(&record[4..], &blob[32 + id * 4..36 + id * 4]);
            units.push(id);
        }
        units.sort_unstable();
        assert_eq!(units, (0..2049).collect::<Vec<_>>());
    }

    #[test]
    fn forged_groups_and_short_leaf_continuations_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let values = (0..65)
            .map(|id| [f16::from_f32(id as f32).to_bits(), 0])
            .collect::<Vec<_>>();
        let (sha, blob) = fixture(&root, 2049, &values);
        let output = temp.path().join("router");
        build(&root, &sha, &output).unwrap();
        let body = fs::read(output.join("root.bin")).unwrap();
        let membership = fs::read(output.join("membership.bin")).unwrap();
        let payload = fs::read(output.join("leaves.bin")).unwrap();
        verify(&body, &membership, &payload, &sha, &blob).unwrap();
        let manifest = manifest_value(
            &output,
            &root_sha_or_sha(&root),
            &fs::read(root.join("centroids.bin")).unwrap(),
        );
        assert_eq!(manifest["algorithm"]["training_centers"], 2);
        assert!(manifest["leaves"][0]["unit_count"].as_u64().unwrap() < 64);
        let mut forged = manifest.clone();
        forged["leaves"][0]["group_ordinal"] = 2.into();
        assert!(verify(&encode_value(&forged), &membership, &payload, &sha, &blob).is_err());
        forged = manifest;
        forged["leaves"][1]["group_ordinal"] = 0.into();
        forged["leaves"][1]["chunk_ordinal"] = 1.into();
        assert!(verify(&encode_value(&forged), &membership, &payload, &sha, &blob).is_err());
    }
}
