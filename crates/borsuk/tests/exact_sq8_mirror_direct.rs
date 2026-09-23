#[path = "../src/exact_sq8_mirror.rs"]
mod exact_sq8_mirror;
#[path = "../src/exact_sq8_nominee.rs"]
mod exact_sq8_nominee;

use exact_sq8_mirror::{ExactSq8Mirror, MirrorManifest, Placement};
use exact_sq8_nominee::Sq8Geometry;
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

const OBJECT_SHA: &str = "ea480fa7f894804c8db18066d56ae797ed2ba972dfaf8e0aad9ec344cd02ab4c";
const SIDECAR_SHA: &str = "f8614acc87e87cc1cbbd2dd14ed22291d1305eea13a90b073b1a31e981297af5";
const BLOCK_SHA: [u8; 32] = [
    0xea, 0x48, 0x0f, 0xa7, 0xf8, 0x94, 0x80, 0x4c, 0x8d, 0xb1, 0x80, 0x66, 0xd5, 0x6a, 0xe7, 0x97,
    0xed, 0x2b, 0xa9, 0x72, 0xdf, 0xaf, 0x8e, 0x0a, 0xad, 0x9e, 0xc3, 0x44, 0xcd, 0x02, 0xab, 0x4c,
];

fn fixture() -> (PathBuf, PathBuf, PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "borsuk-v114-mirror-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    fs::create_dir(&root).unwrap();
    let object = root.join("sq8.bin");
    let sidecar = root.join("blocks.sha256");
    let mut bytes = Vec::new();
    for (id, norm, code) in [(4i64, 1.0f32, [1, 0, 0, 0]), (9, 4.0, [2, 0, 0, 0])] {
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&norm.to_le_bytes());
        bytes.extend_from_slice(&code);
    }
    fs::write(&object, bytes).unwrap();
    fs::write(&sidecar, BLOCK_SHA).unwrap();
    (root, object, sidecar)
}

fn manifest() -> MirrorManifest {
    MirrorManifest {
        format_version: 1,
        generation: 7,
        max_nominees: 2,
        geometry: Sq8Geometry {
            rows: 2,
            dimensions: 4,
        },
        object_sha256: OBJECT_SHA.to_owned(),
        block_digest_sha256: SIDECAR_SHA.to_owned(),
        low: vec![0.0; 4],
        step: vec![1.0; 4],
    }
}

#[test]
fn authenticated_ram_and_file_score_identically_then_detect_disk_corruption() {
    let (root, object, sidecar) = fixture();
    let ram = ExactSq8Mirror::open(&object, &sidecar, manifest(), Placement::Ram).unwrap();
    let disk = ExactSq8Mirror::open(&object, &sidecar, manifest(), Placement::File).unwrap();
    let query = [1.0, 0.0, 0.0, 0.0];
    let expected = ram.score(&[1, 0], &query).unwrap();
    assert_eq!(expected, disk.score(&[1, 0], &query).unwrap());
    assert_eq!(ram.generation(), 7);
    let mut writer = OpenOptions::new().write(true).open(&object).unwrap();
    writer.write_at(&[99], 12).unwrap();
    writer.flush().unwrap();
    assert!(disk.score(&[0], &query).is_err());
    assert_eq!(expected, ram.score(&[1, 0], &query).unwrap());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn rejects_wrong_object_or_sidecar_identity() {
    let (root, object, sidecar) = fixture();
    let mut wrong = manifest();
    wrong.object_sha256 = "0".repeat(64);
    assert!(ExactSq8Mirror::open(&object, &sidecar, wrong, Placement::File).is_err());
    let mut wrong = manifest();
    wrong.block_digest_sha256 = "0".repeat(64);
    assert!(ExactSq8Mirror::open(&object, &sidecar, wrong, Placement::File).is_err());
    let mut bounded = manifest();
    bounded.max_nominees = 1;
    let mirror = ExactSq8Mirror::open(&object, &sidecar, bounded, Placement::File).unwrap();
    assert!(mirror.score(&[0, 1], &[0.0; 4]).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn rejects_a_manifest_bound_sidecar_that_describes_other_bytes() {
    let (root, object, sidecar) = fixture();
    let wrong_block_digest = [7u8; 32];
    fs::write(&sidecar, wrong_block_digest).unwrap();
    let mut authority = manifest();
    authority.block_digest_sha256 = format!("{:x}", Sha256::digest(wrong_block_digest));
    assert!(ExactSq8Mirror::open(&object, &sidecar, authority, Placement::File).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn verifies_both_blocks_of_a_crossing_final_row() {
    let (root, object, sidecar) = fixture();
    let mut bytes = Vec::new();
    for id in 0i64..6 {
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&0.0f32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 780]);
    }
    let mut digests = Vec::new();
    for block in bytes.chunks(4096) {
        digests.extend_from_slice(&Sha256::digest(block));
    }
    fs::write(&object, &bytes).unwrap();
    fs::write(&sidecar, &digests).unwrap();
    let mut authority = manifest();
    authority.geometry = Sq8Geometry {
        rows: 6,
        dimensions: 780,
    };
    authority.low = vec![0.0; 780];
    authority.step = vec![1.0; 780];
    authority.object_sha256 = format!("{:x}", Sha256::digest(&bytes));
    authority.block_digest_sha256 = format!("{:x}", Sha256::digest(&digests));
    let disk = ExactSq8Mirror::open(&object, &sidecar, authority, Placement::File).unwrap();
    let scores = disk.score(&[5], &vec![0.0; 780]).unwrap();
    assert_eq!(scores[0].id, 5);
    let writer = OpenOptions::new().write(true).open(&object).unwrap();
    writer.write_at(&[1], 4700).unwrap();
    assert!(disk.score(&[5], &vec![0.0; 780]).is_err());
    fs::remove_dir_all(root).unwrap();
}
