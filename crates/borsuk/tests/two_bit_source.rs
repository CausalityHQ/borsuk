use borsuk::two_bit_source::{TwoBitPlane, TwoBitSource};
use sha2::{Digest, Sha256};
use std::fs;

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn authenticates_full_source_unit_and_partial_tail_on_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let raw = [1.0_f32, 2.0, -1.0, 0.5, 3.0]
        .repeat(33)
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect::<Vec<_>>();
    let sq8 = (0_i64..33)
        .flat_map(|id| {
            let mut bytes = id.to_le_bytes().to_vec();
            bytes.extend_from_slice(&1.0_f32.to_le_bytes());
            bytes.extend_from_slice(&[0; 5]);
            bytes
        })
        .collect::<Vec<_>>();
    let raw_path = directory.path().join("raw");
    let sq8_path = directory.path().join("sq8");
    fs::write(&raw_path, &raw).unwrap();
    fs::write(&sq8_path, &sq8).unwrap();
    let output = directory.path().join("plane");
    TwoBitSource {
        raw: &raw_path,
        raw_sha256: &hash(&raw),
        sq8: &sq8_path,
        sq8_sha256: &hash(&sq8),
        rows: 33,
        dimensions: 5,
    }
    .build(&output, 1_000_000)
    .unwrap();
    let records = fs::read(output.join("records.bin")).unwrap();
    let digests = fs::read(output.join("page_digests.bin")).unwrap();
    assert_eq!(digests.len(), 64);
    assert_eq!(&digests[..32], Sha256::digest(&records[..320]).as_slice());
    assert_eq!(&digests[32..], Sha256::digest(&records[320..]).as_slice());
    let body = fs::read(output.join("manifest.json")).unwrap();
    assert!(TwoBitPlane::open(&output, &hash(&body), &hash(&sq8), 1_000_000).is_ok());
    let mut wrong = digests;
    wrong[32] ^= 1;
    fs::write(output.join("page_digests.bin"), &wrong).unwrap();
    assert!(TwoBitPlane::open(&output, &hash(&body), &hash(&sq8), 1_000_000).is_err());
    let mut manifest: serde_json::Value = serde_json::from_slice(&body).unwrap();
    manifest["page_digest_sha256"] = hash(&wrong).into();
    let rebound = serde_json::to_vec(&manifest).unwrap();
    fs::write(output.join("manifest.json"), &rebound).unwrap();
    assert!(TwoBitPlane::open(&output, &hash(&rebound), &hash(&sq8), 1_000_000).is_err());
}

#[test]
fn authentication_backend_matches_sha256_known_answers() {
    assert_eq!(
        hash(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    let mut digest = Sha256::new();
    for _ in 0..1000 {
        digest.update([b'a'; 1000]);
    }
    assert_eq!(
        format!("{:x}", digest.finalize()),
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
    );
}

#[test]
fn streams_source_records_and_rejects_wrong_identity_order_budget_and_overwrite() {
    let directory = tempfile::tempdir().unwrap();
    let row = [1.0_f32, 2.0, -1.0, 0.5, 3.0];
    let raw = row
        .iter()
        .chain(&row)
        .flat_map(|v| v.to_le_bytes())
        .collect::<Vec<_>>();
    let sq8 = [1_i64, 0]
        .into_iter()
        .flat_map(|id| {
            let mut bytes = id.to_le_bytes().to_vec();
            bytes.extend_from_slice(&1.0_f32.to_le_bytes());
            bytes.extend_from_slice(&[0; 5]);
            bytes
        })
        .collect::<Vec<_>>();
    let raw_path = directory.path().join("raw");
    let sq8_path = directory.path().join("sq8");
    fs::write(&raw_path, &raw).unwrap();
    fs::write(&sq8_path, &sq8).unwrap();
    let raw_sha = hash(&raw);
    let sq8_sha = hash(&sq8);
    let input = TwoBitSource {
        raw: &raw_path,
        raw_sha256: &raw_sha,
        sq8: &sq8_path,
        sq8_sha256: &sq8_sha,
        rows: 2,
        dimensions: 5,
    };
    let output = directory.path().join("plane");
    assert!(input.build(&output, 1).is_err());
    assert!(!output.exists());
    let receipt = input.build(&output, 1024 * 1024).unwrap();
    assert_eq!(
        (receipt.rows, receipt.dimensions, receipt.record_bytes),
        (2, 5, 10)
    );
    let mean = fs::read(output.join("mean.bin")).unwrap();
    assert_eq!(mean, raw[..20]);
    let records = fs::read(output.join("records.bin")).unwrap();
    assert_eq!(records.len(), 20);
    for record in records.chunks_exact(10) {
        assert_eq!(&record[..2], &[0xaa, 0xaa]);
        assert_eq!(f32::from_le_bytes(record[2..6].try_into().unwrap()), 0.0);
        let inv = f32::from_le_bytes(record[6..].try_into().unwrap());
        assert!((inv - 1.0 / 15.25_f32.sqrt()).abs() < 1e-7);
    }
    assert_eq!(receipt.mean_sha256, hash(&mean));
    assert_eq!(receipt.records_sha256, hash(&records));
    let digests = fs::read(output.join("page_digests.bin")).unwrap();
    assert_eq!(digests, Sha256::digest(&records).to_vec());
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(output.join("manifest.json")).unwrap()).unwrap();
    assert!(!output.join("manifest.pending").exists());
    assert_eq!(manifest["query_or_truth_used"], false);
    assert_eq!(manifest["schema"], "borsuk-two-bit-plane-v3");
    assert_eq!(manifest["page_rows"], 32);
    assert_eq!(manifest["page_digest_sha256"], hash(&digests));
    assert_eq!(manifest["source_sha256"], raw_sha);
    assert!(input.build(&output, 1024 * 1024).is_err());
    let duplicate = directory.path().join("duplicate");
    let mut duplicate_sq8 = sq8.clone();
    duplicate_sq8[17..25].copy_from_slice(&1_i64.to_le_bytes());
    fs::write(&sq8_path, &duplicate_sq8).unwrap();
    let duplicate_hash = hash(&duplicate_sq8);
    let duplicate_input = TwoBitSource {
        sq8_sha256: &duplicate_hash,
        ..input
    };
    assert!(duplicate_input.build(&duplicate, 1024 * 1024).is_err());
    assert!(!duplicate.exists());
    fs::write(&sq8_path, &sq8).unwrap();
    let bad = directory.path().join("wrong-hash");
    let wrong = TwoBitSource {
        raw_sha256: &"0".repeat(64),
        ..input
    };
    assert!(wrong.build(&bad, 1024 * 1024).is_err());
    assert!(!bad.exists());
    let range_output = directory.path().join("out-of-range");
    let mut invalid_ids = sq8.clone();
    invalid_ids[..8].copy_from_slice(&2_i64.to_le_bytes());
    fs::write(&sq8_path, &invalid_ids).unwrap();
    let id_sha = hash(&invalid_ids);
    let invalid = TwoBitSource {
        sq8_sha256: &id_sha,
        ..input
    };
    assert!(invalid.build(&range_output, 1024 * 1024).is_err());
    assert!(!range_output.exists());
    fs::write(&sq8_path, &sq8).unwrap();
    let mut invalid_raw = raw.clone();
    invalid_raw[..4].copy_from_slice(&f32::NAN.to_le_bytes());
    fs::write(&raw_path, &invalid_raw).unwrap();
    let nan_sha = hash(&invalid_raw);
    let invalid = TwoBitSource {
        raw_sha256: &nan_sha,
        ..input
    };
    let nan_output = directory.path().join("nonfinite");
    assert!(invalid.build(&nan_output, 1024 * 1024).is_err());
    assert!(!nan_output.exists());

    // Distinct rows make an ignored physical permutation observable in CI.
    let varied_raw = row
        .iter()
        .copied()
        .chain(row.map(|value| -value))
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<_>>();
    fs::write(&raw_path, &varied_raw).unwrap();
    let varied_sha = hash(&varied_raw);
    let varied = TwoBitSource {
        raw_sha256: &varied_sha,
        ..input
    };
    let varied_output = directory.path().join("varied");
    varied.build(&varied_output, 1024 * 1024).unwrap();
    let records = fs::read(varied_output.join("records.bin")).unwrap();
    let codec = borsuk::rotated_two_bit::RotatedTwoBitCodec::new(&[0.0; 5], 20260923).unwrap();
    let prepared = codec.prepare_query(&row, 8192).unwrap();
    assert!(prepared.score(&records[..10]).unwrap() < -0.95);
    assert!(prepared.score(&records[10..]).unwrap() > 0.95);
}

#[test]
fn opens_authenticated_plane_and_rejects_wrong_generation_corruption_and_budget() {
    let dir = tempfile::tempdir().unwrap();
    let raw = [1_f32, 2., -1., -2.]
        .into_iter()
        .flat_map(f32::to_le_bytes)
        .collect::<Vec<_>>();
    let sq8 = [0_i64, 1]
        .into_iter()
        .flat_map(|id| {
            let mut record = id.to_le_bytes().to_vec();
            record.extend_from_slice(&1_f32.to_le_bytes());
            record.extend_from_slice(&[0, 0]);
            record
        })
        .collect::<Vec<_>>();
    let raw_path = dir.path().join("raw");
    let sq8_path = dir.path().join("sq8");
    fs::write(&raw_path, &raw).unwrap();
    fs::write(&sq8_path, &sq8).unwrap();
    let root = dir.path().join("plane");
    let sq8_sha = hash(&sq8);
    TwoBitSource {
        raw: &raw_path,
        raw_sha256: &hash(&raw),
        sq8: &sq8_path,
        sq8_sha256: &sq8_sha,
        rows: 2,
        dimensions: 2,
    }
    .build(&root, 1024 * 1024)
    .unwrap();
    let manifest = fs::read(root.join("manifest.json")).unwrap();
    let trusted = hash(&manifest);
    let plane = TwoBitPlane::open(&root, &trusted, &sq8_sha, 1024 * 1024).unwrap();
    assert_eq!(plane.receipt().rows, 2);
    let query = plane.prepare_query(&[1., 2.], 8192).unwrap();
    assert!(query.score(plane.record(0).unwrap()).unwrap() > 0.95);
    assert!(query.score(plane.record(1).unwrap()).unwrap() < -0.95);
    assert!(plane.record(2).is_none());
    assert!(TwoBitPlane::open(&root, &trusted, &sq8_sha, 1).is_err());
    assert!(TwoBitPlane::open(&root, &"0".repeat(64), &sq8_sha, 1024 * 1024).is_err());
    assert!(TwoBitPlane::open(&root, &trusted, &"0".repeat(64), 1024 * 1024).is_err());
    for name in ["mean.bin", "records.bin"] {
        let original = fs::read(root.join(name)).unwrap();
        let mut corrupt = original.clone();
        corrupt[0] ^= 1;
        fs::write(root.join(name), corrupt).unwrap();
        assert!(TwoBitPlane::open(&root, &trusted, &sq8_sha, 1024 * 1024).is_err());
        fs::write(root.join(name), original).unwrap();
    }
    let mut unsupported: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    unsupported["schema"] = "legacy".into();
    let body = serde_json::to_vec(&unsupported).unwrap();
    fs::write(root.join("manifest.json"), &body).unwrap();
    assert!(TwoBitPlane::open(&root, &hash(&body), &sq8_sha, 1024 * 1024).is_err());
}
