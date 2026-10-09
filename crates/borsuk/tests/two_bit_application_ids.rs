mod common;

use borsuk::{
    exact_sq8_nominee::Sq8Geometry,
    returned_sq8::{ReturnedRange, rank_returned_ranges},
    sq8_source::{build_sq8_source, build_sq8_source_with_ids},
    two_bit_build::TwoBitGenerationBuilder,
    two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits},
    two_bit_source::{TwoBitPlane, TwoBitSource},
    two_bit_store::{publish_two_bit_generation, read_two_bit_head},
};
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath,
};
use sha2::{Digest, Sha256};

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[tokio::test]
async fn application_ids_survive_ordered_build_publication_reload_and_ranking() {
    let temp = tempfile::tempdir().unwrap();
    let raw = temp.path().join("raw");
    let sq8 = temp.path().join("sq8");
    let body = (0..512)
        .flat_map(|i| if i % 2 == 0 { [1_f32, 0.] } else { [0., 1.] })
        .flat_map(f32::to_le_bytes)
        .collect::<Vec<_>>();
    std::fs::write(&raw, &body).unwrap();
    let raw_sha = hash(&body);
    let order = (0..512_u64).rev().collect::<Vec<_>>();
    let ids = (0..512)
        .map(|i| {
            if i % 2 == 0 {
                i64::MIN + i
            } else {
                i64::MAX - i
            }
        })
        .collect::<Vec<_>>();
    let rejected = temp.path().join("rejected-sq8");
    assert!(
        build_sq8_source_with_ids(&raw, &raw_sha, 2, &order, &[5; 512], &rejected, 1_000_000)
            .is_err()
    );
    assert!(!rejected.exists());
    assert!(build_sq8_source_with_ids(&raw, &raw_sha, 2, &order, &ids, &sq8, 0).is_err());
    assert!(!sq8.exists());
    let encoding =
        build_sq8_source_with_ids(&raw, &raw_sha, 2, &order, &ids, &sq8, 1_000_000).unwrap();
    let sq8_body = std::fs::read(&sq8).unwrap();
    for (physical, &ordinal) in order.iter().enumerate() {
        assert_eq!(
            i64::from_le_bytes(
                sq8_body[physical * 14..physical * 14 + 8]
                    .try_into()
                    .unwrap()
            ),
            ids[ordinal as usize]
        );
    }
    let source = TwoBitSource {
        raw: &raw,
        raw_sha256: &raw_sha,
        sq8: &sq8,
        sq8_sha256: &encoding.sha256,
        rows: 512,
        dimensions: 2,
    };
    assert!(
        source
            .build(&temp.path().join("implicit-id-order"), 1_000_000)
            .is_err()
    );
    assert!(
        source
            .build_with_order(&[0; 512], &temp.path().join("bad-order"), 1_000_000)
            .is_err()
    );
    assert!(!temp.path().join("bad-order/manifest.json").exists());
    let plane_path = temp.path().join("plane");
    let receipt = source
        .build_with_order(&order, &plane_path, 1_000_000)
        .unwrap();
    assert_eq!(receipt.schema, "borsuk-two-bit-plane-v3");
    assert_eq!(
        receipt.source_order_sha256,
        hash(
            &order
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<_>>()
        )
    );
    let ordinal_sq8 = temp.path().join("ordinal-sq8");
    let ordinal_encoding =
        build_sq8_source(&raw, &raw_sha, 2, &order, &ordinal_sq8, 1_000_000).unwrap();
    let ordinal_source = TwoBitSource {
        raw: &raw,
        raw_sha256: &raw_sha,
        sq8: &ordinal_sq8,
        sq8_sha256: &ordinal_encoding.sha256,
        rows: 512,
        dimensions: 2,
    };
    let control = ordinal_source
        .build(&temp.path().join("ordinal-plane"), 1_000_000)
        .unwrap();
    assert_eq!(receipt.source_order_sha256, control.source_order_sha256);
    assert_eq!(receipt.mean_sha256, control.mean_sha256);
    assert_eq!(receipt.records_sha256, control.records_sha256);
    let duplicate_sq8 = temp.path().join("duplicate-sq8");
    let mut duplicate_body = sq8_body.clone();
    duplicate_body[14..22].copy_from_slice(&sq8_body[..8]);
    std::fs::write(&duplicate_sq8, &duplicate_body).unwrap();
    let duplicate_sha = hash(&duplicate_body);
    let duplicate_source = TwoBitSource {
        raw: &raw,
        raw_sha256: &raw_sha,
        sq8: &duplicate_sq8,
        sq8_sha256: &duplicate_sha,
        rows: 512,
        dimensions: 2,
    };
    assert!(
        duplicate_source
            .build_with_order(&order, &temp.path().join("duplicate-plane"), 1_000_000)
            .is_err()
    );
    assert!(!temp.path().join("duplicate-plane/manifest.json").exists());
    let manifest = std::fs::read(plane_path.join("manifest.json")).unwrap();
    TwoBitPlane::open(&plane_path, &hash(&manifest), &encoding.sha256, 1_000_000).unwrap();
    let mut old: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    old["schema"] = "borsuk-two-bit-plane-v1".into();
    let old_body = serde_json::to_vec(&old).unwrap();
    std::fs::write(plane_path.join("manifest.json"), &old_body).unwrap();
    assert!(TwoBitPlane::open(&plane_path, &hash(&old_body), &encoding.sha256, 1_000_000).is_err());
    let store = std::sync::Arc::new(InMemory::new());
    let key = ObjectPath::from(format!("tenant/app/objects/{}", encoding.sha256));
    let put = store
        .put(&key, PutPayload::from(sq8_body.clone()))
        .await
        .unwrap();
    let etag = put.e_tag.unwrap();
    let builder = TwoBitGenerationBuilder {
        base_epoch: 0,
        source,
        generation: 1,
        low: &encoding.low,
        step: &encoding.step,
        sq8_object_key: key.as_ref(),
        sq8_etag: &etag,
    };
    let root = temp.path().join("generation");
    let root_sha = builder.build_with_order(&order, &root, 4_000_000).unwrap();
    let generation_root: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(generation_root["schema"], "borsuk-two-bit-generation-v8");
    let canonical_body = std::fs::read(root.join("canonical.bin")).unwrap();
    assert_eq!(canonical_body.len(), 512 * 16);
    for (physical, &ordinal) in order.iter().enumerate() {
        let row = &canonical_body[physical * 16..(physical + 1) * 16];
        assert_eq!(
            i64::from_le_bytes(row[..8].try_into().unwrap()),
            ids[ordinal as usize]
        );
        assert_eq!(
            &row[8..],
            &body[ordinal as usize * 8..(ordinal as usize + 1) * 8]
        );
    }
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
    let local = TwoBitGeneration::open(&root, &root_sha, limits).unwrap();
    // Admission must reject before any S3 request, even with a malformed query.
    let reader = borsuk::sq8_s3_range::OneAttemptS3::new("unused-fixture", "us-east-1").unwrap();
    assert!(matches!(
        local.search_excluding(&reader, &[], 1, &[i64::MIN]).await,
        Err(borsuk::two_bit_generation::TwoBitGenerationError::Invalid(
            "mutation roster or admission"
        ))
    ));
    let charged = TwoBitGeneration::open(
        &root,
        &root_sha,
        TwoBitGenerationLimits {
            already_pinned_bytes: 16,
            ..limits
        },
    )
    .unwrap();
    assert!(matches!(
        charged.search_excluding(&reader, &[], 1, &[10, 9]).await,
        Err(borsuk::two_bit_generation::TwoBitGenerationError::Invalid(
            "mutation roster or admission"
        ))
    ));
    let plan = local.plan(&[1., 0.]).await.unwrap();
    let prefix = ObjectPath::from("tenant/application-index");
    let mut bad_local_source = canonical_body.clone();
    bad_local_source[8] ^= 1;
    std::fs::write(root.join("canonical.bin"), &bad_local_source).unwrap();
    assert!(
        publish_two_bit_generation(store.as_ref(), &prefix, &root, &root_sha, limits, None)
            .await
            .is_err()
    );
    assert!(
        read_two_bit_head(store.as_ref(), &prefix)
            .await
            .unwrap()
            .is_none()
    );
    std::fs::write(root.join("canonical.bin"), &canonical_body).unwrap();
    let head = publish_two_bit_generation(store.as_ref(), &prefix, &root, &root_sha, limits, None)
        .await
        .unwrap();
    assert_eq!(
        read_two_bit_head(store.as_ref(), &prefix)
            .await
            .unwrap()
            .unwrap()
            .root_sha256(),
        root_sha
    );
    let recovered_source = temp.path().join("recovered-canonical");
    let source_stats = borsuk::canonical_source::recover_two_bit_source(
        store.as_ref(),
        &head,
        &recovered_source,
        10000,
        131072,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(&recovered_source).unwrap(), canonical_body);
    assert_eq!(source_stats.submitted_gets, 2);
    assert_eq!(
        source_stats.source_response_bytes,
        canonical_body.len() as u64
    );
    assert!(
        borsuk::canonical_source::recover_two_bit_source(
            store.as_ref(),
            &head,
            &recovered_source,
            10000,
            131072
        )
        .await
        .is_err()
    );
    let rejected_source = temp.path().join("rejected-canonical");
    let cap_error = borsuk::canonical_source::recover_two_bit_source(
        store.as_ref(),
        &head,
        &rejected_source,
        1,
        131072,
    )
    .await
    .unwrap_err();
    assert_eq!(cap_error.stats.submitted_gets, 1);
    assert!(!rejected_source.exists());
    let chunk_error = borsuk::canonical_source::recover_two_bit_source(
        store.as_ref(),
        &head,
        &rejected_source,
        10000,
        1,
    )
    .await
    .unwrap_err();
    assert_eq!(chunk_error.stats.submitted_gets, 2);
    assert!(!rejected_source.exists());
    let canonical_key =
        ObjectPath::from(generation_root["canonical"]["object_key"].as_str().unwrap());
    let mut corrupted = canonical_body.clone();
    corrupted[8] ^= 1;
    store
        .put(&canonical_key, PutPayload::from(corrupted))
        .await
        .unwrap();
    let failure = borsuk::canonical_source::recover_two_bit_source(
        store.as_ref(),
        &head,
        &rejected_source,
        10000,
        131072,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.stats.submitted_gets, 2);
    assert_eq!(
        failure.stats.source_response_bytes,
        canonical_body.len() as u64
    );
    assert!(!rejected_source.exists());
    store
        .put(&canonical_key, PutPayload::from(canonical_body.clone()))
        .await
        .unwrap();
    use borsuk::two_bit_mutations::{
        TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
    };
    let mutation_limits = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
    assert!(
        read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
            .await
            .unwrap()
            .is_none()
    );
    let first = apply_two_bit_mutations(
        store.as_ref(),
        &head,
        2,
        None,
        &[
            TwoBitMutation {
                id: i64::MIN,
                vector: None,
            },
            TwoBitMutation {
                id: 7,
                vector: Some(vec![3., 4.]),
            },
        ],
        mutation_limits,
    )
    .await
    .unwrap();
    assert_eq!(first.revision(), 1);
    assert_eq!(first.excluded_ids(), &[i64::MIN, 7]);
    let recovered = read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.sha256(), first.sha256());
    assert_eq!(recovered.rows(), first.rows());
    let other = publish_two_bit_generation(
        store.as_ref(),
        &ObjectPath::from("tenant/other-index"),
        &root,
        &root_sha,
        limits,
        None,
    )
    .await
    .unwrap();
    assert!(
        apply_two_bit_mutations(
            store.as_ref(),
            &other,
            2,
            Some(&first),
            &[TwoBitMutation {
                id: 7,
                vector: None
            },],
            mutation_limits
        )
        .await
        .is_err()
    );
    let lost_ack = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        store.clone(),
        1,
        |op, path| op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json"),
    );
    let acknowledged = apply_two_bit_mutations(
        &lost_ack,
        &other,
        2,
        None,
        &[TwoBitMutation {
            id: 7,
            vector: Some(vec![1., 0.]),
        }],
        mutation_limits,
    )
    .await
    .unwrap();
    assert_eq!(acknowledged.revision(), 1);
    let before_commit =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 1, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json")
        });
    assert!(
        apply_two_bit_mutations(
            &before_commit,
            &other,
            2,
            Some(&acknowledged),
            &[TwoBitMutation {
                id: 7,
                vector: None
            },],
            mutation_limits
        )
        .await
        .is_err()
    );
    assert_eq!(
        read_two_bit_mutations(store.as_ref(), &other, 2, mutation_limits)
            .await
            .unwrap()
            .unwrap()
            .sha256(),
        acknowledged.sha256()
    );
    // A concurrent no-op put has exactly the intended seal's immutable body.
    // Matching revision/SHA without a sealed head must never acknowledge a fence.
    let no_op = apply_two_bit_mutations(
        store.as_ref(),
        &other,
        2,
        Some(&acknowledged),
        &[TwoBitMutation {
            id: 7,
            vector: Some(vec![1., 0.]),
        }],
        mutation_limits,
    )
    .await
    .unwrap();
    assert!(!no_op.is_sealed());
    assert!(
        borsuk::two_bit_mutations::seal_two_bit_mutations(
            store.as_ref(),
            &other,
            Some(&acknowledged),
            mutation_limits,
        )
        .await
        .is_err()
    );
    let acknowledged = no_op;
    let lost_seal_ack = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        store.clone(),
        1,
        |op, path| op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json"),
    );
    let sealed_other = borsuk::two_bit_mutations::seal_two_bit_mutations(
        &lost_seal_ack,
        &other,
        Some(&acknowledged),
        mutation_limits,
    )
    .await
    .unwrap();
    assert!(sealed_other.is_sealed());
    assert_eq!(sealed_other.rows(), acknowledged.rows());
    let second = apply_two_bit_mutations(
        store.as_ref(),
        &head,
        2,
        Some(&recovered),
        &[
            TwoBitMutation {
                id: 7,
                vector: None,
            },
            TwoBitMutation {
                id: i64::MAX,
                vector: Some(vec![1., 0.]),
            },
        ],
        mutation_limits,
    )
    .await
    .unwrap();
    assert_eq!(second.revision(), 2);
    assert_eq!(second.excluded_ids(), &[i64::MIN, 7, i64::MAX]);
    assert!(second.rows()[1].vector.is_none());
    assert!(
        apply_two_bit_mutations(
            store.as_ref(),
            &head,
            2,
            Some(&first),
            &[TwoBitMutation {
                id: 20,
                vector: None
            },],
            mutation_limits
        )
        .await
        .is_err()
    );
    assert!(
        read_two_bit_mutations(store.as_ref(), &head, 3, mutation_limits)
            .await
            .is_err()
    );
    for invalid in [vec![0., 0.], vec![f32::NAN, 1.], vec![1.]] {
        assert!(
            apply_two_bit_mutations(
                store.as_ref(),
                &head,
                2,
                Some(&second),
                &[TwoBitMutation {
                    id: 1,
                    vector: Some(invalid)
                },],
                mutation_limits
            )
            .await
            .is_err()
        );
    }
    assert!(
        apply_two_bit_mutations(
            store.as_ref(),
            &head,
            2,
            Some(&second),
            &[
                TwoBitMutation {
                    id: 1,
                    vector: None
                },
                TwoBitMutation {
                    id: 1,
                    vector: None
                },
            ],
            mutation_limits
        )
        .await
        .is_err()
    );
    assert!(
        apply_two_bit_mutations(
            store.as_ref(),
            &head,
            2,
            Some(&second),
            &[TwoBitMutation {
                id: 1,
                vector: None
            },],
            TwoBitMutationLimits {
                max_snapshot_bytes: 100,
                ..mutation_limits
            }
        )
        .await
        .is_err()
    );
    assert_eq!(
        read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
            .await
            .unwrap()
            .unwrap()
            .sha256(),
        second.sha256()
    );
    let snapshot_key = head
        .metadata_prefix()
        .join("mutations")
        .join(second.sha256());
    let original_snapshot = store
        .get(&snapshot_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut corrupt_snapshot = original_snapshot.to_vec();
    *corrupt_snapshot.last_mut().unwrap() ^= 1;
    store
        .put(&snapshot_key, PutPayload::from(corrupt_snapshot))
        .await
        .unwrap();
    assert!(
        read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
            .await
            .is_err()
    );
    store
        .put(&snapshot_key, PutPayload::from(original_snapshot))
        .await
        .unwrap();
    let mutation_head_key = prefix.clone().join("head.json");
    let original_head = store
        .get(&mutation_head_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut legacy: serde_json::Value = serde_json::from_slice(&original_head).unwrap();
    legacy["schema"] = "borsuk-two-bit-head-v1".into();
    legacy.as_object_mut().unwrap().remove("epoch");
    store
        .put(
            &mutation_head_key,
            PutPayload::from(serde_json::to_vec(&legacy).unwrap()),
        )
        .await
        .unwrap();
    assert!(
        read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
            .await
            .is_err()
    );
    store
        .put(&mutation_head_key, PutPayload::from(original_head.clone()))
        .await
        .unwrap();
    let original_snapshot = store
        .get(&snapshot_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    // Hash each malformed body correctly: parser/binding checks must still fail.
    for case in 0..9 {
        let mut malformed = original_snapshot.to_vec();
        match case {
            0 => malformed[108] = 2,  // unknown operation
            1 => malformed[8] = b'x', // wrong base root
            2 => malformed[72..76].copy_from_slice(&3u32.to_le_bytes()),
            3 => malformed[76..84].copy_from_slice(&3u64.to_le_bytes()),
            4 => malformed[109..117].copy_from_slice(&i64::MIN.to_le_bytes()), // duplicate ID
            5 => {
                let len = malformed.len();
                malformed[len - 8..].fill(0);
            } // zero put
            6 => malformed[92..100].fill(0), // zero publication epoch
            7 => malformed[..8].copy_from_slice(b"BTMUT001"), // incompatible format
            _ => malformed.push(0),          // trailing data
        }
        let digest = hash(&malformed);
        store
            .put(
                &head
                    .metadata_prefix()
                    .join("mutations")
                    .join(digest.as_str()),
                PutPayload::from(malformed),
            )
            .await
            .unwrap();
        let mut forged_head: serde_json::Value = serde_json::from_slice(&original_head).unwrap();
        forged_head["mutation"]["sha256"] = digest.into();
        store
            .put(
                &mutation_head_key,
                PutPayload::from(serde_json::to_vec(&forged_head).unwrap()),
            )
            .await
            .unwrap();
        assert!(
            read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
                .await
                .is_err(),
            "malformed case {case}"
        );
    }
    store
        .put(&mutation_head_key, PutPayload::from(original_head))
        .await
        .unwrap();
    let restored = read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.sha256(), second.sha256());
    assert!(restored.resident_payload_bytes() <= mutation_limits.max_memory_bytes);
    assert!(
        read_two_bit_mutations(
            store.as_ref(),
            &head,
            2,
            TwoBitMutationLimits {
                max_memory_bytes: 1,
                ..mutation_limits
            }
        )
        .await
        .is_err()
    );
    let (metadata_store, metadata_ops) =
        common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
    let remote = TwoBitGeneration::open_remote(
        &metadata_store,
        &head.metadata_prefix(),
        &root_sha,
        limits,
        temp.path(),
    )
    .await
    .unwrap();
    assert_eq!(
        metadata_ops.count_matching(|op, _| op == common::StoreOperation::Get),
        9
    );
    assert_eq!(
        metadata_ops.count_matching(|op, path| op == common::StoreOperation::Get
            && path == head.metadata_prefix().join("diverse_graph.bin").as_ref()),
        1
    );
    assert_eq!(
        metadata_ops.count_matching(|op, path| op == common::StoreOperation::Get
            && (path == canonical_key.as_ref() || path == key.as_ref())),
        0
    );
    assert!(remote.plan(&[1., 0.]).await.is_err());
    assert_eq!(
        remote
            .plan_with_store(&metadata_store, &[1., 0.])
            .await
            .unwrap()
            .0,
        plan
    );
    let ranges = plan
        .ranges
        .iter()
        .map(|r| ReturnedRange {
            start: r.start,
            bytes: &sq8_body[r.start..r.end],
        })
        .collect::<Vec<_>>();
    let hits = rank_returned_ranges(
        Sq8Geometry {
            rows: 512,
            dimensions: 2,
        },
        &ranges,
        &[1., 0.],
        &encoding.low,
        &encoding.step,
        5,
        16384,
    )
    .unwrap();
    assert_eq!(
        hits.iter().map(|h| h.id).collect::<Vec<_>>(),
        ids.iter().step_by(2).take(5).copied().collect::<Vec<_>>()
    );
    let sealed = borsuk::two_bit_mutations::seal_two_bit_mutations(
        store.as_ref(),
        &head,
        Some(&restored),
        mutation_limits,
    )
    .await
    .unwrap();
    assert!(sealed.is_sealed());
    assert_eq!(sealed.rows(), restored.rows());
    assert_eq!(sealed.revision(), restored.revision() + 1);
    let idempotent = borsuk::two_bit_mutations::seal_two_bit_mutations(
        store.as_ref(),
        &head,
        Some(&sealed),
        mutation_limits,
    )
    .await
    .unwrap();
    assert_eq!(idempotent.sha256(), sealed.sha256());
    for expected in [Some(&restored), Some(&sealed), None] {
        assert!(
            apply_two_bit_mutations(
                store.as_ref(),
                &head,
                2,
                expected,
                &[TwoBitMutation {
                    id: 0,
                    vector: None
                }],
                mutation_limits
            )
            .await
            .is_err()
        );
    }
    let (closed_store, closed_ops) =
        common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
    assert!(
        apply_two_bit_mutations(
            &closed_store,
            &head,
            2,
            Some(&sealed),
            &[TwoBitMutation {
                id: 0,
                vector: None
            }],
            mutation_limits
        )
        .await
        .is_err()
    );
    assert_eq!(
        closed_ops.count_matching(|op, _| op == common::StoreOperation::Put),
        0
    );
    let recovered_seal = read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
        .await
        .unwrap()
        .unwrap();
    assert!(recovered_seal.is_sealed());
    assert_eq!(recovered_seal.rows(), restored.rows());
    let compacted_dir = temp.path().join("compaction-input");
    let compact_caps = borsuk::canonical_source::TwoBitCompactionLimits {
        max_memory_bytes: 1_000_000,
        max_disk_bytes: 1_000_000,
        max_source_chunk_bytes: 16384,
    };
    let prepared = borsuk::canonical_source::prepare_two_bit_compaction(
        store.as_ref(),
        &head,
        &sealed,
        &compacted_dir,
        compact_caps,
    )
    .await
    .unwrap();
    assert_eq!(prepared.dimensions, 2);
    assert_eq!(prepared.base_root_sha256, head.root_sha256());
    assert_eq!(prepared.mutation_sha256, sealed.sha256());
    let merged_raw = std::fs::read(compacted_dir.join("source.f32")).unwrap();
    let merged_ids_bytes = std::fs::read(compacted_dir.join("ids.i64")).unwrap();
    assert_eq!(hash(&merged_raw), prepared.raw_sha256);
    assert_eq!(hash(&merged_ids_bytes), prepared.ids_sha256);
    assert_eq!(prepared.recovery.submitted_gets, 2);
    assert_eq!(
        prepared.recovery.source_response_bytes,
        canonical_body.len() as u64
    );
    let merged_ids = merged_ids_bytes
        .chunks_exact(8)
        .map(|r| i64::from_le_bytes(r.try_into().unwrap()))
        .collect::<Vec<_>>();
    let physical_ids = canonical_body
        .chunks_exact(16)
        .map(|r| i64::from_le_bytes(r[..8].try_into().unwrap()))
        .collect::<Vec<_>>();
    let mut expected_ids = physical_ids
        .iter()
        .copied()
        .filter(|id| sealed.excluded_ids().binary_search(id).is_err())
        .collect::<Vec<_>>();
    expected_ids.extend(
        sealed
            .rows()
            .iter()
            .filter(|m| m.vector.is_some())
            .map(|m| m.id),
    );
    assert_eq!(merged_ids, expected_ids);
    assert_eq!(prepared.rows, expected_ids.len());
    assert_eq!(merged_raw.len(), prepared.rows * 8);
    for (physical, id) in physical_ids.iter().enumerate() {
        if sealed.excluded_ids().binary_search(id).is_ok() {
            continue;
        }
        let output_row = merged_ids.iter().position(|found| found == id).unwrap();
        assert_eq!(
            &merged_raw[output_row * 8..output_row * 8 + 8],
            &canonical_body[physical * 16 + 8..physical * 16 + 16]
        );
    }
    assert_eq!(
        &merged_raw[merged_raw.len() - 8..],
        &[1f32.to_le_bytes(), 0f32.to_le_bytes()].concat()
    );
    let on_disk: borsuk::canonical_source::TwoBitCompactionSource =
        serde_json::from_slice(&std::fs::read(compacted_dir.join("manifest.json")).unwrap())
            .unwrap();
    assert_eq!(on_disk.raw_sha256, prepared.raw_sha256);
    for (snapshot, caps, name) in [
        (&restored, compact_caps, "unsealed"),
        (&sealed_other, compact_caps, "wrong-namespace"),
        (
            &sealed,
            borsuk::canonical_source::TwoBitCompactionLimits {
                max_memory_bytes: 1,
                ..compact_caps
            },
            "memory",
        ),
        (
            &sealed,
            borsuk::canonical_source::TwoBitCompactionLimits {
                max_disk_bytes: 1,
                ..compact_caps
            },
            "disk",
        ),
    ] {
        let path = temp.path().join(format!("rejected-merge-{name}"));
        let failure = borsuk::canonical_source::prepare_two_bit_compaction(
            store.as_ref(),
            &head,
            snapshot,
            &path,
            caps,
        )
        .await
        .unwrap_err();
        assert_eq!(failure.stats.submitted_gets, 0);
        assert!(!path.exists());
    }
    assert!(
        borsuk::canonical_source::prepare_two_bit_compaction(
            store.as_ref(),
            &head,
            &sealed,
            &compacted_dir,
            compact_caps
        )
        .await
        .is_err()
    );
    let mut corrupt_canonical = canonical_body.to_vec();
    *corrupt_canonical.last_mut().unwrap() ^= 1;
    store
        .put(&canonical_key, PutPayload::from(corrupt_canonical))
        .await
        .unwrap();
    let rejected_merge = temp.path().join("rejected-corrupt-merge");
    let failure = borsuk::canonical_source::prepare_two_bit_compaction(
        store.as_ref(),
        &head,
        &sealed,
        &rejected_merge,
        compact_caps,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.stats.submitted_gets, 2);
    assert_eq!(
        failure.stats.source_response_bytes,
        canonical_body.len() as u64
    );
    assert!(!rejected_merge.exists());
    store
        .put(&canonical_key, PutPayload::from(canonical_body.clone()))
        .await
        .unwrap();

    let empty_prefix = ObjectPath::from("tenant/all-deleted-compaction");
    let empty_head = publish_two_bit_generation(
        store.as_ref(),
        &empty_prefix,
        &root,
        &root_sha,
        limits,
        None,
    )
    .await
    .unwrap();
    let mut deletes = ids
        .iter()
        .copied()
        .map(|id| TwoBitMutation { id, vector: None })
        .collect::<Vec<_>>();
    deletes.sort_by_key(|m| m.id);
    let deleted = apply_two_bit_mutations(
        store.as_ref(),
        &empty_head,
        2,
        None,
        &deletes,
        mutation_limits,
    )
    .await
    .unwrap();
    let deleted = borsuk::two_bit_mutations::seal_two_bit_mutations(
        store.as_ref(),
        &empty_head,
        Some(&deleted),
        mutation_limits,
    )
    .await
    .unwrap();
    let empty_dir = temp.path().join("empty-compaction-input");
    let empty = borsuk::canonical_source::prepare_two_bit_compaction(
        store.as_ref(),
        &empty_head,
        &deleted,
        &empty_dir,
        compact_caps,
    )
    .await
    .unwrap();
    assert_eq!(empty.rows, 0);
    assert!(
        std::fs::read(empty_dir.join("source.f32"))
            .unwrap()
            .is_empty()
    );
    assert!(std::fs::read(empty_dir.join("ids.i64")).unwrap().is_empty());

    let empty_lost_ack = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        store.clone(),
        1,
        |op, path| op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json"),
    );
    let empty_generation = borsuk::two_bit_store::publish_empty_two_bit_generation(
        &empty_lost_ack,
        &empty_prefix,
        2,
        2,
        Some(&empty_head),
    )
    .await
    .unwrap();
    assert!(empty_generation.is_empty());
    let reread_empty = read_two_bit_head(store.as_ref(), &empty_prefix)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reread_empty.root_sha256(), empty_generation.root_sha256());
    assert!(reread_empty.is_empty());
    let index = borsuk::two_bit_index::TwoBitIndex::open_remote(
        store.as_ref(),
        reread_empty,
        limits,
        temp.path(),
    )
    .await
    .unwrap();
    let hits = index.search(&reader, &[1., 0.], 100, None).await.unwrap();
    assert!(hits.candidates.is_empty());
    assert_eq!(hits.stats.submitted_gets, 0);
    let empty_root_key = index.head().metadata_prefix().join("manifest.json");
    let original_empty_root = store
        .get(&empty_root_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut bad_empty_root = original_empty_root.to_vec();
    *bad_empty_root.last_mut().unwrap() ^= 1;
    store
        .put(&empty_root_key, PutPayload::from(bad_empty_root))
        .await
        .unwrap();
    assert!(
        read_two_bit_head(store.as_ref(), &empty_prefix)
            .await
            .is_err()
    );
    store
        .put(&empty_root_key, PutPayload::from(original_empty_root))
        .await
        .unwrap();
    let pending = apply_two_bit_mutations(
        store.as_ref(),
        index.head(),
        2,
        None,
        &[
            TwoBitMutation {
                id: i64::MIN,
                vector: Some(vec![0., 3.]),
            },
            TwoBitMutation {
                id: i64::MAX,
                vector: Some(vec![3., 0.]),
            },
        ],
        mutation_limits,
    )
    .await
    .unwrap();
    let pinned_limits = borsuk::two_bit_generation::TwoBitGenerationLimits {
        already_pinned_bytes: pending.resident_payload_bytes() as u64,
        ..limits
    };
    let index = borsuk::two_bit_index::TwoBitIndex::open_remote(
        store.as_ref(),
        read_two_bit_head(store.as_ref(), &empty_prefix)
            .await
            .unwrap()
            .unwrap(),
        pinned_limits,
        temp.path(),
    )
    .await
    .unwrap();
    let hits = index
        .search(&reader, &[4., 0.], usize::MAX, Some(&pending))
        .await
        .unwrap();
    assert_eq!(
        hits.candidates.iter().map(|h| h.id).collect::<Vec<_>>(),
        [i64::MAX, i64::MIN]
    );
    assert_eq!(hits.stats.submitted_gets, 0);
    for query in [&[][..], &[0., 0.], &[f32::NAN, 0.]] {
        assert!(
            index
                .search(&reader, query, 100, Some(&pending))
                .await
                .is_err()
        );
    }
    assert!(index.search(&reader, &[1., 0.], 0, None).await.is_err());
    assert!(
        index
            .search(&reader, &[1., 0.], 100, Some(&sealed_other))
            .await
            .is_err()
    );
    let uncharged = borsuk::two_bit_index::TwoBitIndex::open_remote(
        store.as_ref(),
        read_two_bit_head(store.as_ref(), &empty_prefix)
            .await
            .unwrap()
            .unwrap(),
        limits,
        temp.path(),
    )
    .await
    .unwrap();
    assert!(
        uncharged
            .search(&reader, &[1., 0.], 100, Some(&pending))
            .await
            .is_err()
    );
    assert!(
        borsuk::two_bit_store::publish_empty_two_bit_generation(
            store.as_ref(),
            &empty_prefix,
            3,
            3,
            Some(index.head())
        )
        .await
        .is_err()
    );
    assert!(
        borsuk::two_bit_store::publish_empty_two_bit_generation(
            store.as_ref(),
            &empty_prefix,
            2,
            3,
            Some(index.head())
        )
        .await
        .is_err()
    ); // unsealed puts
    let sealed_pending = borsuk::two_bit_mutations::seal_two_bit_mutations(
        store.as_ref(),
        index.head(),
        Some(&pending),
        mutation_limits,
    )
    .await
    .unwrap();
    let from_empty_dir = temp.path().join("from-empty-compaction");
    let from_empty = borsuk::canonical_source::prepare_two_bit_compaction(
        store.as_ref(),
        index.head(),
        &sealed_pending,
        &from_empty_dir,
        compact_caps,
    )
    .await
    .unwrap();
    assert_eq!(from_empty.rows, 2);
    assert_eq!(from_empty.recovery.submitted_gets, 1);
    assert_eq!(from_empty.recovery.source_response_bytes, 0);
    assert_eq!(
        std::fs::read(from_empty_dir.join("ids.i64")).unwrap(),
        [i64::MIN.to_le_bytes(), i64::MAX.to_le_bytes()].concat()
    );

    // Use only the existing native APIs to rebuild and publish this merged corpus.
    let merged_order = (0..prepared.rows as u64).collect::<Vec<_>>();
    let merged_sq8 = temp.path().join("compacted.sq8");
    let encoding = build_sq8_source_with_ids(
        &compacted_dir.join("source.f32"),
        &prepared.raw_sha256,
        2,
        &merged_order,
        &merged_ids,
        &merged_sq8,
        1_000_000,
    )
    .unwrap();
    let merged_key_string = format!("tenant/objects/{}", encoding.sha256);
    let merged_key = ObjectPath::from(merged_key_string.clone());
    let merged_sq8_body = std::fs::read(&merged_sq8).unwrap();
    let merged_etag = store
        .put(&merged_key, PutPayload::from(merged_sq8_body.clone()))
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let next_root_dir = temp.path().join("compacted-generation");
    let next_source = TwoBitSource {
        raw: &compacted_dir.join("source.f32"),
        raw_sha256: &prepared.raw_sha256,
        sq8: &merged_sq8,
        sq8_sha256: &encoding.sha256,
        rows: prepared.rows,
        dimensions: 2,
    };
    let next_root_sha = TwoBitGenerationBuilder {
        base_epoch: read_two_bit_head(store.as_ref(), &prefix)
            .await
            .unwrap()
            .unwrap()
            .control_epoch(),
        source: next_source,
        generation: 2,
        low: &encoding.low,
        step: &encoding.step,
        sq8_object_key: &merged_key_string,
        sq8_etag: &merged_etag,
    }
    .build_with_order(&merged_order, &next_root_dir, 1_000_000)
    .unwrap();
    // A freshly captured publisher must reject earlier prepared identities before staging.
    let correct_root = std::fs::read(next_root_dir.join("manifest.json")).unwrap();
    for case in 0..2 {
        let mut stale: serde_json::Value = serde_json::from_slice(&correct_root).unwrap();
        let message = if case == 0 {
            stale["base_epoch"] = 0.into();
            "prepared generation epoch changed"
        } else {
            stale["sq8_object_key"] = format!(
                "{}/maintenance/{}/objects/{}",
                prefix.as_ref(),
                "0".repeat(48),
                encoding.sha256
            )
            .into();
            "maintenance owner epoch"
        };
        let body = serde_json::to_vec(&stale).unwrap();
        std::fs::write(next_root_dir.join("manifest.json"), &body).unwrap();
        let (guarded, operations) = common::FaultInjectingObjectStore::fail_nth_matching(
            store.clone(),
            usize::MAX,
            true,
            |_, _| false,
        )
        .with_operation_log();
        let error = publish_two_bit_generation(
            &guarded,
            &prefix,
            &next_root_dir,
            &hash(&body),
            limits,
            Some(&head),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, borsuk::two_bit_store::TwoBitStoreError::Invalid(m) if m == message)
        );
        assert!(
            operations
                .matching_paths(|op, _| op == common::StoreOperation::Put)
                .is_empty()
        );
    }
    std::fs::write(next_root_dir.join("manifest.json"), &correct_root).unwrap();
    let next_head = publish_two_bit_generation(
        store.as_ref(),
        &prefix,
        &next_root_dir,
        &next_root_sha,
        limits,
        Some(&head),
    )
    .await
    .unwrap();
    assert_eq!(next_head.generation(), 2);
    assert!(
        read_two_bit_mutations(store.as_ref(), &next_head, 2, mutation_limits)
            .await
            .unwrap()
            .is_none()
    );
    let new_canonical = std::fs::read(next_root_dir.join("canonical.bin")).unwrap();
    let new_ids = new_canonical
        .chunks_exact(16)
        .map(|r| i64::from_le_bytes(r[..8].try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(new_ids, merged_ids);
    let all = [ReturnedRange {
        start: 0,
        bytes: &merged_sq8_body,
    }];
    let hits = rank_returned_ranges(
        Sq8Geometry {
            rows: prepared.rows,
            dimensions: 2,
        },
        &all,
        &[1., 0.],
        &encoding.low,
        &encoding.step,
        prepared.rows,
        1_000_000,
    )
    .unwrap();
    let mut returned_ids = hits.iter().map(|h| h.id).collect::<Vec<_>>();
    returned_ids.sort_unstable();
    let mut expected_sorted = merged_ids.clone();
    expected_sorted.sort_unstable();
    assert_eq!(returned_ids, expected_sorted);
    assert!(!returned_ids.contains(&i64::MIN));
    assert!(!returned_ids.contains(&7));
    assert!(returned_ids.contains(&i64::MAX));
    assert!(
        read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
            .await
            .is_err()
    );
    let maintenance = temp.path().join("callable-maintenance");
    let options = borsuk::two_bit_compaction::TwoBitCompactionOptions {
        mutations: mutation_limits,
        source: borsuk::canonical_source::TwoBitCompactionLimits {
            max_memory_bytes: 4_000_000,
            max_disk_bytes: 4_000_000,
            ..compact_caps
        },
        generation: limits,
    };
    let (failing_store, retry_ops) =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 1, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json")
        })
        .with_operation_log();
    assert!(
        borsuk::two_bit_compaction::compact_two_bit_index(
            std::sync::Arc::new(failing_store),
            &empty_prefix,
            &maintenance,
            options,
        )
        .await
        .is_err()
    );
    assert_eq!(
        read_two_bit_head(store.as_ref(), &empty_prefix)
            .await
            .unwrap()
            .unwrap()
            .root_sha256(),
        index.head().root_sha256()
    );
    let failed_job = maintenance.join(index.head().root_sha256());
    assert!(failed_job.join("ready.json").exists());
    let target_manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(failed_job.join("generation/manifest.json")).unwrap(),
    )
    .unwrap();
    let sq8_key = target_manifest["sq8_object_key"].as_str().unwrap();
    let sq8_upload_paths = retry_ops
        .matching_paths(|op, path| op == common::StoreOperation::MultipartPut && path == sq8_key);
    assert_eq!(sq8_upload_paths.len(), 1);
    let sq8_etag = store
        .head(&ObjectPath::from(sq8_upload_paths[0].clone()))
        .await
        .unwrap()
        .e_tag;
    let saved_ready = std::fs::read(failed_job.join("ready.json")).unwrap();
    let mut corrupt_ready: serde_json::Value = serde_json::from_slice(&saved_ready).unwrap();
    corrupt_ready["job_sha256"] = "0".repeat(64).into();
    std::fs::write(
        failed_job.join("ready.json"),
        serde_json::to_vec(&corrupt_ready).unwrap(),
    )
    .unwrap();
    assert!(
        borsuk::two_bit_compaction::compact_two_bit_index(
            store.clone(),
            &empty_prefix,
            &maintenance,
            options
        )
        .await
        .is_err()
    );
    std::fs::write(failed_job.join("ready.json"), saved_ready).unwrap();
    let restored_base = borsuk::two_bit_compaction::compact_two_bit_index(
        store.clone(),
        &empty_prefix,
        &maintenance,
        options,
    )
    .await
    .unwrap();
    assert!(!restored_base.is_empty());
    assert_eq!(
        store
            .head(&ObjectPath::from(sq8_upload_paths[0].clone()))
            .await
            .unwrap()
            .e_tag,
        sq8_etag
    );
    assert!(!failed_job.exists());

    assert_eq!(restored_base.generation(), 3);
    let no_work = borsuk::two_bit_compaction::compact_two_bit_index(
        store.clone(),
        &empty_prefix,
        &maintenance,
        options,
    )
    .await
    .unwrap();
    assert_eq!(no_work.root_sha256(), restored_base.root_sha256());
    let live = borsuk::two_bit_index::TwoBitIndex::open_remote(
        store.as_ref(),
        read_two_bit_head(store.as_ref(), &empty_prefix)
            .await
            .unwrap()
            .unwrap(),
        limits,
        temp.path(),
    )
    .await
    .unwrap();
    let revived = std::fs::read(maintenance.join("index.json")).unwrap();
    assert!(!revived.is_empty());
    assert!(
        borsuk::two_bit_compaction::compact_two_bit_index(
            store.clone(),
            &prefix,
            &maintenance,
            options
        )
        .await
        .is_err()
    );
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(maintenance.join("compaction.lock"))
        .unwrap();
    lock_file.try_lock().unwrap();
    assert!(
        borsuk::two_bit_compaction::compact_two_bit_index(
            store.clone(),
            &empty_prefix,
            &maintenance,
            options
        )
        .await
        .is_err()
    );
    drop(lock_file);
    let deleted_again = apply_two_bit_mutations(
        store.as_ref(),
        live.head(),
        2,
        None,
        &[
            TwoBitMutation {
                id: i64::MIN,
                vector: None,
            },
            TwoBitMutation {
                id: i64::MAX,
                vector: None,
            },
        ],
        mutation_limits,
    )
    .await
    .unwrap();
    assert_eq!(deleted_again.rows().len(), 2);
    let emptied = borsuk::two_bit_compaction::compact_two_bit_index(
        store.clone(),
        &empty_prefix,
        &maintenance,
        options,
    )
    .await
    .unwrap();
    assert!(emptied.is_empty());
    assert_eq!(emptied.generation(), 4);
    let empty = borsuk::two_bit_index::TwoBitIndex::open_remote(
        store.as_ref(),
        emptied,
        limits,
        temp.path(),
    )
    .await
    .unwrap();
    assert!(
        empty
            .search(&reader, &[1., 0.], 100, None)
            .await
            .unwrap()
            .candidates
            .is_empty()
    );
    assert!(
        borsuk::two_bit_mutations::read_two_bit_mutations(
            store.as_ref(),
            live.head(),
            2,
            mutation_limits
        )
        .await
        .is_err()
    );
    let coordinated = borsuk::two_bit_index::TwoBitIndex::open_coordinated(
        store.as_ref(),
        &empty_prefix,
        &maintenance,
        limits,
        temp.path(),
    )
    .await
    .unwrap();
    let reclamation_lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(maintenance.join("lifecycle.lock"))
        .unwrap();
    assert!(reclamation_lock.try_lock().is_err());
    assert!(
        borsuk::two_bit_index::TwoBitIndex::open_coordinated(
            store.as_ref(),
            &prefix,
            &maintenance,
            limits,
            temp.path(),
        )
        .await
        .is_err()
    );
    drop(coordinated);
    reclamation_lock.try_lock().unwrap();
    assert!(
        borsuk::two_bit_index::TwoBitIndex::open_coordinated(
            store.as_ref(),
            &empty_prefix,
            &maintenance,
            limits,
            temp.path(),
        )
        .await
        .is_err()
    );
    assert!(
        borsuk::two_bit_compaction::compact_two_bit_index(
            store.clone(),
            &empty_prefix,
            &maintenance,
            options,
        )
        .await
        .is_err()
    );
    drop(reclamation_lock);
    let coordinated = borsuk::two_bit_index::TwoBitIndex::open_coordinated(
        store.as_ref(),
        &empty_prefix,
        &maintenance,
        limits,
        temp.path(),
    )
    .await
    .unwrap();
    assert!(
        borsuk::two_bit_compaction::compact_two_bit_index(
            store.clone(),
            &empty_prefix,
            &maintenance,
            options,
        )
        .await
        .is_ok()
    );
    drop(coordinated);
    // All old readers of this index are unregistered; retire them before GC.
    drop(live);
    drop(index);
    drop(empty);
    let base = read_two_bit_head(store.as_ref(), &empty_prefix)
        .await
        .unwrap()
        .unwrap();
    let caps = borsuk::two_bit_gc::TwoBitGcLimits {
        max_memory_bytes: 2_000_000,
        mutations: mutation_limits,
        max_objects_scanned: 10_000,
        max_objects_deleted: 10_000,
        max_deleted_object_bytes: 10_000_000,
    };
    let pending = apply_two_bit_mutations(
        store.as_ref(),
        &base,
        2,
        None,
        &[
            TwoBitMutation {
                id: 1,
                vector: Some(vec![1., 0.]),
            },
            TwoBitMutation {
                id: 2,
                vector: Some(vec![0., 1.]),
            },
        ],
        mutation_limits,
    )
    .await
    .unwrap();
    assert_eq!(pending.rows().len(), 2);
    // Valid modeled caps can still be too small for the actual pending body.
    let failure = borsuk::two_bit_gc::collect_two_bit_garbage(
        store.clone(),
        &empty_prefix,
        &maintenance,
        borsuk::two_bit_gc::TwoBitGcLimits {
            mutations: TwoBitMutationLimits {
                max_snapshot_bytes: 100,
                ..mutation_limits
            },
            ..caps
        },
    )
    .await
    .unwrap_err();
    assert_eq!(failure.report.delete_attempts, 0);
    assert!(
        read_two_bit_head(store.as_ref(), &empty_prefix)
            .await
            .is_ok(),
        "validation failure before deletes must release the fence"
    );
    // First PUT seals; use a lost generation attempt by rejecting the second head PUT.
    let failed_publish =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 2, true, |op, p| {
            op == common::StoreOperation::Put && p.as_ref().ends_with("/head.json")
        });
    assert!(
        borsuk::two_bit_compaction::compact_two_bit_index(
            std::sync::Arc::new(failed_publish),
            &empty_prefix,
            &maintenance,
            options
        )
        .await
        .is_err()
    );
    assert!(
        maintenance
            .join(base.root_sha256())
            .join("ready.json")
            .exists()
    );
    // A capped sweep can remove the claim before reaching its staged SQ8.
    let staged_manifest = maintenance
        .join(base.root_sha256())
        .join("generation/manifest.json");
    let staged: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&staged_manifest).unwrap()).unwrap();
    let old_sq8 = ObjectPath::from(staged["sq8_object_key"].as_str().unwrap());
    let owner = old_sq8.as_ref().rsplit_once("/objects/").unwrap().0;
    let claim = ObjectPath::from(format!("{owner}/claim.json"));
    for _ in 0..64 {
        if store.head(&claim).await.is_err() {
            break;
        }
        let report = borsuk::two_bit_gc::collect_two_bit_garbage(
            store.clone(),
            &empty_prefix,
            &maintenance,
            borsuk::two_bit_gc::TwoBitGcLimits {
                max_objects_deleted: 1,
                ..caps
            },
        )
        .await
        .unwrap();
        assert_eq!(report.objects_deleted, 1);
    }
    assert!(store.head(&claim).await.is_err());
    assert!(store.head(&old_sq8).await.is_ok());
    let failed_publish =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 1, true, |op, p| {
            op == common::StoreOperation::Put && p.as_ref().ends_with("/head.json")
        });
    assert!(
        borsuk::two_bit_compaction::compact_two_bit_index(
            std::sync::Arc::new(failed_publish),
            &empty_prefix,
            &maintenance,
            options,
        )
        .await
        .is_err()
    );
    let rebuilt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&staged_manifest).unwrap()).unwrap();
    let new_sq8 = rebuilt["sq8_object_key"].as_str().unwrap();
    assert_ne!(
        new_sq8,
        old_sq8.as_ref(),
        "missing claim must rebuild ready state"
    );
    let owner = new_sq8.rsplit_once("/objects/").unwrap().0;
    assert!(
        store
            .head(&ObjectPath::from(format!("{owner}/claim.json")))
            .await
            .is_ok()
    );
    let swept = borsuk::two_bit_gc::collect_two_bit_garbage(
        store.clone(),
        &empty_prefix,
        &maintenance,
        caps,
    )
    .await
    .unwrap();
    assert!(swept.objects_deleted > 0);
    let base = borsuk::two_bit_compaction::compact_two_bit_index(
        store.clone(),
        &empty_prefix,
        &maintenance,
        options,
    )
    .await
    .unwrap();
    let pending = apply_two_bit_mutations(
        store.as_ref(),
        &base,
        2,
        None,
        &[TwoBitMutation {
            id: 99,
            vector: Some(vec![1., 0.]),
        }],
        mutation_limits,
    )
    .await
    .unwrap();
    let orphan = empty_prefix
        .clone()
        .join("maintenance")
        .join("a".repeat(48))
        .join("objects")
        .join(hash(b"orphan"));
    store
        .put(&orphan, PutPayload::from_static(b"orphan"))
        .await
        .unwrap();
    let unknown = empty_prefix
        .clone()
        .join("maintenance")
        .join("a".repeat(48))
        .join("unknown");
    store
        .put(&unknown, PutPayload::from_static(b"keep"))
        .await
        .unwrap();
    let coordinated = borsuk::two_bit_index::TwoBitIndex::open_coordinated(
        store.as_ref(),
        &empty_prefix,
        &maintenance,
        limits,
        temp.path(),
    )
    .await
    .unwrap();
    assert!(
        borsuk::two_bit_gc::collect_two_bit_garbage(
            store.clone(),
            &empty_prefix,
            &maintenance,
            caps
        )
        .await
        .is_err()
    );
    drop(coordinated);
    let failing =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 2, true, |op, _| {
            op == common::StoreOperation::Delete
        });
    let failure = borsuk::two_bit_gc::collect_two_bit_garbage(
        std::sync::Arc::new(failing),
        &empty_prefix,
        &maintenance,
        caps,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.report.delete_attempts, 2);
    assert_eq!(failure.report.objects_deleted, 1);
    assert!(
        read_two_bit_head(store.as_ref(), &empty_prefix)
            .await
            .is_err()
    );
    let report = borsuk::two_bit_gc::collect_two_bit_garbage(
        store.clone(),
        &empty_prefix,
        &maintenance,
        caps,
    )
    .await
    .unwrap();
    assert!(report.scan_complete);
    assert!(report.objects_deleted > 0);
    assert!(store.head(&orphan).await.is_err());
    assert!(store.head(&unknown).await.is_ok());
    let base = read_two_bit_head(store.as_ref(), &empty_prefix)
        .await
        .unwrap()
        .unwrap();
    let recovered = read_two_bit_mutations(store.as_ref(), &base, 2, mutation_limits)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.rows(), pending.rows());
    borsuk::two_bit_index::TwoBitIndex::open_coordinated(
        store.as_ref(),
        &empty_prefix,
        &maintenance,
        limits,
        temp.path(),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_fence_rejects_delayed_mutation_and_generation_commits_after_release() {
    use borsuk::two_bit_mutations::{
        TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
        seal_two_bit_mutations,
    };
    use borsuk::two_bit_store::{
        begin_two_bit_write_fence, end_two_bit_write_fence, publish_empty_two_bit_generation,
    };
    use object_store::ObjectStore;
    use std::sync::{Arc, Barrier};
    let store = Arc::new(InMemory::new());
    let prefix = ObjectPath::from("fenced");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let caps = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
    let current = apply_two_bit_mutations(
        store.as_ref(),
        &base,
        2,
        None,
        &[TwoBitMutation {
            id: 1,
            vector: Some(vec![1., 0.]),
        }],
        caps,
    )
    .await
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let (paused, log) = common::FaultInjectingObjectStore::new(store.clone())
        .with_put_barrier(barrier.clone(), |op, path| {
            op == common::StoreOperation::Put && path.as_ref() == "fenced/head.json"
        })
        .with_operation_log();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(apply_two_bit_mutations(
                &paused,
                &base,
                2,
                Some(&current),
                &[TwoBitMutation {
                    id: 2,
                    vector: None,
                }],
                caps,
            ))
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while log.count_matching(|op, p| op == common::StoreOperation::Put && p == "fenced/head.json")
        == 0
    {
        assert!(std::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let lost_ack = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        store.clone(),
        1,
        |op, path| op == common::StoreOperation::Put && path.as_ref() == "fenced/head.json",
    );
    let fence = begin_two_bit_write_fence(&lost_ack, &prefix).await.unwrap();
    assert!(read_two_bit_head(store.as_ref(), &prefix).await.is_err());
    let resumed = begin_two_bit_write_fence(store.as_ref(), &prefix)
        .await
        .unwrap();
    assert_eq!(fence.id(), resumed.id());
    let staged =
        log.matching_paths(|op, p| op == common::StoreOperation::Put && p.contains("/mutations/"));
    assert_eq!(staged.len(), 1);
    store
        .delete(&ObjectPath::from(staged[0].clone()))
        .await
        .unwrap();
    let lost_exit_ack = common::FaultInjectingObjectStore::accept_then_fail_nth_put(
        store.clone(),
        1,
        |op, path| op == common::StoreOperation::Put && path.as_ref() == "fenced/head.json",
    );
    end_two_bit_write_fence(&lost_exit_ack, &fence)
        .await
        .unwrap();
    end_two_bit_write_fence(store.as_ref(), &resumed)
        .await
        .unwrap();
    let next_fence = begin_two_bit_write_fence(store.as_ref(), &prefix)
        .await
        .unwrap();
    assert!(
        end_two_bit_write_fence(store.as_ref(), &resumed)
            .await
            .is_err()
    );
    end_two_bit_write_fence(store.as_ref(), &next_fence)
        .await
        .unwrap();
    barrier.wait();
    assert!(worker.join().unwrap().is_err());
    let base = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .unwrap()
        .unwrap();
    let current = read_two_bit_mutations(store.as_ref(), &base, 2, caps)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.rows().len(), 1);
    seal_two_bit_mutations(store.as_ref(), &base, Some(&current), caps)
        .await
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let (paused, log) = common::FaultInjectingObjectStore::new(store.clone())
        .with_put_barrier(barrier.clone(), |op, path| {
            op == common::StoreOperation::Put && path.as_ref() == "fenced/head.json"
        })
        .with_operation_log();
    let target_prefix = prefix.clone();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(publish_empty_two_bit_generation(
                &paused,
                &target_prefix,
                2,
                2,
                Some(&base),
            ))
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while log.count_matching(|op, p| op == common::StoreOperation::Put && p == "fenced/head.json")
        == 0
    {
        assert!(std::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let fence = begin_two_bit_write_fence(store.as_ref(), &prefix)
        .await
        .unwrap();
    let staged = log
        .matching_paths(|op, p| op == common::StoreOperation::Put && p.ends_with("/manifest.json"));
    assert_eq!(staged.len(), 1);
    store
        .delete(&ObjectPath::from(staged[0].clone()))
        .await
        .unwrap();
    end_two_bit_write_fence(store.as_ref(), &fence)
        .await
        .unwrap();
    barrier.wait();
    assert!(worker.join().unwrap().is_err());
    assert_eq!(
        read_two_bit_head(store.as_ref(), &prefix)
            .await
            .unwrap()
            .unwrap()
            .generation(),
        1
    );
}

#[tokio::test]
#[ignore = "requires owned AWS test prefix and instance-role credentials"]
async fn native_s3_conditional_fence_and_gc_smoke() {
    use borsuk::two_bit_mutations::{
        TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
    };
    use borsuk::two_bit_store::{
        begin_two_bit_write_fence, end_two_bit_write_fence, publish_empty_two_bit_generation,
    };
    use futures_util::TryStreamExt;
    use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
    use std::sync::Arc;
    let bucket = std::env::var("BORSUK_NATIVE_TEST_BUCKET").unwrap();
    let region = std::env::var("BORSUK_NATIVE_TEST_REGION").unwrap();
    let prefix = ObjectPath::from(std::env::var("BORSUK_NATIVE_TEST_PREFIX").unwrap());
    assert!(
        prefix
            .as_ref()
            .starts_with("research/native-library-check/gc/")
    );
    let store = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&bucket)
            .with_region(&region)
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
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
    let head = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    apply_two_bit_mutations(
        store.as_ref(),
        &head,
        2,
        None,
        &[
            TwoBitMutation {
                id: 1,
                vector: Some(vec![1., 0.]),
            },
            TwoBitMutation {
                id: 2,
                vector: Some(vec![0., 1.]),
            },
        ],
        mutations,
    )
    .await
    .unwrap();
    let maintenance = temp.path().join("maintenance");
    let head = borsuk::two_bit_compaction::compact_two_bit_index(
        store.clone(),
        &prefix,
        &maintenance,
        borsuk::two_bit_compaction::TwoBitCompactionOptions {
            mutations,
            generation: limits,
            source: borsuk::canonical_source::TwoBitCompactionLimits {
                max_memory_bytes: 4_000_000,
                max_disk_bytes: 4_000_000,
                max_source_chunk_bytes: 16384,
            },
        },
    )
    .await
    .unwrap();
    apply_two_bit_mutations(
        store.as_ref(),
        &head,
        2,
        None,
        &[TwoBitMutation {
            id: 99,
            vector: Some(vec![1., 0.]),
        }],
        mutations,
    )
    .await
    .unwrap();
    let head_key = prefix.clone().join("head.json");
    let before = store.get(&head_key).await.unwrap();
    let version = UpdateVersion {
        e_tag: before.meta.e_tag.clone(),
        version: before.meta.version.clone(),
    };
    let old_bytes = before.bytes().await.unwrap();
    let fence = begin_two_bit_write_fence(store.as_ref(), &prefix)
        .await
        .unwrap();
    end_two_bit_write_fence(store.as_ref(), &fence)
        .await
        .unwrap();
    let after = store.get(&head_key).await.unwrap().bytes().await.unwrap();
    let old: serde_json::Value = serde_json::from_slice(&old_bytes).unwrap();
    let new: serde_json::Value = serde_json::from_slice(&after).unwrap();
    assert_eq!(
        new["epoch"].as_u64().unwrap(),
        old["epoch"].as_u64().unwrap() + 2
    );
    assert_ne!(old_bytes, after);
    assert!(matches!(
        store
            .put_opts(
                &head_key,
                PutPayload::from(old_bytes),
                PutOptions {
                    mode: PutMode::Update(version),
                    ..Default::default()
                }
            )
            .await,
        Err(object_store::Error::Precondition { .. })
    ));
    let orphan = prefix
        .clone()
        .join("maintenance")
        .join("a".repeat(48))
        .join("objects")
        .join(hash(b"orphan"));
    store
        .put(&orphan, PutPayload::from_static(b"orphan"))
        .await
        .unwrap();
    let report = borsuk::two_bit_gc::collect_two_bit_garbage(
        store.clone(),
        &prefix,
        &maintenance,
        borsuk::two_bit_gc::TwoBitGcLimits {
            max_memory_bytes: 2_000_000,
            mutations,
            max_objects_scanned: 1000,
            max_objects_deleted: 1000,
            max_deleted_object_bytes: 1_000_000,
        },
    )
    .await
    .unwrap();
    assert!(report.scan_complete && report.objects_deleted > 0);
    assert!(matches!(
        store.head(&orphan).await,
        Err(object_store::Error::NotFound { .. })
    ));
    let head = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .unwrap()
        .unwrap();
    let pending = read_two_bit_mutations(store.as_ref(), &head, 2, mutations)
        .await
        .unwrap()
        .unwrap();
    let index = borsuk::two_bit_index::TwoBitIndex::open_coordinated(
        store.as_ref(),
        &prefix,
        &maintenance,
        TwoBitGenerationLimits {
            already_pinned_bytes: pending.resident_payload_bytes() as u64,
            ..limits
        },
        temp.path(),
    )
    .await
    .unwrap();
    let reader = borsuk::sq8_s3_range::OneAttemptS3::new(&bucket, &region).unwrap();
    let result = index
        .search(&reader, &[1., 0.], 3, Some(&pending))
        .await
        .unwrap();
    let mut ids = result.candidates.iter().map(|h| h.id).collect::<Vec<_>>();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 99]);
    println!(
        "native_s3_gc_smoke bucket={bucket} prefix={prefix} generation={} deleted={} query_ids={ids:?}",
        index.head().generation(),
        report.objects_deleted
    );
    drop(index);
    let objects = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(objects.len() < 100);
    for object in objects {
        assert!(
            object
                .location
                .as_ref()
                .starts_with(&format!("{}/", prefix.as_ref()))
        );
        store.delete(&object.location).await.unwrap();
    }
    assert!(
        store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn gc_rejects_inconsistent_mutation_caps_before_remote_io() {
    use borsuk::two_bit_gc::{TwoBitGcLimits, collect_two_bit_garbage};
    let (store, operations) =
        common::FaultInjectingObjectStore::new(std::sync::Arc::new(InMemory::new()))
            .with_operation_log();
    let directory = tempfile::tempdir().unwrap();
    let error = collect_two_bit_garbage(
        std::sync::Arc::new(store),
        &ObjectPath::from("admission"),
        directory.path(),
        TwoBitGcLimits {
            max_memory_bytes: 2_000_000,
            mutations: borsuk::two_bit_mutations::TwoBitMutationLimits {
                max_snapshot_bytes: 100_000_000,
                max_memory_bytes: 1_000_000,
            },
            max_objects_scanned: 1,
            max_objects_deleted: 1,
            max_deleted_object_bytes: 1,
        },
    )
    .await
    .unwrap_err();
    assert!(
        operations.entries().is_empty(),
        "invalid payload caps must cause no remote IO: {error}"
    );
}

async fn semantic_compaction_fixture(
    store: std::sync::Arc<dyn ObjectStore>,
    dimensions: usize,
    interrupt_publication: u8,
    profile: borsuk::semantic_unit_router::SemanticProfile,
) {
    use borsuk::{
        canonical_source::{TwoBitCompactionLimits, recover_two_bit_source},
        rotated_two_bit::RotatedTwoBitCodec,
        semantic_unit_router::SemanticProfile,
        two_bit_compaction::{TwoBitCompactionOptions, compact_two_bit_index},
        two_bit_mutations::{
            TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
        },
    };
    let temp = tempfile::tempdir().unwrap();
    let vector = |components: &[(usize, f32)]| {
        let mut values = vec![0.; dimensions];
        for &(coordinate, value) in components {
            values[coordinate] = value;
        }
        values
    };
    let vectors = [
        vector(&[(0, 2.)]),
        vector(&[(dimensions - 1, 3.)]),
        vector(&[(0, -4.), (dimensions / 2, 3.)]),
        vector(&[(dimensions / 2, -7.), (dimensions - 1, 2.)]),
    ];
    let ids = [i64::MIN + 3, 71, -19, i64::MAX - 9];
    let order = [2, 0, 3, 1];
    let body = vectors
        .iter()
        .flat_map(|vector| {
            let norm = vector
                .iter()
                .map(|&v| f64::from(v).powi(2))
                .sum::<f64>()
                .sqrt();
            vector
                .iter()
                .flat_map(move |&v| ((f64::from(v) / norm) as f32).to_le_bytes())
        })
        .collect::<Vec<_>>();
    let raw = temp.path().join("raw");
    let sq8 = temp.path().join("sq8");
    std::fs::write(&raw, &body).unwrap();
    let raw_sha = hash(&body);
    let encoding =
        build_sq8_source_with_ids(&raw, &raw_sha, dimensions, &order, &ids, &sq8, 1_000_000)
            .unwrap();
    let key = ObjectPath::from(format!("semantic/objects/{}", encoding.sha256));
    let etag = store
        .put(&key, PutPayload::from(std::fs::read(&sq8).unwrap()))
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let root = temp.path().join("generation");
    let root_sha = TwoBitGenerationBuilder {
        source: TwoBitSource {
            raw: &raw,
            raw_sha256: &raw_sha,
            sq8: &sq8,
            sq8_sha256: &encoding.sha256,
            rows: ids.len(),
            dimensions,
        },
        generation: 1,
        base_epoch: 0,
        low: &encoding.low,
        step: &encoding.step,
        sq8_object_key: key.as_ref(),
        sq8_etag: &etag,
    }
    .build_with_semantic_profile(Some(&order), profile, &root, 32 * 1024 * 1024)
    .unwrap();
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: 32 * 1024 * 1024,
        max_active_queries: 1,
        max_query_bytes: ids.len() * (dimensions + 12),
        max_query_gets: 1,
        max_parallel_gets: 1,
        max_source_bytes: ids.len()
            * (RotatedTwoBitCodec::new(&vec![0.; dimensions], 42)
                .unwrap()
                .record_bytes()),
        max_source_gets: 1,
        max_parallel_source_gets: 1,
        // Ordinary search retains no diagnostic trace; charge the codec's exact peak.
        max_query_scratch_bytes: RotatedTwoBitCodec::required_query_scratch_bytes(dimensions)
            .unwrap(),
        already_pinned_bytes: 0,
    };
    let prefix = ObjectPath::from("semantic/index");
    let head = publish_two_bit_generation(store.as_ref(), &prefix, &root, &root_sha, limits, None)
        .await
        .unwrap();
    let mutation_limits = TwoBitMutationLimits {
        max_snapshot_bytes: 32768,
        max_memory_bytes: 1_000_000,
    };
    let updated = vector(&[(0, -4.), (dimensions - 1, -9.)]);
    let inserted = vector(&[(0, 5.), (dimensions / 2, 2.), (dimensions - 1, 1.)]);
    let mutations = apply_two_bit_mutations(
        store.as_ref(),
        &head,
        dimensions,
        None,
        &[
            TwoBitMutation {
                id: ids[0],
                vector: None,
            },
            TwoBitMutation {
                id: 71,
                vector: Some(updated.clone()),
            },
            TwoBitMutation {
                id: i64::MAX,
                vector: Some(inserted.clone()),
            },
        ],
        mutation_limits,
    )
    .await
    .unwrap();
    let expected_ids = vec![-19, 71, i64::MAX - 9, i64::MAX];
    let mutable = TwoBitGeneration::open(
        &root,
        &root_sha,
        TwoBitGenerationLimits {
            already_pinned_bytes: mutations.resident_payload_bytes() as u64,
            ..limits
        },
    )
    .unwrap();
    assert_eq!(mutable.semantic_profile(), Some(profile));
    let visible = mutable
        .search_with_mutations_store(store.as_ref(), &updated, 4, &mutations)
        .await
        .unwrap();
    let mut visible_ids = visible
        .candidates
        .iter()
        .map(|hit| hit.id)
        .collect::<Vec<_>>();
    visible_ids.sort_unstable();
    assert_eq!(visible_ids, expected_ids);
    assert_eq!(visible.mutation_rows_scanned, 3);
    assert_eq!(visible.mutation_put_rows_scored, 2);
    assert_eq!(visible.candidates[0].id, 71);
    drop(mutable);
    drop(mutations);
    drop(visible);
    let options = TwoBitCompactionOptions {
        mutations: mutation_limits,
        source: TwoBitCompactionLimits {
            max_memory_bytes: 32 * 1024 * 1024,
            max_disk_bytes: 4 * 1024 * 1024,
            max_source_chunk_bytes: 65536,
        },
        generation: limits,
    };
    let maintenance = temp.path().join("maintenance");
    let job = maintenance.join(&root_sha);
    let mut saved_ready = None;
    let mut saved_sq8 = None;
    for refused in [
        TwoBitCompactionOptions {
            generation: TwoBitGenerationLimits {
                already_pinned_bytes: options.source.max_memory_bytes as u64,
                ..limits
            },
            ..options
        },
        TwoBitCompactionOptions {
            source: TwoBitCompactionLimits {
                max_memory_bytes: 2 * 1024 * 1024,
                ..options.source
            },
            ..options
        },
        TwoBitCompactionOptions {
            source: TwoBitCompactionLimits {
                max_disk_bytes: 262144,
                ..options.source
            },
            ..options
        },
    ] {
        let (observed, operations) =
            common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
        assert!(
            compact_two_bit_index(
                std::sync::Arc::new(observed),
                &prefix,
                &maintenance,
                refused
            )
            .await
            .is_err()
        );
        assert!(!job.exists());
        assert!(operations.entries().iter().all(|entry| matches!(
            entry.operation,
            common::StoreOperation::Get | common::StoreOperation::Head
        )));
        assert!(
            operations
                .entries()
                .iter()
                .all(|entry| !entry.path.contains("/objects/"))
        );
    }
    if interrupt_publication > 0 {
        let (failing, attempts) = common::FaultInjectingObjectStore::fail_nth_matching(
            store.clone(),
            // The first head write seals the delta; the second publishes the base.
            if interrupt_publication == 1 { 2 } else { 1 },
            true,
            move |op, path| {
                match interrupt_publication {
                    1 => op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json"),
                    2 => {
                        op == common::StoreOperation::Put && path.as_ref().ends_with("/claim.json")
                    }
                    // Metadata publication streams the root via multipart PUT.
                    3 => {
                        op == common::StoreOperation::MultipartPut
                            && path.as_ref().contains("/generations/")
                            && path.as_ref().ends_with("/manifest.json")
                            && !path.as_ref().contains("/mutations/")
                    }
                    _ => unreachable!(),
                }
            },
        )
        .with_operation_log();
        let error =
            compact_two_bit_index(std::sync::Arc::new(failing), &prefix, &maintenance, options)
                .await
                .unwrap_err();
        let injected = if interrupt_publication == 3 {
            matches!(
                &error,
                borsuk::two_bit_store::TwoBitStoreError::Upload(
                    borsuk::resident_graph_store::ResidentGraphStoreError::Store(
                        object_store::Error::Generic {
                            store: "fault-injecting",
                            ..
                        }
                    )
                )
            )
        } else {
            matches!(
                &error,
                borsuk::two_bit_store::TwoBitStoreError::Store(object_store::Error::Generic {
                    store: "fault-injecting",
                    ..
                })
            )
        };
        assert!(
            injected,
            "unexpected phase{interrupt_publication} fault: {error}"
        );
        assert_eq!(
            attempts.count_matching(|op, path| op == common::StoreOperation::Put
                && path == format!("{prefix}/head.json")),
            if interrupt_publication == 1 { 2 } else { 1 }
        );
        assert_eq!(job.join("ready.json").exists(), interrupt_publication != 2);
        let job_body = std::fs::read(job.join("job.json")).unwrap();
        let job_value: serde_json::Value = serde_json::from_slice(&job_body).unwrap();
        let encoded_profile = serde_json::to_value(profile).unwrap();
        assert_eq!(job_value["base_profile"], encoded_profile);
        assert_eq!(job_value["profile"], encoded_profile);
        for field in ["profile", "base_profile"] {
            let mut missing = job_value.clone();
            missing.as_object_mut().unwrap().remove(field);
            std::fs::write(job.join("job.json"), serde_json::to_vec(&missing).unwrap()).unwrap();
            let error = compact_two_bit_index(store.clone(), &prefix, &maintenance, options)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                borsuk::two_bit_store::TwoBitStoreError::Invalid("compaction journal schema")
            ));
        }
        let mut changed = job_value.clone();
        changed["base_profile"] = serde_json::to_value(if profile == SemanticProfile::Scale1m {
            SemanticProfile::Native100k
        } else {
            SemanticProfile::Scale1m
        })
        .unwrap();
        std::fs::write(job.join("job.json"), serde_json::to_vec(&changed).unwrap()).unwrap();
        let error = compact_two_bit_index(store.clone(), &prefix, &maintenance, options)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            borsuk::two_bit_store::TwoBitStoreError::Invalid("compaction job changed")
        ));
        std::fs::write(job.join("job.json"), &job_body).unwrap();
        let other_profile = if profile == SemanticProfile::Scale1m {
            SemanticProfile::Native100k
        } else {
            SemanticProfile::Scale1m
        };
        if interrupt_publication == 2 {
            // Explicit profile replacement is permitted only before ready.
            // Fail after each preparation so this remains a captured unready job.
            for target in [other_profile, profile] {
                let failing = common::FaultInjectingObjectStore::fail_nth_matching(
                    store.clone(),
                    1,
                    true,
                    |op, path| {
                        op == common::StoreOperation::Put && path.as_ref().ends_with("/claim.json")
                    },
                );
                let error =
                    borsuk::two_bit_compaction::compact_two_bit_index_with_semantic_profile(
                        std::sync::Arc::new(failing),
                        &prefix,
                        &maintenance,
                        options,
                        target,
                    )
                    .await
                    .unwrap_err();
                assert!(
                    matches!(
                        error,
                        borsuk::two_bit_store::TwoBitStoreError::Store(
                            object_store::Error::Generic {
                                store: "fault-injecting",
                                ..
                            }
                        )
                    ),
                    "{error}"
                );
                assert!(!job.join("ready.json").exists());
                let captured: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(job.join("job.json")).unwrap()).unwrap();
                assert_eq!(captured["profile"], serde_json::to_value(target).unwrap());
                assert_eq!(captured["base_profile"], encoded_profile);
            }
        } else {
            let error = borsuk::two_bit_compaction::compact_two_bit_index_with_semantic_profile(
                store.clone(),
                &prefix,
                &maintenance,
                options,
                other_profile,
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                borsuk::two_bit_store::TwoBitStoreError::Invalid("compaction job changed")
            ));
        }
        if interrupt_publication == 2 {
            // Represent an interrupted builder after it allocated owned staging.
            std::fs::create_dir(job.join("generation")).unwrap();
            std::fs::write(job.join("generation/interrupted-build"), b"partial").unwrap();
            // Install durable crash residue directly, rather than relying on
            // ordinary error unwinding (which drops the preparation guard).
            let orphan = job.join("preparation");
            std::fs::create_dir(&orphan).unwrap();
            for name in ["canonical.bin", "source.f32", "ids.i64"] {
                let file = std::fs::File::create(orphan.join(name)).unwrap();
                file.set_len(options.source.max_disk_bytes).unwrap();
                file.sync_all().unwrap();
            }
            std::fs::File::open(&orphan).unwrap().sync_all().unwrap();
            std::fs::File::open(&job).unwrap().sync_all().unwrap();
            let unrelated = temp.path().join(".tmp-unrelated");
            std::fs::create_dir(&unrelated).unwrap();
            std::fs::write(unrelated.join("keep"), b"caller-owned").unwrap();
            for field in ["base_root_sha256", "mutation_sha256", "base_epoch"] {
                let mut changed = job_value.clone();
                changed[field] = if field == "base_epoch" {
                    (job_value[field].as_u64().unwrap() + 1).into()
                } else {
                    "0".repeat(64).into()
                };
                std::fs::write(job.join("job.json"), serde_json::to_vec(&changed).unwrap())
                    .unwrap();
                let error = compact_two_bit_index(store.clone(), &prefix, &maintenance, options)
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error,
                    borsuk::two_bit_store::TwoBitStoreError::Invalid("compaction job changed")
                ));
                assert!(orphan.join("canonical.bin").exists());
                assert!(job.join("generation/interrupted-build").exists());
            }
            std::fs::write(job.join("job.json"), &job_body).unwrap();
            let failing = common::FaultInjectingObjectStore::fail_nth_matching(
                store.clone(),
                1,
                true,
                |op, path| {
                    op == common::StoreOperation::Put && path.as_ref().ends_with("/claim.json")
                },
            );
            let error =
                compact_two_bit_index(std::sync::Arc::new(failing), &prefix, &maintenance, options)
                    .await
                    .unwrap_err();
            assert!(
                matches!(
                    error,
                    borsuk::two_bit_store::TwoBitStoreError::Store(object_store::Error::Generic {
                        store: "fault-injecting",
                        ..
                    })
                ),
                "{error}"
            );
            // Post-preparation failure proves reclamation happened before
            // allocating another admitted staging set, not final job removal.
            assert!(job.join("input/manifest.json").exists());
            assert!(!orphan.exists());
            assert!(!job.join("generation/interrupted-build").exists());
            assert_eq!(
                std::fs::read(unrelated.join("keep")).unwrap(),
                b"caller-owned"
            );
        }
        if interrupt_publication != 2 {
            let error = borsuk::two_bit_compaction::compact_two_bit_index_with_discovery(
                store.clone(),
                &prefix,
                &maintenance,
                options,
                Some(borsuk::two_bit_generation::DiscoveryMode::Graph),
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                borsuk::two_bit_store::TwoBitStoreError::Invalid("compaction job changed")
            ));
            let ready_body = std::fs::read(job.join("ready.json")).unwrap();
            saved_ready = Some(ready_body.clone());
            let root_path = job.join("generation/manifest.json");
            let root_body = std::fs::read(&root_path).unwrap();
            let mut target: serde_json::Value = serde_json::from_slice(&root_body).unwrap();
            let staged_key = ObjectPath::from(target["sq8_object_key"].as_str().unwrap());
            saved_sq8 = Some((
                staged_key.clone(),
                store.head(&staged_key).await.unwrap().e_tag,
            ));
            target["discovery"]["profile"] = changed["base_profile"].clone();
            let rewritten = serde_json::to_vec(&target).unwrap();
            let mut ready: serde_json::Value = serde_json::from_slice(&ready_body).unwrap();
            ready["target_root_sha256"] = hash(&rewritten).into();
            std::fs::write(&root_path, rewritten).unwrap();
            std::fs::write(job.join("ready.json"), serde_json::to_vec(&ready).unwrap()).unwrap();
            let error = compact_two_bit_index(store.clone(), &prefix, &maintenance, options)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                borsuk::two_bit_store::TwoBitStoreError::Invalid(
                    "compaction target mode/input/job"
                )
            ));
            std::fs::write(&root_path, root_body).unwrap();
            std::fs::write(job.join("ready.json"), ready_body).unwrap();
        }
        let unchanged = read_two_bit_head(store.as_ref(), &prefix)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.root_sha256(), root_sha);
        let sealed =
            read_two_bit_mutations(store.as_ref(), &unchanged, dimensions, mutation_limits)
                .await
                .unwrap()
                .unwrap();
        assert!(sealed.is_sealed());
        assert_eq!(sealed.rows().len(), 3);
        // Independently reload the old semantic base while its durable delta is sealed.
        let reopened = TwoBitGeneration::open_remote_from_head(
            store.as_ref(),
            &unchanged,
            TwoBitGenerationLimits {
                already_pinned_bytes: sealed.resident_payload_bytes() as u64,
                ..limits
            },
            temp.path(),
        )
        .await
        .unwrap();
        let visible = reopened
            .search_with_mutations_store(store.as_ref(), &updated, 4, &sealed)
            .await
            .unwrap();
        assert_eq!(visible.candidates[0].id, 71);
    }
    let (observed, operations) =
        common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
    let compacted = compact_two_bit_index(
        std::sync::Arc::new(observed),
        &prefix,
        &maintenance,
        options,
    )
    .await
    .unwrap();
    assert_eq!(compacted.generation(), 2);
    assert_ne!(compacted.root_sha256(), root_sha);
    assert!(!job.exists());
    if let Some(ready) = saved_ready {
        let ready: serde_json::Value = serde_json::from_slice(&ready).unwrap();
        assert_eq!(ready["target_root_sha256"], compacted.root_sha256());
        let (key, etag) = saved_sq8.unwrap();
        assert_eq!(store.head(&key).await.unwrap().e_tag, etag);
        assert_eq!(
            operations.count_matching(
                |op, path| op == common::StoreOperation::MultipartPut && path == key.as_ref()
            ),
            0
        );
        assert_eq!(
            operations.count_matching(
                |op, path| op == common::StoreOperation::Put && path.ends_with("/claim.json")
            ),
            0
        );
    }
    let authorized = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(authorized.root_sha256(), compacted.root_sha256());
    assert!(
        read_two_bit_mutations(store.as_ref(), &authorized, dimensions, mutation_limits)
            .await
            .unwrap()
            .is_none()
    );
    let reopened =
        TwoBitGeneration::open_remote_from_head(store.as_ref(), &authorized, limits, temp.path())
            .await
            .unwrap();
    assert_eq!(reopened.semantic_profile(), Some(profile));
    assert_eq!(reopened.rows(), 4);
    let canonical = temp.path().join("recovered-canonical");
    recover_two_bit_source(store.as_ref(), &authorized, &canonical, 65536, 65536)
        .await
        .unwrap();
    let canonical = std::fs::read(canonical).unwrap();
    assert_eq!(canonical.len(), 4 * (8 + dimensions * 4));
    let mut recovered_ids = Vec::new();
    for record in canonical.chunks_exact(8 + dimensions * 4) {
        let id = i64::from_le_bytes(record[..8].try_into().unwrap());
        recovered_ids.push(id);
        let expected = match id {
            -19 => &vectors[2],
            71 => &updated,
            id if id == i64::MAX - 9 => &vectors[3],
            i64::MAX => &inserted,
            _ => panic!("unexpected compacted ID {id}"),
        };
        let norm = expected
            .iter()
            .map(|&v| f64::from(v).powi(2))
            .sum::<f64>()
            .sqrt();
        for (bytes, &value) in record[8..].chunks_exact(4).zip(expected) {
            let actual = f32::from_le_bytes(bytes.try_into().unwrap());
            assert!(
                (actual - (f64::from(value) / norm) as f32).abs() <= 1e-7,
                "ID {id}: update/source not incorporated"
            );
        }
    }
    recovered_ids.sort_unstable();
    assert_eq!(recovered_ids, expected_ids);
    // Independent sequential scalar SQ8 oracle, including authenticated stored norms.
    let manifest = store
        .get(&authorized.metadata_prefix().join("manifest.json"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    let low: Vec<f32> = serde_json::from_value(manifest["low"].clone()).unwrap();
    let step: Vec<f32> = serde_json::from_value(manifest["step"].clone()).unwrap();
    let sq8 = store
        .get(&ObjectPath::from(
            manifest["sq8_object_key"].as_str().unwrap(),
        ))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let canonical_rows = canonical
        .chunks_exact(8 + dimensions * 4)
        .map(|record| {
            record[8..]
                .chunks_exact(4)
                .map(|bytes| f32::from_le_bytes(bytes.try_into().unwrap()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let spans = (0..dimensions)
        .map(|d| {
            let high = canonical_rows
                .iter()
                .map(|row| row[d])
                .fold(f32::NEG_INFINITY, f32::max);
            (high - low[d]).max(1e-12)
        })
        .collect::<Vec<_>>();
    for (values, record) in canonical_rows.iter().zip(sq8.chunks_exact(dimensions + 12)) {
        let mut norm = 0_f32;
        for d in 0..dimensions {
            let code = ((values[d] - low[d]) / spans[d] * 255.)
                .round_ties_even()
                .clamp(0., 255.) as u8;
            assert_eq!(record[12 + d], code);
            let decoded = low[d] + f32::from(code) * step[d];
            norm += decoded * decoded;
        }
        assert_eq!(&record[8..12], &norm.to_le_bytes());
    }
    for query in [&updated, &inserted] {
        let norm = query
            .iter()
            .map(|&v| f64::from(v).powi(2))
            .sum::<f64>()
            .sqrt();
        let normalized = query
            .iter()
            .map(|&v| (f64::from(v) / norm) as f32)
            .collect::<Vec<_>>();
        let mut shift = 0_f32;
        let mut qnorm = 0_f32;
        for d in 0..dimensions {
            shift += normalized[d] * low[d];
            qnorm += normalized[d] * normalized[d];
        }
        shift -= qnorm / 2.;
        let mut oracle = sq8
            .chunks_exact(dimensions + 12)
            .enumerate()
            .map(|(ordinal, row)| {
                let id = i64::from_le_bytes(row[..8].try_into().unwrap());
                let norm = f32::from_le_bytes(row[8..12].try_into().unwrap());
                let mut inner = 0_f32;
                for d in 0..dimensions {
                    inner += f32::from(row[12 + d]) * (normalized[d] * step[d]);
                }
                (ordinal, id, norm - 2. * (inner + shift))
            })
            .collect::<Vec<_>>();
        oracle.sort_by(|a, b| a.2.total_cmp(&b.2).then(a.1.cmp(&b.1)));
        let found = reopened
            .search_with_store(store.as_ref(), query, 4, None)
            .await
            .unwrap();
        assert_eq!(
            found
                .ranked
                .candidates
                .iter()
                .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
                .collect::<Vec<_>>(),
            oracle
                .iter()
                .map(|&(ordinal, id, score)| (ordinal, id, score.to_bits()))
                .collect::<Vec<_>>()
        );
        let top = reopened
            .search_with_store(store.as_ref(), query, 2, None)
            .await
            .unwrap();
        assert_eq!(
            top.ranked
                .candidates
                .iter()
                .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
                .collect::<Vec<_>>(),
            oracle
                .iter()
                .take(2)
                .map(|&(ordinal, id, score)| (ordinal, id, score.to_bits()))
                .collect::<Vec<_>>()
        );
        assert_eq!(found.ranked.candidates.len(), 4);
        assert!(found.ranked.candidates.iter().all(|hit| hit.id != ids[0]));
    }
    drop(reopened);
    let (observed, operations) =
        common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
    let no_work = compact_two_bit_index(
        std::sync::Arc::new(observed),
        &prefix,
        &maintenance,
        options,
    )
    .await
    .unwrap();
    assert_eq!(no_work.root_sha256(), compacted.root_sha256());
    assert_eq!(no_work.generation(), 2);
    assert!(operations.entries().iter().all(|entry| matches!(
        entry.operation,
        common::StoreOperation::Get | common::StoreOperation::Head
    )));
    if profile == SemanticProfile::Scale1m {
        apply_two_bit_mutations(
            store.as_ref(),
            &authorized,
            dimensions,
            None,
            &expected_ids
                .iter()
                .map(|&id| TwoBitMutation { id, vector: None })
                .collect::<Vec<_>>(),
            mutation_limits,
        )
        .await
        .unwrap();
        let empty = compact_two_bit_index(store.clone(), &prefix, &maintenance, options)
            .await
            .unwrap();
        assert!(empty.is_empty());
        let empty = read_two_bit_head(store.as_ref(), &prefix)
            .await
            .unwrap()
            .unwrap();
        let bytes = store
            .get(&empty.metadata_prefix().join("manifest.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schema"], "borsuk-two-bit-empty-generation-v4");
        assert_eq!(value["profile"], "scale1m");
        apply_two_bit_mutations(
            store.as_ref(),
            &empty,
            dimensions,
            None,
            &[TwoBitMutation {
                id: -777,
                vector: Some(inserted.clone()),
            }],
            mutation_limits,
        )
        .await
        .unwrap();
        let filled = compact_two_bit_index(store.clone(), &prefix, &maintenance, options)
            .await
            .unwrap();
        assert!(!filled.is_empty());
        let filled = read_two_bit_head(store.as_ref(), &prefix)
            .await
            .unwrap()
            .unwrap();
        let generation =
            TwoBitGeneration::open_remote_from_head(store.as_ref(), &filled, limits, temp.path())
                .await
                .unwrap();
        assert_eq!(generation.semantic_profile(), Some(profile));
        assert_eq!(generation.rows(), 1);
        let result = generation
            .search_with_store(store.as_ref(), &inserted, 1, None)
            .await
            .unwrap();
        assert_eq!(result.ranked.candidates[0].id, -777);
    }
}

#[tokio::test]
async fn semantic_d1024_compaction_reopens_with_application_ids_and_score_bits() {
    semantic_compaction_fixture(
        std::sync::Arc::new(InMemory::new()),
        1024,
        0,
        borsuk::semantic_unit_router::SemanticProfile::Native100k,
    )
    .await;
}

#[tokio::test]
async fn semantic_d1024_compaction_recovers_ready_publication_in_memory() {
    semantic_compaction_fixture(
        std::sync::Arc::new(InMemory::new()),
        1024,
        1,
        borsuk::semantic_unit_router::SemanticProfile::Native100k,
    )
    .await;
}

#[tokio::test]
async fn semantic_d768_compaction_reopens_with_application_ids_and_score_bits() {
    semantic_compaction_fixture(
        std::sync::Arc::new(InMemory::new()),
        768,
        0,
        borsuk::semantic_unit_router::SemanticProfile::Native100k,
    )
    .await;
}

#[tokio::test]
async fn semantic_d1025_compaction_refuses_before_side_effects() {
    use borsuk::{
        canonical_source::TwoBitCompactionLimits,
        rotated_two_bit::RotatedTwoBitCodec,
        two_bit_compaction::{TwoBitCompactionOptions, compact_two_bit_index_with_discovery},
        two_bit_generation::DiscoveryMode,
        two_bit_mutations::{
            TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
        },
        two_bit_store::{TwoBitStoreError, publish_empty_two_bit_generation},
    };
    let store = std::sync::Arc::new(InMemory::new());
    let prefix = ObjectPath::from("semantic-refusal/index");
    let head = publish_empty_two_bit_generation(store.as_ref(), &prefix, 1025, 1, None)
        .await
        .unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 32768,
        max_memory_bytes: 1_000_000,
    };
    let snapshot = apply_two_bit_mutations(
        store.as_ref(),
        &head,
        1025,
        None,
        &[TwoBitMutation {
            id: -71,
            vector: Some(vec![1.; 1025]),
        }],
        mutations,
    )
    .await
    .unwrap();
    let control = store
        .get(&prefix.clone().join("head.json"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let (observed, operations) =
        common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
    let temp = tempfile::tempdir().unwrap();
    let maintenance = temp.path().join("absent");
    let error = compact_two_bit_index_with_discovery(
        std::sync::Arc::new(observed),
        &prefix,
        &maintenance,
        TwoBitCompactionOptions {
            mutations,
            source: TwoBitCompactionLimits {
                max_memory_bytes: 32 * 1024 * 1024,
                max_disk_bytes: 4 * 1024 * 1024,
                max_source_chunk_bytes: 65536,
            },
            generation: TwoBitGenerationLimits {
                max_memory_bytes: 32 * 1024 * 1024,
                max_active_queries: 1,
                max_query_bytes: 65536,
                max_query_gets: 1,
                max_parallel_gets: 1,
                max_source_bytes: 65536,
                max_source_gets: 1,
                max_parallel_source_gets: 1,
                max_query_scratch_bytes: RotatedTwoBitCodec::required_query_scratch_bytes(1025)
                    .unwrap(),
                already_pinned_bytes: 0,
            },
        },
        Some(DiscoveryMode::Semantic),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        TwoBitStoreError::Invalid("semantic compaction dimensions")
    ));
    assert!(!maintenance.exists());
    assert!(operations.entries().iter().all(|entry| matches!(
        entry.operation,
        common::StoreOperation::Get | common::StoreOperation::Head
    )));
    assert_eq!(
        store
            .get(&prefix.join("head.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        control
    );
    let current = read_two_bit_mutations(store.as_ref(), &head, 1025, mutations)
        .await
        .unwrap()
        .unwrap();
    assert!(!current.is_sealed());
    assert_eq!(current.sha256(), snapshot.sha256());
    assert_eq!(current.revision(), snapshot.revision());
}

#[tokio::test]
async fn scale_profile_varied_dimensions_scalar_codes_norms_scores_topk_and_refill() {
    for dimensions in [1, 3, 255, 257, 1024] {
        semantic_compaction_fixture(
            std::sync::Arc::new(InMemory::new()),
            dimensions,
            0,
            borsuk::semantic_unit_router::SemanticProfile::Scale1m,
        )
        .await;
    }
}

#[tokio::test]
async fn scale_profile_recovers_build_ready_and_prehead_with_exact_profile() {
    for phase in [2, 3, 1] {
        semantic_compaction_fixture(
            std::sync::Arc::new(InMemory::new()),
            257,
            phase,
            borsuk::semantic_unit_router::SemanticProfile::Scale1m,
        )
        .await;
    }
}

#[tokio::test]
async fn scale_profile_explicit_empty_creation_and_missing_profile_authority_refuse() {
    use borsuk::{
        semantic_unit_router::SemanticProfile,
        two_bit_store::publish_empty_two_bit_generation_with_semantic_profile,
    };
    let store = InMemory::new();
    let prefix = ObjectPath::from("explicit-empty");
    for dimensions in [0, 1025] {
        assert!(
            publish_empty_two_bit_generation_with_semantic_profile(
                &store,
                &prefix,
                dimensions,
                1,
                None,
                SemanticProfile::Scale1m
            )
            .await
            .is_err()
        );
        assert!(read_two_bit_head(&store, &prefix).await.unwrap().is_none());
    }
    let head = publish_empty_two_bit_generation_with_semantic_profile(
        &store,
        &prefix,
        3,
        1,
        None,
        SemanticProfile::Scale1m,
    )
    .await
    .unwrap();
    assert!(head.is_empty());
    let head_path = prefix.clone().join("head.json");
    let control = store.get(&head_path).await.unwrap().bytes().await.unwrap();
    let root_body = store
        .get(&head.metadata_prefix().join("manifest.json"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let root: serde_json::Value = serde_json::from_slice(&root_body).unwrap();
    assert_eq!(root["profile"], "scale1m");
    for missing in [true, false] {
        let mut corrupt = root.clone();
        if missing {
            corrupt.as_object_mut().unwrap().remove("profile");
        } else {
            corrupt["profile"] = "fresh1m".into();
        }
        let body = serde_json::to_vec(&corrupt).unwrap();
        let digest = hash(&body);
        store
            .put(
                &prefix
                    .clone()
                    .join("generations")
                    .join(digest.as_str())
                    .join("manifest.json"),
                PutPayload::from(body),
            )
            .await
            .unwrap();
        let mut changed: serde_json::Value = serde_json::from_slice(&control).unwrap();
        changed["root_sha256"] = digest.into();
        store
            .put(
                &head_path,
                PutPayload::from(serde_json::to_vec(&changed).unwrap()),
            )
            .await
            .unwrap();
        assert!(read_two_bit_head(&store, &prefix).await.is_err());
    }
}

#[tokio::test]
async fn scale_profile_remaining_mutation_budget_refuses_before_body_read() {
    use borsuk::{
        canonical_source::TwoBitCompactionLimits,
        rotated_two_bit::RotatedTwoBitCodec,
        semantic_unit_router::SemanticProfile,
        two_bit_compaction::{TwoBitCompactionOptions, compact_two_bit_index},
        two_bit_mutations::{
            TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
        },
        two_bit_store::{TwoBitStoreError, publish_empty_two_bit_generation_with_semantic_profile},
    };
    let store = std::sync::Arc::new(InMemory::new());
    let prefix = ObjectPath::from("remaining-budget");
    let head = publish_empty_two_bit_generation_with_semantic_profile(
        store.as_ref(),
        &prefix,
        3,
        1,
        None,
        SemanticProfile::Scale1m,
    )
    .await
    .unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 32768,
        max_memory_bytes: 1_000_000,
    };
    drop(
        apply_two_bit_mutations(
            store.as_ref(),
            &head,
            3,
            None,
            &[TwoBitMutation {
                id: -777,
                vector: Some(vec![1., 0., 0.]),
            }],
            mutations,
        )
        .await
        .unwrap(),
    );
    let total = 1_000_000_usize;
    // Even the zero-input mutation model needs more than this remaining allowance.
    let remaining = 12 * mutations.max_snapshot_bytes + 4096 - 1;
    let pinned = total as u64 - head.retained_root_bytes() - 262144 - remaining as u64;
    assert!(pinned > 0 && pinned < total as u64);
    assert!(mutations.max_memory_bytes <= total && mutations.max_memory_bytes > remaining);
    assert_eq!(
        total as u64 - pinned - head.retained_root_bytes() - 262144,
        remaining as u64
    );
    let options = TwoBitCompactionOptions {
        mutations,
        source: TwoBitCompactionLimits {
            max_memory_bytes: total,
            max_disk_bytes: 1_000_000,
            max_source_chunk_bytes: 65536,
        },
        generation: TwoBitGenerationLimits {
            max_memory_bytes: total as u64,
            already_pinned_bytes: pinned,
            max_active_queries: 1,
            max_query_bytes: 65536,
            max_query_gets: 1,
            max_parallel_gets: 1,
            max_source_bytes: 65536,
            max_source_gets: 1,
            max_parallel_source_gets: 1,
            max_query_scratch_bytes: RotatedTwoBitCodec::required_query_scratch_bytes(3).unwrap(),
        },
    };
    let temp = tempfile::tempdir().unwrap();
    let maintenance = temp.path().join("maintenance");
    let (observed, operations) =
        common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
    let error = compact_two_bit_index(
        std::sync::Arc::new(observed),
        &prefix,
        &maintenance,
        options,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, TwoBitStoreError::Invalid("mutation payload cap")),
        "unexpected refusal: {error}"
    );
    assert_eq!(
        operations.count_matching(|_, path| path.contains("/mutations/")),
        0
    );
    assert!(operations.entries().iter().all(|entry| matches!(
        entry.operation,
        common::StoreOperation::Get | common::StoreOperation::Head
    )));
    assert!(!maintenance.join(head.root_sha256()).exists());
    assert!(
        !read_two_bit_mutations(store.as_ref(), &head, 3, mutations)
            .await
            .unwrap()
            .unwrap()
            .is_sealed()
    );
}

#[tokio::test]
async fn fresh_profile_target_refuses_before_seal_and_explicit_scale_transition_works() {
    use borsuk::{
        canonical_source::TwoBitCompactionLimits,
        rotated_two_bit::RotatedTwoBitCodec,
        semantic_unit_router::SemanticProfile,
        two_bit_compaction::{
            TwoBitCompactionOptions, compact_two_bit_index, compact_two_bit_index_with_discovery,
            compact_two_bit_index_with_semantic_profile,
        },
        two_bit_generation::DiscoveryMode,
        two_bit_mutations::{
            TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
        },
        two_bit_store::{
            TwoBitStoreError, publish_empty_two_bit_generation,
            publish_empty_two_bit_generation_with_semantic_profile,
        },
    };
    let store = std::sync::Arc::new(InMemory::new());
    let temp = tempfile::tempdir().unwrap();
    let prefix = ObjectPath::from("fresh-target");
    // A durable Fresh empty authority exercises inherited profile resolution
    // without constructing a million-row historical corpus.
    let root = serde_json::json!({
        "schema": "borsuk-two-bit-empty-generation-v4", "generation": 1,
        "dimensions": 768, "base_epoch": 0, "discovery": "semantic", "profile": "fresh1m"
    });
    let body = serde_json::to_vec(&root).unwrap();
    let digest = hash(&body);
    store
        .put(
            &prefix
                .clone()
                .join("generations")
                .join(digest.as_str())
                .join("manifest.json"),
            body.into(),
        )
        .await
        .unwrap();
    let control = serde_json::json!({
        "schema": "borsuk-two-bit-head-v2", "epoch": 1, "generation": 1,
        "root_sha256": digest, "mutation": null, "fence": null
    });
    store
        .put(
            &prefix.clone().join("head.json"),
            serde_json::to_vec(&control).unwrap().into(),
        )
        .await
        .unwrap();
    let head = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .unwrap()
        .unwrap();
    let mutation_limits = TwoBitMutationLimits {
        max_snapshot_bytes: 32768,
        max_memory_bytes: 1_000_000,
    };
    let mut vector = vec![0.; 768];
    vector[3] = 1.;
    let mut snapshot = apply_two_bit_mutations(
        store.as_ref(),
        &head,
        768,
        None,
        &[TwoBitMutation {
            id: -71,
            vector: Some(vector.clone()),
        }],
        mutation_limits,
    )
    .await
    .unwrap();
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: 32 * 1024 * 1024,
        max_active_queries: 1,
        max_query_bytes: 780,
        max_query_gets: 1,
        max_parallel_gets: 1,
        max_source_bytes: RotatedTwoBitCodec::new(&vec![0.; 768], 42)
            .unwrap()
            .record_bytes(),
        max_source_gets: 1,
        max_parallel_source_gets: 1,
        max_query_scratch_bytes: RotatedTwoBitCodec::required_query_scratch_bytes(768).unwrap(),
        already_pinned_bytes: 0,
    };
    let options = TwoBitCompactionOptions {
        mutations: mutation_limits,
        source: TwoBitCompactionLimits {
            max_memory_bytes: 32 * 1024 * 1024,
            max_disk_bytes: 4 * 1024 * 1024,
            max_source_chunk_bytes: 65536,
        },
        generation: limits,
    };
    let maintenance = temp.path().join("maintenance");
    for request in 0..3 {
        let (observed, operations) =
            common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
        let observed = std::sync::Arc::new(observed);
        let error = match request {
            0 => compact_two_bit_index(observed, &prefix, &maintenance, options).await,
            1 => {
                compact_two_bit_index_with_discovery(
                    observed,
                    &prefix,
                    &maintenance,
                    options,
                    Some(DiscoveryMode::Semantic),
                )
                .await
            }
            _ => {
                compact_two_bit_index_with_semantic_profile(
                    observed,
                    &prefix,
                    &maintenance,
                    options,
                    SemanticProfile::Fresh1m,
                )
                .await
            }
        }
        .unwrap_err();
        assert!(matches!(
            error,
            TwoBitStoreError::Invalid("Fresh1m maintenance is unsupported")
        ));
        assert!(!maintenance.exists());
        assert!(operations.entries().iter().all(|entry| matches!(
            entry.operation,
            common::StoreOperation::Get | common::StoreOperation::Head
        )));
        let writable = read_two_bit_mutations(store.as_ref(), &head, 768, mutation_limits)
            .await
            .unwrap()
            .unwrap();
        assert!(!writable.is_sealed());
        // Write another revision after each refusal, using the exact current snapshot.
        snapshot = apply_two_bit_mutations(
            store.as_ref(),
            &head,
            768,
            Some(&snapshot),
            &[TwoBitMutation {
                id: -71,
                vector: Some(vector.clone()),
            }],
            mutation_limits,
        )
        .await
        .unwrap();
    }
    for explicit in [false, true] {
        let (observed, operations) =
            common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
        let error = if explicit {
            publish_empty_two_bit_generation_with_semantic_profile(
                &observed,
                &prefix,
                768,
                2,
                Some(&head),
                SemanticProfile::Fresh1m,
            )
            .await
        } else {
            publish_empty_two_bit_generation(&observed, &prefix, 768, 2, Some(&head)).await
        }
        .unwrap_err();
        assert!(matches!(
            error,
            TwoBitStoreError::Invalid("Fresh1m maintenance is unsupported")
        ));
        assert!(operations.entries().iter().all(|entry| matches!(
            entry.operation,
            common::StoreOperation::Get | common::StoreOperation::Head
        )));
        assert!(
            !read_two_bit_mutations(store.as_ref(), &head, 768, mutation_limits,)
                .await
                .unwrap()
                .unwrap()
                .is_sealed()
        );
    }
    let (observed, operations) =
        common::FaultInjectingObjectStore::new(store.clone()).with_operation_log();
    let error = publish_empty_two_bit_generation_with_semantic_profile(
        &observed,
        &ObjectPath::from("wrong-namespace"),
        768,
        2,
        Some(&head),
        SemanticProfile::Scale1m,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        TwoBitStoreError::Invalid("empty generation namespace/order/dimensions")
    ));
    assert!(operations.entries().is_empty());
    assert!(
        publish_empty_two_bit_generation_with_semantic_profile(
            store.as_ref(),
            &ObjectPath::from("fresh-create"),
            768,
            1,
            None,
            SemanticProfile::Fresh1m,
        )
        .await
        .is_err()
    );
    assert!(
        read_two_bit_head(store.as_ref(), &ObjectPath::from("fresh-create"))
            .await
            .unwrap()
            .is_none()
    );
    drop(snapshot);
    let failing =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 1, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/claim.json")
        });
    let error = compact_two_bit_index_with_semantic_profile(
        std::sync::Arc::new(failing),
        &prefix,
        &maintenance,
        options,
        SemanticProfile::Scale1m,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            error,
            TwoBitStoreError::Store(object_store::Error::Generic {
                store: "fault-injecting",
                ..
            })
        ),
        "{error}"
    );
    let job = maintenance.join(head.root_sha256());
    let captured: serde_json::Value =
        serde_json::from_slice(&std::fs::read(job.join("job.json")).unwrap()).unwrap();
    assert_eq!(captured["base_profile"], "fresh1m");
    assert_eq!(captured["profile"], "scale1m");
    assert!(job.join("input/manifest.json").exists());
    assert!(!job.join("ready.json").exists());
    // Retry inherits the captured Scale target, not the Fresh base.
    let compacted = compact_two_bit_index(store.clone(), &prefix, &maintenance, options)
        .await
        .unwrap();
    let reopened =
        TwoBitGeneration::open_remote_from_head(store.as_ref(), &compacted, limits, temp.path())
            .await
            .unwrap();
    assert_eq!(reopened.semantic_profile(), Some(SemanticProfile::Scale1m));
    let result = reopened
        .search_with_store(store.as_ref(), &vector, 1, None)
        .await
        .unwrap();
    assert_eq!(result.ranked.candidates[0].id, -71);
    assert!(
        read_two_bit_mutations(store.as_ref(), &compacted, 768, mutation_limits)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn native_overpopulation_unready_job_recovers_with_explicit_scale_profile() {
    use borsuk::{
        canonical_source::TwoBitCompactionLimits,
        rotated_two_bit::RotatedTwoBitCodec,
        semantic_unit_router::SemanticProfile,
        two_bit_compaction::{
            TwoBitCompactionOptions, compact_two_bit_index,
            compact_two_bit_index_with_semantic_profile,
        },
        two_bit_mutations::{
            TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
        },
    };
    let rows = 100_000;
    let temp = tempfile::tempdir().unwrap();
    let raw = temp.path().join("raw");
    let sq8 = temp.path().join("sq8");
    let body = (0..rows)
        .flat_map(|row| {
            let angle = std::f64::consts::TAU * row as f64 / rows as f64;
            [angle.cos() as f32, angle.sin() as f32]
        })
        .flat_map(f32::to_le_bytes)
        .collect::<Vec<_>>();
    std::fs::write(&raw, &body).unwrap();
    let raw_sha = hash(&body);
    let ids = (0..rows).map(|row| -1 - row as i64).collect::<Vec<_>>();
    let order = (0..rows as u64).collect::<Vec<_>>();
    let encoding =
        build_sq8_source_with_ids(&raw, &raw_sha, 2, &order, &ids, &sq8, 128 * 1024 * 1024)
            .unwrap();
    let store = std::sync::Arc::new(InMemory::new());
    let key = ObjectPath::from(format!("overpopulation/objects/{}", encoding.sha256));
    let etag = store
        .put(&key, std::fs::read(&sq8).unwrap().into())
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let root = temp.path().join("generation");
    let root_sha = TwoBitGenerationBuilder {
        source: TwoBitSource {
            raw: &raw,
            raw_sha256: &raw_sha,
            sq8: &sq8,
            sq8_sha256: &encoding.sha256,
            rows,
            dimensions: 2,
        },
        generation: 1,
        base_epoch: 0,
        low: &encoding.low,
        step: &encoding.step,
        sq8_object_key: key.as_ref(),
        sq8_etag: &etag,
    }
    .build_with_semantic_profile(
        Some(&order),
        SemanticProfile::Native100k,
        &root,
        128 * 1024 * 1024,
    )
    .unwrap();
    drop(body);
    drop(ids);
    drop(order);
    // InMemory delivers the complete canonical object in one transport chunk.
    // Declare that fixture payload explicitly; production chunk caps stay enforced.
    let canonical_chunk_bytes =
        usize::try_from(std::fs::metadata(root.join("canonical.bin")).unwrap().len()).unwrap();
    assert_eq!(canonical_chunk_bytes, rows * 16);
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: 128 * 1024 * 1024,
        max_active_queries: 1,
        max_query_bytes: (rows + 1) * 14,
        max_query_gets: 512,
        max_parallel_gets: 1,
        max_source_bytes: 2 * 1024 * 1024,
        max_source_gets: 512,
        max_parallel_source_gets: 1,
        max_query_scratch_bytes: RotatedTwoBitCodec::required_query_scratch_bytes(2).unwrap(),
        already_pinned_bytes: 0,
    };
    let prefix = ObjectPath::from("overpopulation/index");
    let head = publish_two_bit_generation(store.as_ref(), &prefix, &root, &root_sha, limits, None)
        .await
        .unwrap();
    let mutation_limits = TwoBitMutationLimits {
        max_snapshot_bytes: 32768,
        max_memory_bytes: 1_000_000,
    };
    apply_two_bit_mutations(
        store.as_ref(),
        &head,
        2,
        None,
        &[TwoBitMutation {
            id: i64::MAX,
            vector: Some(vec![1., 0.]),
        }],
        mutation_limits,
    )
    .await
    .unwrap();
    let options = TwoBitCompactionOptions {
        mutations: mutation_limits,
        generation: limits,
        source: TwoBitCompactionLimits {
            max_memory_bytes: 128 * 1024 * 1024,
            max_disk_bytes: 64 * 1024 * 1024,
            max_source_chunk_bytes: canonical_chunk_bytes,
        },
    };
    let maintenance = temp.path().join("maintenance");
    let failed = compact_two_bit_index(store.clone(), &prefix, &maintenance, options)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &failed,
            borsuk::two_bit_store::TwoBitStoreError::Generation(
                borsuk::two_bit_generation::TwoBitGenerationError::Invalid("build inputs")
            )
        ),
        "{failed}"
    );
    let job = maintenance.join(head.root_sha256());
    assert!(job.join("input/manifest.json").exists(), "{failed}");
    assert!(!job.join("ready.json").exists());
    let prepared: serde_json::Value =
        serde_json::from_slice(&std::fs::read(job.join("input/manifest.json")).unwrap()).unwrap();
    assert_eq!(prepared["rows"], rows + 1);
    assert!(!SemanticProfile::Native100k.valid_geometry(rows + 1, 2));
    let captured: serde_json::Value =
        serde_json::from_slice(&std::fs::read(job.join("job.json")).unwrap()).unwrap();
    assert_eq!(captured["base_profile"], "native100k");
    assert_eq!(captured["profile"], "native100k");
    assert!(
        read_two_bit_mutations(store.as_ref(), &head, 2, mutation_limits)
            .await
            .unwrap()
            .unwrap()
            .is_sealed()
    );
    let compacted = compact_two_bit_index_with_semantic_profile(
        store.clone(),
        &prefix,
        &maintenance,
        options,
        SemanticProfile::Scale1m,
    )
    .await
    .unwrap();
    assert!(!job.exists());
    assert_eq!(compacted.generation(), 2);
    let manifest: serde_json::Value = serde_json::from_slice(
        &store
            .get(&compacted.metadata_prefix().join("manifest.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["canonical"]["rows"], rows + 1);
    assert_eq!(manifest["discovery"]["profile"], "scale1m");
    let reopened =
        TwoBitGeneration::open_remote_from_head(store.as_ref(), &compacted, limits, temp.path())
            .await
            .unwrap();
    assert_eq!(reopened.semantic_profile(), Some(SemanticProfile::Scale1m));
}
