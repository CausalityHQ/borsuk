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
    assert_eq!(generation_root["schema"], "borsuk-two-bit-generation-v5");
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
        10
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
    assert_eq!(remote.plan(&[1., 0.]).await.unwrap().ranges, plan.ranges);
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
