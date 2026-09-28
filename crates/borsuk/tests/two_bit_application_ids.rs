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
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};
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
    assert_eq!(receipt.schema, "borsuk-two-bit-plane-v2");
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
    assert_eq!(generation_root["schema"], "borsuk-two-bit-generation-v2");
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
        |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("mutation-head.json")
        },
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
            op == common::StoreOperation::Put && path.as_ref().ends_with("mutation-head.json")
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
        |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("mutation-head.json")
        },
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
    let mutation_head_key = head.metadata_prefix().join("mutation-head.json");
    let original_head = store
        .get(&mutation_head_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut legacy: serde_json::Value = serde_json::from_slice(&original_head).unwrap();
    legacy["schema"] = "borsuk-two-bit-mutation-head-v1".into();
    legacy.as_object_mut().unwrap().remove("sealed");
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
    for case in 0..7 {
        let mut malformed = original_snapshot.to_vec();
        match case {
            0 => malformed[100] = 2,  // unknown operation
            1 => malformed[8] = b'x', // wrong base root
            2 => malformed[72..76].copy_from_slice(&3u32.to_le_bytes()),
            3 => malformed[76..84].copy_from_slice(&3u64.to_le_bytes()),
            4 => malformed[101..109].copy_from_slice(&i64::MIN.to_le_bytes()), // duplicate ID
            5 => {
                let len = malformed.len();
                malformed[len - 8..].fill(0);
            } // zero put
            _ => malformed.push(0),                                            // trailing data
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
        forged_head["sha256"] = digest.into();
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
        8
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
        source: next_source,
        generation: 2,
        low: &encoding.low,
        step: &encoding.step,
        sq8_object_key: &merged_key_string,
        sq8_etag: &merged_etag,
    }
    .build_with_order(&merged_order, &next_root_dir, 1_000_000)
    .unwrap();
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
            .unwrap()
            .unwrap()
            .is_sealed()
    );
}
