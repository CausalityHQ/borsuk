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
    let store = InMemory::new();
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
    let plan = local.plan(&[1., 0.]).await.unwrap();
    let prefix = ObjectPath::from("tenant/application-index");
    let head = publish_two_bit_generation(&store, &prefix, &root, &root_sha, limits, None)
        .await
        .unwrap();
    assert_eq!(
        read_two_bit_head(&store, &prefix)
            .await
            .unwrap()
            .unwrap()
            .root_sha256(),
        root_sha
    );
    let remote = TwoBitGeneration::open_remote(
        &store,
        &head.metadata_prefix(),
        &root_sha,
        limits,
        temp.path(),
    )
    .await
    .unwrap();
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
}
