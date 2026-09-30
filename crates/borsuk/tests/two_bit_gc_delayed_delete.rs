mod common;

use borsuk::{
    two_bit_gc::{TwoBitGcLimits, collect_two_bit_garbage},
    two_bit_mutations::{
        TwoBitMutation, TwoBitMutationLimits, apply_two_bit_mutations, read_two_bit_mutations,
    },
    two_bit_store::{publish_empty_two_bit_generation, read_two_bit_head},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

#[tokio::test]
async fn acknowledged_mutation_survives_an_earlier_ambiguous_delete() {
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("late-delete");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
    let batch = [TwoBitMutation {
        id: 7,
        vector: Some(vec![1., 0.]),
    }];
    let (failed_writer, writes) =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 1, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json")
        })
        .with_operation_log();
    assert!(
        apply_two_bit_mutations(&failed_writer, &base, 2, None, &batch, mutations)
            .await
            .is_err()
    );
    let staged = writes.matching_paths(|op, path| {
        op == common::StoreOperation::Put && path.contains("/mutations/")
    });
    assert_eq!(staged.len(), 1);
    let orphan = Path::from(staged[0].clone());
    let directory = tempfile::tempdir().unwrap();
    let caps = TwoBitGcLimits {
        max_memory_bytes: 2_000_000,
        mutations,
        max_objects_scanned: 100,
        max_objects_deleted: 100,
        max_deleted_object_bytes: 1_000_000,
    };
    let target = orphan.clone();
    let failed_delete = common::FaultInjectingObjectStore::fail_nth_matching(
        store.clone(),
        1,
        true,
        move |op, path| op == common::StoreOperation::Delete && *path == target,
    );
    let failure = collect_two_bit_garbage(Arc::new(failed_delete), &prefix, directory.path(), caps)
        .await
        .unwrap_err();
    assert_eq!(failure.report.delete_attempts, 1);
    assert!(store.head(&orphan).await.is_ok());
    collect_two_bit_garbage(store.clone(), &prefix, directory.path(), caps)
        .await
        .unwrap();
    assert!(store.head(&orphan).await.is_err());
    let base = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .unwrap()
        .unwrap();
    let acknowledged = apply_two_bit_mutations(store.as_ref(), &base, 2, None, &batch, mutations)
        .await
        .unwrap();
    // Model the original DELETE reaching storage after its error and the successful retry.
    // An HTTP error cannot establish that the remote operation was cancelled.
    store.delete(&orphan).await.unwrap();
    let recovered = read_two_bit_mutations(store.as_ref(), &base, 2, mutations)
        .await
        .expect("a delayed orphan DELETE must not destroy an acknowledged mutation")
        .unwrap();
    assert_eq!(recovered.rows(), acknowledged.rows());
}

#[tokio::test]
async fn acknowledged_empty_generation_survives_an_earlier_ambiguous_delete() {
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("late-generation-delete");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
    borsuk::two_bit_mutations::seal_two_bit_mutations(store.as_ref(), &base, None, mutations)
        .await
        .unwrap();
    let (failed_writer, writes) =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 1, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json")
        })
        .with_operation_log();
    assert!(
        publish_empty_two_bit_generation(&failed_writer, &prefix, 2, 2, Some(&base))
            .await
            .is_err()
    );
    let staged = writes.matching_paths(|op, path| {
        op == common::StoreOperation::Put && path.ends_with("/manifest.json")
    });
    assert_eq!(staged.len(), 1);
    let orphan = Path::from(staged[0].clone());
    let directory = tempfile::tempdir().unwrap();
    let caps = TwoBitGcLimits {
        max_memory_bytes: 2_000_000,
        mutations,
        max_objects_scanned: 100,
        max_objects_deleted: 100,
        max_deleted_object_bytes: 1_000_000,
    };
    let target = orphan.clone();
    let failed_delete = common::FaultInjectingObjectStore::fail_nth_matching(
        store.clone(),
        1,
        true,
        move |op, path| op == common::StoreOperation::Delete && *path == target,
    );
    let failure = collect_two_bit_garbage(Arc::new(failed_delete), &prefix, directory.path(), caps)
        .await
        .unwrap_err();
    assert_eq!(failure.report.delete_attempts, 1);
    assert!(store.head(&orphan).await.is_ok());
    collect_two_bit_garbage(store.clone(), &prefix, directory.path(), caps)
        .await
        .unwrap();
    assert!(store.head(&orphan).await.is_err());
    let base = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .unwrap()
        .unwrap();
    let acknowledged = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 2, Some(&base))
        .await
        .unwrap();
    store.delete(&orphan).await.unwrap();
    let recovered = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .expect("a delayed orphan DELETE must not destroy an acknowledged generation")
        .unwrap();
    assert_eq!(recovered.root_sha256(), acknowledged.root_sha256());
}

#[tokio::test]
async fn acknowledged_compaction_survives_an_earlier_ambiguous_metadata_delete() {
    use borsuk::{
        canonical_source::TwoBitCompactionLimits,
        two_bit_compaction::{TwoBitCompactionOptions, compact_two_bit_index},
        two_bit_generation::TwoBitGenerationLimits,
    };
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("late-compaction-delete");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let mutations = TwoBitMutationLimits {
        max_snapshot_bytes: 16384,
        max_memory_bytes: 1_000_000,
    };
    apply_two_bit_mutations(
        store.as_ref(),
        &base,
        2,
        None,
        &[TwoBitMutation {
            id: 7,
            vector: Some(vec![1., 0.]),
        }],
        mutations,
    )
    .await
    .unwrap();
    let options = TwoBitCompactionOptions {
        mutations,
        source: TwoBitCompactionLimits {
            max_memory_bytes: 4_000_000,
            max_disk_bytes: 4_000_000,
            max_source_chunk_bytes: 16384,
        },
        generation: TwoBitGenerationLimits {
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
        },
    };
    let directory = tempfile::tempdir().unwrap();
    let (failed_writer, writes) =
        common::FaultInjectingObjectStore::fail_nth_matching(store.clone(), 2, true, |op, path| {
            op == common::StoreOperation::Put && path.as_ref().ends_with("/head.json")
        })
        .with_operation_log();
    let error = compact_two_bit_index(Arc::new(failed_writer), &prefix, directory.path(), options)
        .await
        .unwrap_err();
    assert!(
        matches!(error, borsuk::two_bit_store::TwoBitStoreError::Store(_)),
        "{error:?}"
    );
    let staged = writes.matching_paths(|op, path| {
        op == common::StoreOperation::MultipartPut
            && path.contains("/generations/")
            && path.ends_with("/manifest.json")
            && !path.contains("/plane/")
    });
    assert_eq!(staged.len(), 1);
    let orphan = Path::from(staged[0].clone());
    let prepared: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            directory
                .path()
                .join(base.root_sha256())
                .join("generation/manifest.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let sq8 = Path::from(prepared["sq8_object_key"].as_str().unwrap());
    let claim = Path::from(format!(
        "{}/claim.json",
        sq8.as_ref().rsplit_once("/objects/").unwrap().0
    ));
    let caps = TwoBitGcLimits {
        max_memory_bytes: 2_000_000,
        mutations,
        max_objects_scanned: 100,
        max_objects_deleted: 100,
        max_deleted_object_bytes: 1_000_000,
    };
    let target = orphan.clone();
    let failed_delete = common::FaultInjectingObjectStore::fail_nth_matching(
        store.clone(),
        1,
        true,
        move |op, path| op == common::StoreOperation::Delete && *path == target,
    );
    collect_two_bit_garbage(Arc::new(failed_delete), &prefix, directory.path(), caps)
        .await
        .unwrap_err();
    collect_two_bit_garbage(
        store.clone(),
        &prefix,
        directory.path(),
        TwoBitGcLimits {
            max_objects_deleted: 1,
            ..caps
        },
    )
    .await
    .unwrap();
    assert!(store.head(&orphan).await.is_err());
    assert!(store.head(&sq8).await.is_ok());
    assert!(store.head(&claim).await.is_ok());
    let acknowledged = compact_two_bit_index(store.clone(), &prefix, directory.path(), options)
        .await
        .unwrap();
    store.delete(&orphan).await.unwrap();
    let recovered = read_two_bit_head(store.as_ref(), &prefix)
        .await
        .expect("a delayed metadata DELETE must not destroy acknowledged compaction")
        .unwrap();
    assert_eq!(recovered.root_sha256(), acknowledged.root_sha256());
    assert_ne!(recovered.metadata_prefix().join("manifest.json"), orphan);
}

#[tokio::test]
async fn empty_generation_rejects_invalid_geometry_before_io() {
    let store = Arc::new(InMemory::new());
    let prefix = Path::from("empty-admission");
    let base = publish_empty_two_bit_generation(store.as_ref(), &prefix, 2, 1, None)
        .await
        .unwrap();
    let (guarded, operations) =
        common::FaultInjectingObjectStore::fail_nth_matching(store, usize::MAX, true, |_, _| false)
            .with_operation_log();
    for (dimensions, generation) in [(0, 2), (2, 1)] {
        assert!(matches!(
            publish_empty_two_bit_generation(
                &guarded,
                &prefix,
                dimensions,
                generation,
                Some(&base)
            )
            .await,
            Err(borsuk::two_bit_store::TwoBitStoreError::Invalid(
                "empty generation namespace/order/dimensions"
            ))
        ));
    }
    assert!(operations.matching_paths(|_, _| true).is_empty());
}
