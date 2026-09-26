use std::{fs, sync::Barrier, thread};

use borsuk::{
    resident_graph_build::build_graph_generation,
    resident_graph_generation::ResidentGraphGeneration,
    resident_graph_store::{hydrate_graph_generation, publish_graph_generation, read_graph_head},
};
use object_store::{memory::InMemory, path::Path as ObjectPath};

#[tokio::test]
async fn create_publish_open_search_and_reopen() {
    let source = (0..256_u64)
        .map(|row| {
            (
                1000 + row,
                (0..64)
                    .map(|dimension| ((row * 37 + dimension * 13) % 101) as f32 / 50.0 - 1.0)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    let query = source[42].1.clone();
    let output = tempfile::tempdir().unwrap();
    let directory = output.path().join("generation");
    let root_sha = build_graph_generation(&directory, source, 1).unwrap();
    let root = fs::read(directory.join("root.json")).unwrap();
    let store = InMemory::new();
    let prefix = ObjectPath::from("release/graph");
    publish_graph_generation(&store, &prefix, &root, &directory, None)
        .await
        .unwrap();
    let head = read_graph_head(&store, &prefix).await.unwrap().unwrap();
    assert_eq!(head.root_sha256, root_sha);
    assert!(
        ResidentGraphGeneration::open_local_authenticated(&root, &root_sha, &directory, 1, 1)
            .is_err()
    );
    let cache = tempfile::tempdir().unwrap();
    let (cold, first) =
        hydrate_graph_generation(&store, &prefix, &head, cache.path(), 32 * 1024 * 1024, 2)
            .await
            .unwrap();
    assert_eq!(first.object_gets, 5);
    let mut worker = cold.searcher().unwrap();
    let ids = worker.search(&query, 10).unwrap().0;
    assert_eq!(ids.len(), 10);
    assert!(ids.contains(&1042));
    drop(worker);
    let barrier = Barrier::new(3);
    thread::scope(|scope| {
        let generation = &cold;
        let query = &query;
        let barrier = &barrier;
        let run = || {
            let mut worker = generation.searcher().unwrap();
            barrier.wait();
            barrier.wait();
            worker.search(query, 10).unwrap().0
        };
        let first = scope.spawn(run);
        let second = scope.spawn(run);
        barrier.wait();
        assert!(generation.searcher().is_err());
        barrier.wait();
        assert_eq!(first.join().unwrap(), ids);
        assert_eq!(second.join().unwrap(), ids);
    });
    drop(cold);
    let (warm, second) =
        hydrate_graph_generation(&store, &prefix, &head, cache.path(), 32 * 1024 * 1024, 2)
            .await
            .unwrap();
    assert_eq!(second.object_gets, 0);
    assert_eq!(warm.searcher().unwrap().search(&query, 10).unwrap().0, ids);
    assert!(
        ResidentGraphGeneration::open_local_authenticated(
            &root,
            &"0".repeat(64),
            &directory,
            32 * 1024 * 1024,
            2,
        )
        .is_err()
    );
    fs::write(directory.join("codes.bin"), b"corrupt").unwrap();
    assert!(
        ResidentGraphGeneration::open_local_authenticated(
            &root,
            &root_sha,
            &directory,
            32 * 1024 * 1024,
            2,
        )
        .is_err()
    );
}

#[test]
fn builder_rejects_duplicate_ids_and_zero_vectors_before_writing() {
    let parent = tempfile::tempdir().unwrap();
    let zero_generation = (0..256).map(|id| (id, vec![1.0; 64])).collect();
    let path = parent.path().join("zero_generation");
    assert!(build_graph_generation(&path, zero_generation, 0).is_err());
    assert!(!path.exists());
    let duplicate = vec![(7, vec![1.0; 64]); 256];
    let path = parent.path().join("duplicate");
    assert!(build_graph_generation(&path, duplicate, 1).is_err());
    assert!(!path.exists());
    let zero = (0..256).map(|id| (id, vec![0.0; 64])).collect();
    let path = parent.path().join("zero");
    assert!(build_graph_generation(&path, zero, 1).is_err());
    assert!(!path.exists());
}
