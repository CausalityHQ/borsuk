//! One conditional collection head pins a graph root and its mutation snapshot.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

use bytes::Bytes;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    resident_graph_generation::{parse_authenticated_root, valid_sha256},
    resident_graph_mutation_snapshot::{
        ResidentGraphMutationSnapshotError, decode_mutation_snapshot, encode_mutation_snapshot,
    },
    resident_graph_overlay::{ResidentGraphOverlay, ResidentGraphOverlayError, ResidentMutation},
    resident_graph_store::{
        ResidentGraphHydrationStats, ResidentGraphStoreError, get_root, hydrate_graph_root,
    },
};

const HEAD_SCHEMA: &str = "borsuk-resident-graph-collection-head-v1";
const MAX_HEAD_BYTES: u64 = 1024;

#[derive(Debug, Error)]
pub enum ResidentGraphCollectionError {
    #[error("collection object store: {0}")]
    Store(#[from] object_store::Error),
    #[error("collection graph store: {0}")]
    Graph(#[from] ResidentGraphStoreError),
    #[error("collection mutation snapshot: {0}")]
    Snapshot(#[from] ResidentGraphMutationSnapshotError),
    #[error("collection overlay: {0}")]
    Overlay(#[from] ResidentGraphOverlayError),
    #[error("collection is invalid: {0}")]
    Invalid(&'static str),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadBody {
    schema: String,
    revision: u64,
    base_root_sha256: String,
    mutation_sha256: String,
}

/// Pinned, authenticated collection revision. Readers retain this value
/// while later CAS updates replace the current head.
pub struct ResidentGraphCollectionHead {
    pub revision: u64,
    pub base_root_sha256: String,
    pub mutation_sha256: String,
    base_root_bytes: Vec<u8>,
    mutation_bytes: Vec<u8>,
    version: UpdateVersion,
}

/// Readers pin one complete graph-plus-mutation revision during a swap.
pub struct ResidentGraphCollectionSlot {
    current: RwLock<(u64, Arc<ResidentGraphOverlay>)>,
}

impl ResidentGraphCollectionSlot {
    pub fn new(
        revision: u64,
        overlay: Arc<ResidentGraphOverlay>,
    ) -> Result<Self, ResidentGraphCollectionError> {
        if revision == 0 {
            return Err(ResidentGraphCollectionError::Invalid("revision zero"));
        }
        Ok(Self {
            current: RwLock::new((revision, overlay)),
        })
    }

    pub fn pin(&self) -> (u64, Arc<ResidentGraphOverlay>) {
        let current = self
            .current
            .read()
            .unwrap_or_else(|error| error.into_inner());
        (current.0, Arc::clone(&current.1))
    }

    pub fn replace(
        &self,
        revision: u64,
        overlay: Arc<ResidentGraphOverlay>,
    ) -> Result<(u64, Arc<ResidentGraphOverlay>), ResidentGraphCollectionError> {
        let mut current = self
            .current
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if revision <= current.0 {
            return Err(ResidentGraphCollectionError::Invalid("revision order"));
        }
        Ok(std::mem::replace(&mut *current, (revision, overlay)))
    }
}

fn mutation_path(prefix: &ObjectPath, sha256: &str) -> ObjectPath {
    prefix.clone().join(format!("mutations/{sha256}.bin"))
}

fn head_path(prefix: &ObjectPath) -> ObjectPath {
    prefix.clone().join("collection-head.json")
}

/// Read the single authoritative head, then authenticate both immutable
/// references. Snapshot decoding is bounded before any row allocation.
pub async fn read_graph_collection_head(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    max_snapshot_bytes: usize,
) -> Result<Option<ResidentGraphCollectionHead>, ResidentGraphCollectionError> {
    let result = match store.get(&head_path(prefix)).await {
        Ok(value) => value,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if result.meta.size > MAX_HEAD_BYTES {
        return Err(ResidentGraphCollectionError::Invalid("head length"));
    }
    let version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let bytes = result.bytes().await?;
    let body: HeadBody = serde_json::from_slice(&bytes)
        .map_err(|_| ResidentGraphCollectionError::Invalid("head JSON"))?;
    if body.schema != HEAD_SCHEMA
        || body.revision == 0
        || !valid_sha256(&body.base_root_sha256)
        || !valid_sha256(&body.mutation_sha256)
    {
        return Err(ResidentGraphCollectionError::Invalid("head identity"));
    }
    let base_root_bytes = get_root(store, prefix, &body.base_root_sha256).await?;
    let root = parse_authenticated_root(&base_root_bytes, &body.base_root_sha256)
        .map_err(ResidentGraphStoreError::from)?;
    let snapshot = store
        .get(&mutation_path(prefix, &body.mutation_sha256))
        .await?;
    if snapshot.meta.size > max_snapshot_bytes as u64 {
        return Err(ResidentGraphCollectionError::Invalid("mutation byte cap"));
    }
    let mutation_bytes = snapshot.bytes().await?.to_vec();
    decode_mutation_snapshot(
        &mutation_bytes,
        &body.mutation_sha256,
        &body.base_root_sha256,
        root.dimensions,
        max_snapshot_bytes,
    )?;
    Ok(Some(ResidentGraphCollectionHead {
        revision: body.revision,
        base_root_sha256: body.base_root_sha256,
        mutation_sha256: body.mutation_sha256,
        base_root_bytes,
        mutation_bytes,
        version,
    }))
}

/// Publish one complete latest-state mutation snapshot, then CAS the
/// collection head. Compaction can supply a newly staged graph root here;
/// readers of older heads continue to use their pinned root and snapshot.
pub async fn publish_graph_collection(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    base_root_sha256: &str,
    mutations: &[ResidentMutation],
    max_snapshot_bytes: usize,
    expected: Option<&ResidentGraphCollectionHead>,
) -> Result<ResidentGraphCollectionHead, ResidentGraphCollectionError> {
    if !valid_sha256(base_root_sha256) {
        return Err(ResidentGraphCollectionError::Invalid("base root SHA-256"));
    }
    let base_root_bytes = get_root(store, prefix, base_root_sha256).await?;
    let root = parse_authenticated_root(&base_root_bytes, base_root_sha256)
        .map_err(ResidentGraphStoreError::from)?;
    let revision = expected
        .map_or(Some(1), |head| head.revision.checked_add(1))
        .ok_or(ResidentGraphCollectionError::Invalid("revision overflow"))?;
    let (mutation_bytes, mutation_sha256) = encode_mutation_snapshot(
        base_root_sha256,
        root.dimensions,
        mutations,
        max_snapshot_bytes,
    )?;
    store
        .put(
            &mutation_path(prefix, &mutation_sha256),
            PutPayload::from(Bytes::from(mutation_bytes.clone())),
        )
        .await?;
    let head_bytes = serde_json::to_vec(&HeadBody {
        schema: HEAD_SCHEMA.to_owned(),
        revision,
        base_root_sha256: base_root_sha256.to_owned(),
        mutation_sha256: mutation_sha256.clone(),
    })
    .map_err(|_| ResidentGraphCollectionError::Invalid("head serialization"))?;
    let mode = expected.map_or(PutMode::Create, |head| {
        PutMode::Update(head.version.clone())
    });
    let result = store
        .put_opts(
            &head_path(prefix),
            PutPayload::from(head_bytes),
            PutOptions {
                mode,
                ..PutOptions::default()
            },
        )
        .await;
    let result = match result {
        Ok(value) => value,
        Err(error) => {
            // A conditional write may commit even if the response is lost.
            if let Ok(Some(head)) =
                read_graph_collection_head(store, prefix, max_snapshot_bytes).await
            {
                if head.revision == revision
                    && head.base_root_sha256 == base_root_sha256
                    && head.mutation_sha256 == mutation_sha256
                {
                    return Ok(head);
                }
            }
            return Err(error.into());
        }
    };
    Ok(ResidentGraphCollectionHead {
        revision,
        base_root_sha256: base_root_sha256.to_owned(),
        mutation_sha256,
        base_root_bytes,
        mutation_bytes,
        version: UpdateVersion::from(result),
    })
}

/// Apply one ordered batch against the latest collection revision. A CAS
/// conflict reloads the winner and reapplies the batch, so distinct writers
/// cannot silently lose each other's updates. Within a batch, the last
/// operation for an ID wins.
pub async fn apply_graph_mutations(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    updates: &[ResidentMutation],
    max_snapshot_bytes: usize,
    max_attempts: usize,
) -> Result<ResidentGraphCollectionHead, ResidentGraphCollectionError> {
    if updates.is_empty() || max_attempts == 0 {
        return Err(ResidentGraphCollectionError::Invalid(
            "mutation batch geometry",
        ));
    }
    for _ in 0..max_attempts {
        let head = read_graph_collection_head(store, prefix, max_snapshot_bytes)
            .await?
            .ok_or(ResidentGraphCollectionError::Invalid(
                "collection not initialized",
            ))?;
        let root = parse_authenticated_root(&head.base_root_bytes, &head.base_root_sha256)
            .map_err(ResidentGraphStoreError::from)?;
        let old = decode_mutation_snapshot(
            &head.mutation_bytes,
            &head.mutation_sha256,
            &head.base_root_sha256,
            root.dimensions,
            max_snapshot_bytes,
        )?;
        let mut latest = old
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<BTreeMap<_, _>>();
        for update in updates {
            latest.insert(update.id, update.clone());
        }
        let rows = latest.into_values().collect::<Vec<_>>();
        match publish_graph_collection(
            store,
            prefix,
            &head.base_root_sha256,
            &rows,
            max_snapshot_bytes,
            Some(&head),
        )
        .await
        {
            Ok(next) => return Ok(next),
            Err(ResidentGraphCollectionError::Store(object_store::Error::Precondition {
                ..
            })) => {}
            Err(error) => return Err(error),
        }
    }
    Err(ResidentGraphCollectionError::Invalid("CAS retry limit"))
}

/// Hydrate an already pinned revision. The graph and mutation snapshot
/// remain aligned even if another writer advances the collection head.
pub async fn hydrate_graph_collection(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    head: &ResidentGraphCollectionHead,
    cache_root: &Path,
    max_graph_resident_bytes: usize,
    active_workers: usize,
    max_snapshot_bytes: usize,
    max_delta_bytes: usize,
) -> Result<(Arc<ResidentGraphOverlay>, ResidentGraphHydrationStats), ResidentGraphCollectionError>
{
    let root = parse_authenticated_root(&head.base_root_bytes, &head.base_root_sha256)
        .map_err(ResidentGraphStoreError::from)?;
    let mutations = decode_mutation_snapshot(
        &head.mutation_bytes,
        &head.mutation_sha256,
        &head.base_root_sha256,
        root.dimensions,
        max_snapshot_bytes,
    )?;
    let (graph, stats) = hydrate_graph_root(
        store,
        prefix,
        &head.base_root_bytes,
        &head.base_root_sha256,
        cache_root,
        max_graph_resident_bytes,
        active_workers,
    )
    .await?;
    let overlay = ResidentGraphOverlay::new(Arc::new(graph), mutations, max_delta_bytes)?;
    Ok((Arc::new(overlay), stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resident_graph_generation::SCHEMA as GRAPH_SCHEMA;
    use crate::resident_graph_store::{publish_graph_generation, stage_graph_generation};
    use object_store::memory::InMemory;
    use sha2::{Digest, Sha256};

    #[tokio::test]
    async fn collection_head_cas_pins_mutations_and_rejects_stale_writer() {
        let store = InMemory::new();
        let prefix = ObjectPath::from("collection");
        let dir = tempfile::tempdir().unwrap();
        let artifact = |name: &str| {
            let bytes = name.as_bytes();
            std::fs::write(dir.path().join(name), bytes).unwrap();
            serde_json::json!({"bytes":bytes.len(),"sha256":format!("{:x}",Sha256::digest(bytes))})
        };
        let root = serde_json::to_vec(&serde_json::json!({
            "schema":GRAPH_SCHEMA,"generation":1,"source_sha256":"11".repeat(32),
            "rows":4,"dimensions":2,"plane":artifact("plane.bin"),
            "graph":artifact("graph.bin"),"map":artifact("map.u32"),
            "books":artifact("books.bin"),"codes":artifact("codes.bin"),
        }))
        .unwrap();
        let base = publish_graph_generation(&store, &prefix, &root, dir.path(), None)
            .await
            .unwrap();
        let first = publish_graph_collection(&store, &prefix, &base.root_sha256, &[], 1024, None)
            .await
            .unwrap();
        let held = read_graph_collection_head(&store, &prefix, 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((first.revision, held.revision), (1, 1));
        let next = publish_graph_collection(
            &store,
            &prefix,
            &base.root_sha256,
            &[ResidentMutation {
                id: 9,
                vector: Some(vec![1.0, 0.0]),
            }],
            1024,
            Some(&first),
        )
        .await
        .unwrap();
        assert_eq!(next.revision, 2);
        assert_ne!(held.mutation_sha256, next.mutation_sha256);
        assert!(
            publish_graph_collection(
                &store,
                &prefix,
                &base.root_sha256,
                &[ResidentMutation {
                    id: 3,
                    vector: None
                }],
                1024,
                Some(&held)
            )
            .await
            .is_err()
        );
        let current = read_graph_collection_head(&store, &prefix, 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.mutation_sha256, next.mutation_sha256);
        let third_batch = [ResidentMutation {
            id: 5,
            vector: Some(vec![0.0, 1.0]),
        }];
        let fourth_batch = [ResidentMutation {
            id: 7,
            vector: None,
        }];
        let (third, fourth) = tokio::join!(
            apply_graph_mutations(&store, &prefix, &third_batch, 1024, 4),
            apply_graph_mutations(&store, &prefix, &fourth_batch, 1024, 4),
        );
        assert!(third.is_ok() && fourth.is_ok());
        let current = read_graph_collection_head(&store, &prefix, 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.revision, 4);
        let live = decode_mutation_snapshot(
            &current.mutation_bytes,
            &current.mutation_sha256,
            &current.base_root_sha256,
            2,
            1024,
        )
        .unwrap();
        assert_eq!(
            live.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![5, 7, 9]
        );
        let mut next_root: serde_json::Value = serde_json::from_slice(&root).unwrap();
        next_root["generation"] = serde_json::json!(2);
        let next_root = serde_json::to_vec(&next_root).unwrap();
        let staged = stage_graph_generation(&store, &prefix, &next_root, dir.path())
            .await
            .unwrap();
        let switched =
            publish_graph_collection(&store, &prefix, &staged, &[], 1024, Some(&current))
                .await
                .unwrap();
        assert_eq!(switched.revision, 5);
        assert_eq!(switched.base_root_sha256, staged);
        assert_eq!(held.base_root_sha256, base.root_sha256);
        assert_ne!(held.base_root_sha256, switched.base_root_sha256);
        let old = decode_mutation_snapshot(
            &held.mutation_bytes,
            &held.mutation_sha256,
            &held.base_root_sha256,
            2,
            1024,
        )
        .unwrap();
        assert!(old.is_empty());
    }
}
