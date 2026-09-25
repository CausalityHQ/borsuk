//! Immutable graph artifacts with one conditional, mutable generation head.

use std::{
    fs,
    io::{self, Read},
    path::Path,
};

use bytes::Bytes;
use futures_util::StreamExt;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::resident_graph_generation::{
    Artifact, MAX_ROOT_BYTES, ResidentGraphGeneration, ResidentGraphGenerationError, Root,
    parse_authenticated_root, preflight_root, valid_sha256,
};

const HEAD_SCHEMA: &str = "borsuk-resident-graph-head-v1";
const MIN_PART_BYTES: u64 = 8 * 1024 * 1024;
const MAX_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_PARTS: u64 = 9_000;

fn multipart_part_bytes(length: u64) -> Result<usize, ResidentGraphStoreError> {
    let mebibyte = 1024 * 1024;
    let bytes = MIN_PART_BYTES.max(length.div_ceil(MAX_PARTS).div_ceil(mebibyte) * mebibyte);
    if bytes > MAX_PART_BYTES {
        return Err(ResidentGraphStoreError::Invalid(
            "artifact exceeds multipart limit",
        ));
    }
    Ok(bytes as usize)
}

#[derive(Debug, Error)]
pub enum ResidentGraphStoreError {
    #[error("graph store: {0}")]
    Store(#[from] object_store::Error),
    #[error("graph local I/O: {0}")]
    Io(#[from] io::Error),
    #[error("graph blocking worker: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("graph root: {0}")]
    Root(#[from] ResidentGraphGenerationError),
    #[error("graph store is invalid: {0}")]
    Invalid(&'static str),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadBody {
    schema: String,
    generation: u64,
    root_sha256: String,
}

/// A pinned head and its authenticated root. Keep this value while serving;
/// a later publisher can replace the head without changing these artifacts.
#[derive(Debug)]
pub struct ResidentGraphHead {
    pub generation: u64,
    pub root_sha256: String,
    pub root_bytes: Vec<u8>,
    version: UpdateVersion,
}

fn root_path(prefix: &ObjectPath, sha256: &str) -> ObjectPath {
    prefix.clone().join(format!("roots/{sha256}.json"))
}

fn blob_path(prefix: &ObjectPath, sha256: &str) -> ObjectPath {
    prefix.clone().join(format!("blobs/{sha256}"))
}

pub(crate) async fn get_root(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    sha256: &str,
) -> Result<Vec<u8>, ResidentGraphStoreError> {
    let result = store.get(&root_path(prefix, sha256)).await?;
    if result.meta.size > MAX_ROOT_BYTES as u64 {
        return Err(ResidentGraphStoreError::Invalid("root length"));
    }
    let bytes = result.bytes().await?.to_vec();
    parse_authenticated_root(&bytes, sha256)?;
    Ok(bytes)
}

/// Fetch the current conditional head and authenticate its immutable root.
pub async fn read_graph_head(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<Option<ResidentGraphHead>, ResidentGraphStoreError> {
    let result = match store.get(&prefix.clone().join("head.json")).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if result.meta.size > 1024 {
        return Err(ResidentGraphStoreError::Invalid("head length"));
    }
    let version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    let bytes = result.bytes().await?;
    let head: HeadBody = serde_json::from_slice(&bytes)
        .map_err(|_| ResidentGraphStoreError::Invalid("head JSON"))?;
    if head.schema != HEAD_SCHEMA || head.generation == 0 || !valid_sha256(&head.root_sha256) {
        return Err(ResidentGraphStoreError::Invalid("head identity"));
    }
    let root_bytes = get_root(store, prefix, &head.root_sha256).await?;
    let root = parse_authenticated_root(&root_bytes, &head.root_sha256)?;
    if root.generation != head.generation {
        return Err(ResidentGraphStoreError::Invalid("head generation"));
    }
    Ok(Some(ResidentGraphHead {
        generation: head.generation,
        root_sha256: head.root_sha256,
        root_bytes,
        version,
    }))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResidentGraphHydrationStats {
    pub object_gets: u64,
    pub response_bytes: u64,
}

fn file_matches(path: &Path, artifact: &Artifact) -> Result<bool, io::Error> {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() != artifact.bytes {
        return Ok(false);
    }
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()) == artifact.sha256)
}

async fn hydrate_artifact(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    directory: &Path,
    name: &str,
    artifact: &Artifact,
) -> Result<ResidentGraphHydrationStats, ResidentGraphStoreError> {
    let path = directory.join(name);
    if tokio::task::spawn_blocking({
        let path = path.clone();
        let artifact = Artifact {
            bytes: artifact.bytes,
            sha256: artifact.sha256.clone(),
        };
        move || file_matches(&path, &artifact)
    })
    .await??
    {
        return Ok(ResidentGraphHydrationStats::default());
    }
    let result = store.get(&blob_path(prefix, &artifact.sha256)).await?;
    if result.meta.size != artifact.bytes {
        return Err(ResidentGraphStoreError::Invalid("blob length"));
    }
    let temporary = NamedTempFile::new_in(directory)?;
    let mut output = tokio::fs::File::from_std(temporary.as_file().try_clone()?);
    let mut received = 0_u64;
    let mut digest = Sha256::new();
    let mut stream = result.into_stream();
    while let Some(next) = stream.next().await {
        let chunk = next?;
        received = received
            .checked_add(chunk.len() as u64)
            .ok_or(ResidentGraphStoreError::Invalid("blob length overflow"))?;
        if received > artifact.bytes {
            return Err(ResidentGraphStoreError::Invalid("blob length"));
        }
        output.write_all(&chunk).await?;
        digest.update(&chunk);
    }
    if received != artifact.bytes || format!("{:x}", digest.finalize()) != artifact.sha256 {
        return Err(ResidentGraphStoreError::Invalid("blob SHA-256 or length"));
    }
    output.flush().await?;
    output.sync_all().await?;
    drop(output);
    temporary
        .persist(path)
        .map_err(|error| ResidentGraphStoreError::Io(error.error))?;
    Ok(ResidentGraphHydrationStats {
        object_gets: 1,
        response_bytes: received,
    })
}

/// Hydrate the exact generation named by a previously read head. Complete
/// files are reused after a full local hash check; bad files are replaced
/// only after the object-store transfer is authenticated.
pub async fn hydrate_graph_generation(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    head: &ResidentGraphHead,
    cache_root: &Path,
    max_resident_bytes: usize,
    active_workers: usize,
) -> Result<(ResidentGraphGeneration, ResidentGraphHydrationStats), ResidentGraphStoreError> {
    let root = parse_authenticated_root(&head.root_bytes, &head.root_sha256)?;
    if root.generation != head.generation {
        return Err(ResidentGraphStoreError::Invalid("head generation"));
    }
    hydrate_graph_root(
        store, prefix, &head.root_bytes, &head.root_sha256,
        cache_root, max_resident_bytes, active_workers,
    ).await
}

/// Hydrate a pinned immutable root named by a collection revision. This
/// remains valid after the legacy graph head has advanced.
pub(crate) async fn hydrate_graph_root(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    root_bytes: &[u8],
    root_sha256: &str,
    cache_root: &Path,
    max_resident_bytes: usize,
    active_workers: usize,
) -> Result<(ResidentGraphGeneration, ResidentGraphHydrationStats), ResidentGraphStoreError> {
    let root = parse_authenticated_root(root_bytes, root_sha256)?;
    preflight_root(&root, max_resident_bytes, active_workers)?;
    let directory = cache_root.join(root_sha256);
    tokio::fs::create_dir_all(&directory).await?;
    let mut stats = ResidentGraphHydrationStats::default();
    for (name, artifact) in [
        ("plane.bin", &root.plane),
        ("graph.bin", &root.graph),
        ("map.u32", &root.map),
        ("books.bin", &root.books),
        ("codes.bin", &root.codes),
    ] {
        let next = hydrate_artifact(store, prefix, &directory, name, artifact).await?;
        stats.object_gets += next.object_gets;
        stats.response_bytes += next.response_bytes;
    }
    let root_bytes = root_bytes.to_vec();
    let trusted = root_sha256.to_owned();
    let generation = tokio::task::spawn_blocking(move || {
        ResidentGraphGeneration::open_local_authenticated(
            &root_bytes,
            &trusted,
            &directory,
            max_resident_bytes,
            active_workers,
        )
    })
    .await??;
    Ok((generation, stats))
}

async fn upload_artifact(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    directory: &Path,
    name: &str,
    artifact: &Artifact,
) -> Result<(), ResidentGraphStoreError> {
    let mut file = tokio::fs::File::open(directory.join(name)).await?;
    if file.metadata().await?.len() != artifact.bytes {
        return Err(ResidentGraphStoreError::Invalid("artifact length"));
    }
    let part_bytes = multipart_part_bytes(artifact.bytes)?;
    let mut upload = store
        .put_multipart(&blob_path(prefix, &artifact.sha256))
        .await?;
    let mut digest = Sha256::new();
    let mut received = 0_u64;
    // S3 allows at most 10,000 parts; leave room for a short final part.
    let mut buffer = vec![0_u8; part_bytes];
    let result = async {
        loop {
            let mut filled = 0;
            while filled < buffer.len() {
                let n = file.read(&mut buffer[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }
            received = received
                .checked_add(filled as u64)
                .ok_or(ResidentGraphStoreError::Invalid("artifact length overflow"))?;
            if received > artifact.bytes {
                return Err(ResidentGraphStoreError::Invalid("artifact length"));
            }
            digest.update(&buffer[..filled]);
            upload
                .put_part(PutPayload::from(Bytes::copy_from_slice(&buffer[..filled])))
                .await?;
        }
        if received != artifact.bytes || format!("{:x}", digest.finalize()) != artifact.sha256 {
            return Err(ResidentGraphStoreError::Invalid(
                "artifact SHA-256 or length",
            ));
        }
        upload.complete().await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = upload.abort().await;
    }
    result
}

/// Stage all five authenticated blobs and their immutable root without
/// moving a mutable head. A compactor can publish a collection revision
/// pointing at this root after its complete build succeeds.
pub async fn stage_graph_generation(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    root_bytes: &[u8],
    directory: &Path,
) -> Result<String, ResidentGraphStoreError> {
    let root_sha256 = format!("{:x}", Sha256::digest(root_bytes));
    let root: Root = parse_authenticated_root(root_bytes, &root_sha256)?;
    for (name, artifact) in [
        ("plane.bin", &root.plane),
        ("graph.bin", &root.graph),
        ("map.u32", &root.map),
        ("books.bin", &root.books),
        ("codes.bin", &root.codes),
    ] {
        upload_artifact(store, prefix, directory, name, artifact).await?;
    }
    store
        .put(
            &root_path(prefix, &root_sha256),
            PutPayload::from(root_bytes.to_vec()),
        )
        .await?;
    Ok(root_sha256)
}

/// Publish all five blobs and an immutable root, then CAS the head last.
/// Failed uploads may leave unreachable blobs; they cannot expose a partial
/// generation. The object store must implement conditional `put_opts`.
pub async fn publish_graph_generation(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    root_bytes: &[u8],
    directory: &Path,
    expected: Option<&ResidentGraphHead>,
) -> Result<ResidentGraphHead, ResidentGraphStoreError> {
    let root: Root = parse_authenticated_root(
        root_bytes, &format!("{:x}", Sha256::digest(root_bytes)))?;
    if expected.is_some_and(|head| root.generation <= head.generation) {
        return Err(ResidentGraphStoreError::Invalid("generation order"));
    }
    let root_sha256 = stage_graph_generation(store, prefix, root_bytes, directory).await?;
    let head_bytes = serde_json::to_vec(&HeadBody {
        schema: HEAD_SCHEMA.to_owned(),
        generation: root.generation,
        root_sha256: root_sha256.clone(),
    })
    .map_err(|_| ResidentGraphStoreError::Invalid("head serialization"))?;
    let mode = expected.map_or(PutMode::Create, |head| {
        PutMode::Update(head.version.clone())
    });
    let result = store
        .put_opts(
            &prefix.clone().join("head.json"),
            PutPayload::from(head_bytes),
            PutOptions {
                mode,
                ..PutOptions::default()
            },
        )
        .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            // The store may commit a conditional write and lose its reply.
            // Reconcile against the durable head before reporting failure.
            if let Ok(Some(head)) = read_graph_head(store, prefix).await {
                if head.root_sha256 == root_sha256 && head.generation == root.generation {
                    return Ok(head);
                }
            }
            return Err(error.into());
        }
    };
    Ok(ResidentGraphHead {
        generation: root.generation,
        root_sha256,
        root_bytes: root_bytes.to_vec(),
        version: UpdateVersion::from(result),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn fixture(directory: &Path, generation: u64) -> Vec<u8> {
        let artifact = |name: &str| {
            let bytes = format!("{generation}:{name}").into_bytes();
            std::fs::write(directory.join(name), &bytes).unwrap();
            serde_json::json!({
                "bytes": bytes.len(),
                "sha256": format!("{:x}", Sha256::digest(bytes))
            })
        };
        serde_json::to_vec(&serde_json::json!({
            "schema": crate::resident_graph_generation::SCHEMA,
            "generation": generation,
            "source_sha256": "11".repeat(32),
            "rows": 4,
            "dimensions": 2,
            "plane": artifact("plane.bin"),
            "graph": artifact("graph.bin"),
            "map": artifact("map.u32"),
            "books": artifact("books.bin"),
            "codes": artifact("codes.bin"),
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn partial_upload_and_stale_writer_never_move_head() {
        let hundred_million_fp16 = 100_000_000_u64 * 768 * 2;
        assert!(
            hundred_million_fp16
                .div_ceil(multipart_part_bytes(hundred_million_fp16).unwrap() as u64)
                <= MAX_PARTS
        );
        let store = InMemory::new();
        let prefix = ObjectPath::from("collection/graph");
        let dir = tempfile::tempdir().unwrap();
        let first = fixture(dir.path(), 1);
        let original = publish_graph_generation(&store, &prefix, &first, dir.path(), None)
            .await
            .unwrap();
        assert_eq!(
            read_graph_head(&store, &prefix)
                .await
                .unwrap()
                .unwrap()
                .root_bytes,
            first
        );

        let second = fixture(dir.path(), 2);
        let staged = stage_graph_generation(&store, &prefix, &second, dir.path())
            .await.unwrap();
        assert_eq!(staged, format!("{:x}", Sha256::digest(&second)));
        assert_eq!(read_graph_head(&store, &prefix).await.unwrap().unwrap().root_bytes, first);
        std::fs::write(dir.path().join("codes.bin"), b"corrupt").unwrap();
        assert!(
            publish_graph_generation(&store, &prefix, &second, dir.path(), Some(&original))
                .await
                .is_err()
        );
        assert_eq!(
            read_graph_head(&store, &prefix)
                .await
                .unwrap()
                .unwrap()
                .root_bytes,
            first
        );

        let second = fixture(dir.path(), 2);
        let current =
            publish_graph_generation(&store, &prefix, &second, dir.path(), Some(&original))
                .await
                .unwrap();
        assert_eq!(current.generation, 2);
        assert_eq!(
            read_graph_head(&store, &prefix)
                .await
                .unwrap()
                .unwrap()
                .root_bytes,
            second
        );

        let third = fixture(dir.path(), 3);
        assert!(
            publish_graph_generation(&store, &prefix, &third, dir.path(), Some(&original))
                .await
                .is_err()
        );
        assert_eq!(
            read_graph_head(&store, &prefix)
                .await
                .unwrap()
                .unwrap()
                .root_bytes,
            second
        );
    }
}
