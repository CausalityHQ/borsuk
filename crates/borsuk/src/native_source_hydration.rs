//! Conditional object-store hydration for a generation-pinned exact source tier.

use std::{fs::File, io, path::Path};

use futures_util::StreamExt;
use object_store::{GetOptions, ObjectStore, path::Path as ObjectPath};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

use crate::native_source_tier::{NativeSourceTier, SourceTierError, decoded_sha256, expected_len};

/// Generation authority inputs required to hydrate an exact source object.
/// The ETag and hashes must come from an authenticated generation manifest.
pub struct SourceHydrationSpec<'a> {
    /// Immutable object location in the configured object store.
    pub location: &'a ObjectPath,
    /// Pinned object ETag used as an HTTP If-Match precondition.
    pub etag: &'a str,
    /// SHA-256 of the complete version-2 source-tier object.
    pub artifact_sha256: &'a str,
    /// SHA-256 identity of the original source object.
    pub source_sha256: &'a str,
    /// Expected source row count.
    pub rows: u64,
    /// Expected vector dimensions.
    pub dimensions: usize,
    /// Pinned generation number.
    pub generation: u64,
    /// Local verification block bytes, a power of two from 4 KiB to 1 MiB.
    pub verification_block_bytes: usize,
}

/// Charged full-object transfer and local full-file validation work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceHydrationStats {
    /// Number of object-store GETs issued by this hydration call.
    pub s3_gets: u64,
    /// Response body bytes streamed by this hydration call.
    pub s3_bytes: u64,
    /// Local bytes scanned while authenticating the opened source tier.
    pub local_validation_bytes: u64,
    /// Whether an existing valid local artifact was reused without a GET.
    pub reused_cache: bool,
}

/// A failed cache or object-store hydration never yields a serving tier.
#[derive(Debug, Error)]
pub enum SourceHydrationError {
    /// The pinned object-store read failed.
    #[error("source hydration object-store operation failed: {0}")]
    Store(#[from] object_store::Error),
    /// The local source-tier artifact failed authentication.
    #[error("source hydration source-tier validation failed: {0}")]
    Source(#[from] SourceTierError),
    /// A local filesystem operation failed.
    #[error("source hydration local I/O failed: {0}")]
    Io(#[from] io::Error),
    /// A blocking local validation or sync worker could not finish.
    #[error("source hydration blocking task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    /// The response or generation declaration differs from the pinned identity.
    #[error("source hydration contract differs: {0}")]
    Invalid(&'static str),
}

async fn open_verified(
    path: &Path,
    spec: &SourceHydrationSpec<'_>,
) -> Result<NativeSourceTier, SourceHydrationError> {
    let path = path.to_owned();
    let artifact_sha256 = spec.artifact_sha256.to_owned();
    let source_sha256 = spec.source_sha256.to_owned();
    let (rows, dimensions, generation, block_bytes) = (
        spec.rows,
        spec.dimensions,
        spec.generation,
        spec.verification_block_bytes,
    );
    Ok(tokio::task::spawn_blocking(move || {
        NativeSourceTier::open_authenticated(
            &path,
            &artifact_sha256,
            &source_sha256,
            rows,
            dimensions,
            generation,
            block_bytes,
        )
    })
    .await??)
}

/// Hydrate a complete source tier through one conditional GET, or reuse a
/// locally authenticated cache after restart. A corrupt cache is replaced
/// atomically only after the replacement is fully authenticated and synced.
/// The returned tier pins its verified file handle even if the cache path is
/// later replaced for another reader.
pub async fn hydrate_source_tier(
    store: &dyn ObjectStore,
    spec: &SourceHydrationSpec<'_>,
    cache_path: &Path,
) -> Result<(NativeSourceTier, SourceHydrationStats), SourceHydrationError> {
    if spec.etag.is_empty() {
        return Err(SourceHydrationError::Invalid("empty source ETag"));
    }
    let expected_digest = decoded_sha256(spec.artifact_sha256)?;
    decoded_sha256(spec.source_sha256)?;
    let length = expected_len(spec.rows, spec.dimensions)?;
    if cache_path.exists() {
        match open_verified(cache_path, spec).await {
            Ok(tier) => {
                return Ok((
                    tier,
                    SourceHydrationStats {
                        local_validation_bytes: length,
                        reused_cache: true,
                        ..SourceHydrationStats::default()
                    },
                ));
            }
            Err(SourceHydrationError::Source(_)) => {}
            Err(other) => return Err(other),
        }
    }
    let parent = cache_path.parent().ok_or(SourceHydrationError::Invalid(
        "source cache path has no parent",
    ))?;
    let result = store
        .get_opts(
            spec.location,
            GetOptions::new().with_if_match(Some(spec.etag.to_owned())),
        )
        .await?;
    if result.meta.size != length
        || result.range != (0..length)
        || result.meta.e_tag.as_deref() != Some(spec.etag)
    {
        return Err(SourceHydrationError::Invalid("source object metadata"));
    }
    let temporary = NamedTempFile::new_in(parent)?;
    let mut async_file = tokio::fs::File::from_std(temporary.as_file().try_clone()?);
    let mut hash = Sha256::new();
    let mut received = 0_u64;
    let mut stream = result.into_stream();
    while let Some(next) = stream.next().await {
        let chunk = next?;
        received =
            received
                .checked_add(chunk.len() as u64)
                .ok_or(SourceHydrationError::Invalid(
                    "source transfer size overflow",
                ))?;
        if received > length {
            return Err(SourceHydrationError::Invalid(
                "source transfer exceeds length",
            ));
        }
        async_file.write_all(&chunk).await?;
        hash.update(&chunk);
    }
    if received != length {
        return Err(SourceHydrationError::Invalid(
            "source transfer is truncated",
        ));
    }
    let actual_digest: [u8; 32] = hash.finalize().into();
    if actual_digest != expected_digest {
        return Err(SourceHydrationError::Invalid("source transfer SHA-256"));
    }
    async_file.sync_all().await?;
    drop(async_file);
    let tier = open_verified(temporary.path(), spec).await?;
    temporary
        .persist(cache_path)
        .map_err(|error| SourceHydrationError::Io(error.error))?;
    let parent = parent.to_owned();
    tokio::task::spawn_blocking(move || File::open(parent)?.sync_all()).await??;
    Ok((
        tier,
        SourceHydrationStats {
            s3_gets: 1,
            s3_bytes: received,
            local_validation_bytes: length,
            reused_cache: false,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_source_tier::{SourceCandidate, write_source_tier};
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory};
    use std::os::unix::fs::FileExt;

    const SOURCE_SHA: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[tokio::test]
    async fn cold_hydration_reuse_and_corrupt_cache_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("built.bin");
        let cache = directory.path().join("cache.bin");
        let digest = write_source_tier(
            &source,
            1,
            2,
            7,
            SOURCE_SHA,
            vec![(42, vec![1.0, 0.0])].into_iter(),
        )
        .unwrap();
        let body = std::fs::read(&source).unwrap();
        let store = InMemory::new();
        let location = ObjectPath::from("source-v2.bin");
        store
            .put(&location, PutPayload::from(body.clone()))
            .await
            .unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let spec = SourceHydrationSpec {
            location: &location,
            etag: &etag,
            artifact_sha256: &digest,
            source_sha256: SOURCE_SHA,
            rows: 1,
            dimensions: 2,
            generation: 7,
            verification_block_bytes: 4096,
        };
        let (tier, cold) = hydrate_source_tier(&store, &spec, &cache).await.unwrap();
        assert_eq!(
            cold,
            SourceHydrationStats {
                s3_gets: 1,
                s3_bytes: body.len() as u64,
                local_validation_bytes: body.len() as u64,
                reused_cache: false,
            }
        );
        let candidate = [SourceCandidate {
            ordinal: 0,
            source_id: 42,
        }];
        assert_eq!(
            tier.rank_exact(&[1.0, 0.0], &candidate, 1).unwrap()[0].score,
            1.0
        );
        let (_, warm) = hydrate_source_tier(&store, &spec, &cache).await.unwrap();
        assert_eq!(warm.s3_gets, 0);
        assert!(warm.reused_cache);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&cache)
            .unwrap();
        file.write_all_at(&0.0_f32.to_le_bytes(), 64 + 8).unwrap();
        let (restored, recovery) = hydrate_source_tier(&store, &spec, &cache).await.unwrap();
        assert_eq!(recovery.s3_gets, 1);
        assert!(!recovery.reused_cache);
        assert_eq!(std::fs::read(&cache).unwrap(), body);
        assert_eq!(
            restored.rank_exact(&[1.0, 0.0], &candidate, 1).unwrap()[0].score,
            1.0
        );
    }

    #[tokio::test]
    async fn wrong_etag_or_digest_never_publishes_cache() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("built.bin");
        let cache = directory.path().join("cache.bin");
        let digest = write_source_tier(
            &source,
            1,
            2,
            7,
            SOURCE_SHA,
            vec![(42, vec![1.0, 0.0])].into_iter(),
        )
        .unwrap();
        let body = std::fs::read(&source).unwrap();
        let store = InMemory::new();
        let location = ObjectPath::from("source-v2.bin");
        store.put(&location, PutPayload::from(body)).await.unwrap();
        let etag = store.head(&location).await.unwrap().e_tag.unwrap();
        let mut spec = SourceHydrationSpec {
            location: &location,
            etag: "wrong",
            artifact_sha256: &digest,
            source_sha256: SOURCE_SHA,
            rows: 1,
            dimensions: 2,
            generation: 7,
            verification_block_bytes: 4096,
        };
        assert!(hydrate_source_tier(&store, &spec, &cache).await.is_err());
        assert!(!cache.exists());
        spec.etag = &etag;
        spec.artifact_sha256 = SOURCE_SHA;
        assert!(hydrate_source_tier(&store, &spec, &cache).await.is_err());
        assert!(!cache.exists());
    }
}
