//! Durable normalized FP32 source for maintenance, never hydrated by queries.
use crate::{
    object_native_generation::valid_object_key,
    resident_graph_generation::valid_sha256,
    sq8_source::cosine_vector,
    two_bit_generation::Manifest,
    two_bit_source::{SourceBuildError, TwoBitSource},
    two_bit_store::{TwoBitHead, TwoBitStoreError, small_object},
};
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};
use tokio::io::AsyncWriteExt;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CanonicalSource {
    pub(crate) rows: usize,
    pub(crate) dimensions: usize,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
    pub(crate) object_key: String,
}
impl CanonicalSource {
    pub(crate) fn valid(&self) -> bool {
        self.rows > 0
            && self.dimensions > 0
            && self
                .dimensions
                .checked_mul(4)
                .and_then(|n| n.checked_add(8))
                .and_then(|n| n.checked_mul(self.rows))
                .map(|n| n as u64)
                == Some(self.bytes)
            && valid_sha256(&self.sha256)
            && valid_object_key(&self.object_key, &self.sha256)
    }
}

/// Stream physical-order logical IDs and normalized rows after source-plane
/// permutation validation. Caller snapshots must remain immutable throughout.
pub(crate) fn write_canonical_source(
    source: &TwoBitSource<'_>,
    order: Option<&[u64]>,
    output: &Path,
    sq8_key: &str,
) -> std::result::Result<CanonicalSource, SourceBuildError> {
    let bad = SourceBuildError::Invalid;
    let width = source
        .dimensions
        .checked_mul(4)
        .ok_or(bad("canonical row width"))?;
    let bytes = width
        .checked_add(8)
        .and_then(|n| n.checked_mul(source.rows))
        .ok_or(bad("canonical geometry"))?;
    let mut raw = File::open(source.raw)?;
    if raw.metadata()?.len()
        != (width as u64)
            .checked_mul(source.rows as u64)
            .ok_or(bad("canonical raw geometry"))?
    {
        return Err(bad("canonical raw length"));
    }
    let mut sq8 = BufReader::with_capacity(65536, File::open(source.sq8)?);
    let mut row = vec![0; width];
    let mut values = vec![0.; source.dimensions];
    let mut sq8_row = vec![
        0;
        source
            .dimensions
            .checked_add(12)
            .ok_or(bad("canonical SQ8 width"))?
    ];
    let mut out = BufWriter::with_capacity(
        65536,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?,
    );
    let mut digest = Sha256::new();
    let mut sq8_digest = Sha256::new();
    for physical in 0..source.rows {
        sq8.read_exact(&mut sq8_row)?;
        sq8_digest.update(&sq8_row);
        let id = i64::from_le_bytes(sq8_row[..8].try_into().unwrap());
        let ordinal = match order {
            Some(order) => order[physical],
            None => u64::try_from(id).map_err(|_| bad("canonical ordinal ID"))?,
        };
        if ordinal >= source.rows as u64 {
            return Err(bad("canonical source order"));
        }
        // ponytail: reuse source-order seeks; sequential externally reordered
        // input is the upgrade if measured maintenance I/O warrants it.
        raw.seek(SeekFrom::Start(
            ordinal
                .checked_mul(width as u64)
                .ok_or(bad("canonical seek"))?,
        ))?;
        raw.read_exact(&mut row)?;
        for (value, encoded) in values.iter_mut().zip(row.chunks_exact(4)) {
            *value = f32::from_le_bytes(encoded.try_into().unwrap());
        }
        for (encoded, &value) in row.chunks_exact_mut(4).zip(cosine_vector(&values)?.iter()) {
            encoded.copy_from_slice(&value.to_le_bytes());
        }
        out.write_all(&sq8_row[..8])?;
        out.write_all(&row)?;
        digest.update(&sq8_row[..8]);
        digest.update(&row);
    }
    if sq8.read(&mut [0])? != 0 || format!("{:x}", sq8_digest.finalize()) != source.sq8_sha256 {
        return Err(bad("canonical SQ8 identity"));
    }
    raw.seek(SeekFrom::Start(0))?;
    let mut raw_digest = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = raw.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        raw_digest.update(&buffer[..n]);
    }
    if format!("{:x}", raw_digest.finalize()) != source.raw_sha256 {
        return Err(bad("canonical raw identity"));
    }
    out.flush()?;
    out.get_ref().sync_all()?;
    let sha256 = format!("{:x}", digest.finalize());
    let parent = sq8_key
        .rsplit_once('/')
        .ok_or(bad("canonical object namespace"))?
        .0;
    Ok(CanonicalSource {
        rows: source.rows,
        dimensions: source.dimensions,
        bytes: bytes as u64,
        object_key: format!("{parent}/{sha256}"),
        sha256,
    })
}

/// Maintenance SDK calls and delivered bytes, including failed source responses.
/// Transport-internal retries and wire bytes are separately measured by callers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CanonicalRecoveryStats {
    /// Submitted root/source GET calls, not hidden transport retry attempts.
    pub submitted_gets: usize,
    /// Bytes returned by the completed bounded root read.
    pub root_response_bytes: u64,
    /// Source chunks delivered before success or failure; may be unverified.
    pub source_response_bytes: u64,
}

/// Recovery failure retains maintenance call/byte charges.
#[derive(Debug)]
pub struct CanonicalRecoveryFailure {
    /// Identity, budget, I/O or object-store failure.
    pub error: TwoBitStoreError,
    /// Submitted calls and bytes observed before failure.
    pub stats: CanonicalRecoveryStats,
}
impl std::fmt::Display for CanonicalRecoveryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "canonical source recovery: {}", self.error)
    }
}
impl std::error::Error for CanonicalRecoveryFailure {}

/// Recover canonical source to a new local maintenance file. Query open/search
/// never call this. Bounded streaming, exact length/SHA, atomic no-clobber rename.
/// Source size is admitted against caller disk budget before the source GET.
/// Payload includes root metadata (up to 128KiB) plus a source chunk; caller owns
/// transport buffers/overhead. Oversized transport chunks reject without publish.
pub async fn recover_two_bit_source(
    store: &dyn ObjectStore,
    base: &TwoBitHead,
    output: &Path,
    max_source_bytes: u64,
    max_buffer_bytes: usize,
) -> std::result::Result<CanonicalRecoveryStats, CanonicalRecoveryFailure> {
    let mut stats = CanonicalRecoveryStats::default();
    let result = async {
        let bad = TwoBitStoreError::Invalid;
        if output.exists() || max_source_bytes == 0 || max_buffer_bytes == 0 {
            return Err(bad("canonical recovery admission"));
        }
        stats.submitted_gets += 1;
        let (root, _) =
            small_object(store, &base.metadata_prefix().join("manifest.json"), 65536).await?;
        stats.root_response_bytes = root.len() as u64;
        if format!("{:x}", Sha256::digest(&root)) != base.root_sha256() {
            return Err(bad("canonical root identity"));
        }
        let manifest: Manifest =
            serde_json::from_slice(&root).map_err(|_| bad("canonical root schema"))?;
        if manifest.schema != "borsuk-two-bit-generation-v2"
            || manifest.generation != base.generation()
            || !manifest.canonical.valid()
            || manifest.canonical.dimensions != base.dimensions()
            || manifest.canonical.bytes > max_source_bytes
        {
            return Err(bad("canonical descriptor or disk cap"));
        }
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let temporary = tempfile::NamedTempFile::new_in(parent)?;
        let mut out = tokio::fs::File::from_std(temporary.as_file().try_clone()?);
        stats.submitted_gets += 1;
        let fetched = store
            .get(&ObjectPath::from(manifest.canonical.object_key.clone()))
            .await?;
        if fetched.meta.size != manifest.canonical.bytes {
            return Err(bad("canonical object length"));
        }
        let mut digest = Sha256::new();
        let mut stream = fetched.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            stats.source_response_bytes = stats
                .source_response_bytes
                .saturating_add(chunk.len() as u64);
            if chunk.len() > max_buffer_bytes
                || stats.source_response_bytes > manifest.canonical.bytes
            {
                return Err(bad("canonical source response cap"));
            }
            digest.update(&chunk);
            out.write_all(&chunk).await?;
        }
        if stats.source_response_bytes != manifest.canonical.bytes
            || format!("{:x}", digest.finalize()) != manifest.canonical.sha256
        {
            return Err(bad("canonical source SHA or length"));
        }
        out.flush().await?;
        out.sync_all().await?;
        drop(out);
        temporary
            .persist_noclobber(output)
            .map_err(|e| TwoBitStoreError::Io(e.error))?;
        Ok(())
    }
    .await;
    result
        .map(|()| stats)
        .map_err(|error| CanonicalRecoveryFailure { error, stats })
}
