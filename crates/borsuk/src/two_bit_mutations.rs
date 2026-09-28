//! Authenticated latest-state deltas for one immutable object-native generation.
use crate::{
    resident_graph_generation::valid_sha256,
    sq8_source::cosine_vector,
    two_bit_store::{TwoBitHead, TwoBitStoreError, small_object},
};
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"BTMUT001";
const HEADER: usize = 92;
type Result<T> = std::result::Result<T, TwoBitStoreError>;
fn bad(message: &'static str) -> TwoBitStoreError {
    TwoBitStoreError::Invalid(message)
}

/// A logical-ID upsert, or a delete when `vector` is None.
#[derive(Debug, PartialEq)]
pub struct TwoBitMutation {
    /// Signed application ID, independent of physical row order.
    pub id: i64,
    /// Finite nonzero cosine vector; publication normalizes it once.
    pub vector: Option<Vec<f32>>,
}

/// Snapshot caps. Concurrent publishers/recovered pins must be charged separately.
#[derive(Clone, Copy)]
pub struct TwoBitMutationLimits {
    /// Maximum serialized immutable latest-state snapshot, including header.
    pub max_snapshot_bytes: usize,
    /// Conservative simultaneous old/new snapshot, body and input payload cap.
    /// Allocator/runtime/transport overhead is outside this payload model.
    pub max_memory_bytes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadBody {
    schema: String,
    revision: u64,
    sha256: String,
}

/// Recovered authenticated latest state plus an opaque conditional writer token.
/// Contains only bounded pending mutations, never the base vector or ID plane.
pub struct TwoBitMutationSnapshot {
    base_root: String,
    dimensions: usize,
    revision: u64,
    serialized_bytes: usize,
    sha256: String,
    prefix: Path,
    version: UpdateVersion,
    rows: Vec<TwoBitMutation>,
    excluded_ids: Vec<i64>,
}
impl TwoBitMutationSnapshot {
    /// Conditional publication sequence within this immutable generation.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    /// Authenticated immutable snapshot digest.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    /// Immutable logical-ID latest states, sorted by signed ID.
    pub fn rows(&self) -> &[TwoBitMutation] {
        &self.rows
    }
    /// Sorted roster hiding stale base rows, for deletes and replacements alike.
    pub fn excluded_ids(&self) -> &[i64] {
        &self.excluded_ids
    }
    /// Conservative retained snapshot payload charge, including fixed metadata.
    /// Add to the generation's `already_pinned_bytes`; runtime overhead is extra.
    pub fn resident_payload_bytes(&self) -> usize {
        self.rows.capacity() * std::mem::size_of::<TwoBitMutation>()
            + self.excluded_ids.capacity() * 8
            + self
                .rows
                .iter()
                .filter_map(|r| r.vector.as_ref())
                .map(|v| v.capacity() * 4)
                .sum::<usize>()
            + self.prefix.as_ref().len()
            + 4096
    }
}

fn admit(limits: TwoBitMutationLimits, input_bytes: usize) -> Result<()> {
    // Twelve body caps cover simultaneously retained old/new rows (including
    // Vec headers and ID rosters), serialization, put/read copies and merge refs.
    let modeled = limits
        .max_snapshot_bytes
        .checked_mul(12)
        .and_then(|n| n.checked_add(input_bytes.checked_mul(8)?))
        .and_then(|n| n.checked_add(4096))
        .ok_or(bad("mutation memory overflow"))?;
    if limits.max_snapshot_bytes < HEADER || modeled > limits.max_memory_bytes {
        return Err(bad("mutation payload cap"));
    }
    Ok(())
}

fn decode(
    bytes: &[u8],
    digest: &str,
    base: &TwoBitHead,
    dimensions: usize,
    revision: u64,
    version: UpdateVersion,
    limits: TwoBitMutationLimits,
) -> Result<TwoBitMutationSnapshot> {
    admit(limits, 0)?;
    if bytes.len() < HEADER
        || bytes.len() > limits.max_snapshot_bytes
        || !valid_sha256(digest)
        || format!("{:x}", Sha256::digest(bytes)) != digest
        || &bytes[..8] != MAGIC
        || &bytes[8..72] != base.root_sha256().as_bytes()
        || dimensions == 0
        || u32::try_from(dimensions).is_err()
        || u32::from_le_bytes(bytes[72..76].try_into().unwrap()) as usize != dimensions
        || u64::from_le_bytes(bytes[76..84].try_into().unwrap()) != revision
        || revision == 0
    {
        return Err(bad("mutation authenticated header"));
    }
    let count = usize::try_from(u64::from_le_bytes(bytes[84..92].try_into().unwrap()))
        .map_err(|_| bad("mutation row count"))?;
    if count > (bytes.len() - HEADER) / 9 {
        return Err(bad("mutation row count"));
    }
    let mut rows = Vec::new();
    let mut excluded_ids = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| bad("mutation row allocation"))?;
    excluded_ids
        .try_reserve_exact(count)
        .map_err(|_| bad("mutation ID allocation"))?;
    let mut offset = HEADER;
    for _ in 0..count {
        let end = offset.checked_add(9).ok_or(bad("mutation row width"))?;
        let header = bytes
            .get(offset..end)
            .ok_or(bad("mutation truncated row"))?;
        let id = i64::from_le_bytes(header[..8].try_into().unwrap());
        if excluded_ids.last().is_some_and(|&old| id <= old) {
            return Err(bad("mutation ID order"));
        }
        offset = end;
        let vector = match header[8] {
            0 => None,
            1 => {
                let width = dimensions
                    .checked_mul(4)
                    .ok_or(bad("mutation vector width"))?;
                let end = offset
                    .checked_add(width)
                    .ok_or(bad("mutation vector width"))?;
                let data = bytes
                    .get(offset..end)
                    .ok_or(bad("mutation truncated vector"))?;
                let mut vector = Vec::new();
                vector
                    .try_reserve_exact(dimensions)
                    .map_err(|_| bad("mutation vector allocation"))?;
                for coordinate in data.chunks_exact(4) {
                    vector.push(f32::from_le_bytes(coordinate.try_into().unwrap()));
                }
                let norm = vector.iter().map(|&x| f64::from(x).powi(2)).sum::<f64>();
                if vector.iter().any(|x| !x.is_finite()) || (norm - 1.).abs() > 1e-6 {
                    return Err(bad("mutation normalized vector"));
                }
                offset = end;
                Some(vector)
            }
            _ => return Err(bad("mutation operation")),
        };
        excluded_ids.push(id);
        rows.push(TwoBitMutation { id, vector });
    }
    if offset != bytes.len() {
        return Err(bad("mutation trailing bytes"));
    }
    Ok(TwoBitMutationSnapshot {
        base_root: base.root_sha256().into(),
        dimensions,
        revision,
        serialized_bytes: bytes.len(),
        sha256: digest.into(),
        prefix: base.metadata_prefix(),
        version,
        rows,
        excluded_ids,
    })
}

/// Recover the latest mutation state from an application-authorized base head.
/// ACLs authorize head writers; digests alone are not writer authorization.
pub async fn read_two_bit_mutations(
    store: &dyn ObjectStore,
    base: &TwoBitHead,
    dimensions: usize,
    limits: TwoBitMutationLimits,
) -> Result<Option<TwoBitMutationSnapshot>> {
    admit(limits, base.metadata_prefix().as_ref().len())?;
    if dimensions != base.dimensions() {
        return Err(bad("mutation base dimensions"));
    }
    let prefix = base.metadata_prefix();
    let (body, version) =
        match small_object(store, &prefix.clone().join("mutation-head.json"), 1024).await {
            Ok(v) => v,
            Err(TwoBitStoreError::Store(object_store::Error::NotFound { .. })) => return Ok(None),
            Err(e) => return Err(e),
        };
    let head: HeadBody = serde_json::from_slice(&body).map_err(|_| bad("mutation head schema"))?;
    if head.schema != "borsuk-two-bit-mutation-head-v1"
        || head.revision == 0
        || !valid_sha256(&head.sha256)
        || (version.e_tag.is_none() && version.version.is_none())
    {
        return Err(bad("mutation head authority"));
    }
    let (bytes, _) = small_object(
        store,
        &prefix.join("mutations").join(head.sha256.as_str()),
        limits.max_snapshot_bytes as u64,
    )
    .await?;
    decode(
        &bytes,
        &head.sha256,
        base,
        dimensions,
        head.revision,
        version,
        limits,
    )
    .map(Some)
}

/// Atomically publish a sorted unique batch over the exact recovered latest state.
/// A stale writer fails CAS: reread/reapply the batch rather than overwrite state.
/// Empty batches reject. Cap exhaustion requires compaction, never dropping rows.
/// This primitive persists rows; query overlay scoring and compaction are separate.
pub async fn apply_two_bit_mutations(
    store: &dyn ObjectStore,
    base: &TwoBitHead,
    dimensions: usize,
    expected: Option<&TwoBitMutationSnapshot>,
    updates: &[TwoBitMutation],
    limits: TwoBitMutationLimits,
) -> Result<TwoBitMutationSnapshot> {
    if dimensions == 0
        || dimensions != base.dimensions()
        || u32::try_from(dimensions).is_err()
        || updates.is_empty()
        || updates.windows(2).any(|r| r[0].id >= r[1].id)
        || expected.is_some_and(|s| {
            s.base_root != base.root_sha256()
                || s.prefix != base.metadata_prefix()
                || s.dimensions != dimensions
                || s.serialized_bytes > limits.max_snapshot_bytes
        })
    {
        return Err(bad("mutation input or namespace"));
    }
    let input_bytes = updates
        .iter()
        .try_fold(0usize, |n, r| {
            n.checked_add(std::mem::size_of::<TwoBitMutation>())?
                .checked_add(
                    r.vector
                        .as_ref()
                        .map_or(Some(0), |v| v.capacity().checked_mul(4))?,
                )
        })
        .ok_or(bad("mutation input width"))?;
    admit(
        limits,
        input_bytes
            .checked_add(base.metadata_prefix().as_ref().len())
            .ok_or(bad("mutation input width"))?,
    )?;
    // Validate the entire incoming batch before staging even one object.
    for update in updates {
        if let Some(vector) = &update.vector {
            if vector.len() != dimensions || vector.iter().any(|x| !x.is_finite()) {
                return Err(bad("mutation input vector"));
            }
            cosine_vector(vector).map_err(|_| bad("mutation input norm"))?;
        }
    }
    let revision = expected
        .map_or(0, |s| s.revision)
        .checked_add(1)
        .ok_or(bad("mutation revision overflow"))?;
    // ponytail: rewrite the bounded latest-state delta per batch; compact into
    // a new base before it hits the cap rather than accumulate an unbounded log.
    let mut old = expected
        .map_or(&[][..], |s| s.rows.as_slice())
        .iter()
        .peekable();
    let mut new = updates.iter().peekable();
    let mut selected = Vec::new();
    let max_rows = expected
        .map_or(0, |s| s.rows.len())
        .checked_add(updates.len())
        .ok_or(bad("mutation row count"))?;
    selected
        .try_reserve_exact(max_rows)
        .map_err(|_| bad("mutation merge allocation"))?;
    while old.peek().is_some() || new.peek().is_some() {
        let row = match (old.peek(), new.peek()) {
            (Some(a), Some(b)) if a.id < b.id => old.next().unwrap(),
            (Some(a), Some(b)) if a.id == b.id => {
                old.next();
                new.next().unwrap()
            }
            (_, Some(_)) => new.next().unwrap(),
            _ => old.next().unwrap(),
        };
        selected.push(row);
    }
    let size = selected
        .iter()
        .try_fold(HEADER, |n, row| {
            n.checked_add(9)?.checked_add(if row.vector.is_some() {
                dimensions.checked_mul(4)?
            } else {
                0
            })
        })
        .ok_or(bad("mutation snapshot width"))?;
    if size > limits.max_snapshot_bytes {
        return Err(bad("mutation snapshot byte cap"));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| bad("mutation serialization allocation"))?;
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(base.root_sha256().as_bytes());
    bytes.extend_from_slice(&(dimensions as u32).to_le_bytes());
    bytes.extend_from_slice(&revision.to_le_bytes());
    bytes.extend_from_slice(&(selected.len() as u64).to_le_bytes());
    for row in selected {
        bytes.extend_from_slice(&row.id.to_le_bytes());
        bytes.push(u8::from(row.vector.is_some()));
        if let Some(vector) = &row.vector {
            for &coordinate in cosine_vector(vector)
                .map_err(|_| bad("mutation input norm"))?
                .iter()
            {
                bytes.extend_from_slice(&coordinate.to_le_bytes());
            }
        }
    }
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let mut snapshot = decode(
        &bytes,
        &sha256,
        base,
        dimensions,
        revision,
        UpdateVersion {
            e_tag: None,
            version: None,
        },
        limits,
    )?;
    let prefix = base.metadata_prefix();
    let location = prefix.clone().join("mutations").join(sha256.as_str());
    let staged = store
        .put_opts(
            &location,
            PutPayload::from(bytes.clone()),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await;
    match staged {
        Ok(_) => {}
        Err(object_store::Error::AlreadyExists { .. }) => {
            let (existing, _) =
                small_object(store, &location, limits.max_snapshot_bytes as u64).await?;
            if existing != bytes {
                return Err(bad("mutation immutable collision"));
            }
        }
        Err(e) => return Err(e.into()),
    }
    let body = serde_json::to_vec(&HeadBody {
        schema: "borsuk-two-bit-mutation-head-v1".into(),
        revision,
        sha256: sha256.clone(),
    })
    .map_err(|_| bad("mutation head serialization"))?;
    let result = store
        .put_opts(
            &prefix.join("mutation-head.json"),
            PutPayload::from(body),
            PutOptions {
                mode: expected.map_or(PutMode::Create, |s| PutMode::Update(s.version.clone())),
                ..Default::default()
            },
        )
        .await;
    let version = match result {
        Ok(result) => UpdateVersion::from(result),
        Err(error) => {
            // The CAS may have committed while its acknowledgement was lost.
            if let Ok(Some(current)) = read_two_bit_mutations(store, base, dimensions, limits).await
            {
                if current.revision == revision && current.sha256 == sha256 {
                    return Ok(current);
                }
            }
            return Err(error.into());
        }
    };
    if version.e_tag.is_none() && version.version.is_none() {
        return Err(bad("mutation conditional token"));
    }
    snapshot.version = version;
    Ok(snapshot)
}
