//! Authenticated latest-state deltas for one immutable object-native generation.
use crate::{
    exact_sq8_nominee::{ScoredNominee, Sq8ScoreError},
    resident_graph_generation::valid_sha256,
    sq8_source::cosine_vector,
    two_bit_store::{
        HeadBody, MutationState, TwoBitHead, TwoBitStoreError, commit_control, read_control,
        small_object,
    },
};
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path};
use sha2::{Digest, Sha256};
use std::{cmp::Ordering, collections::BinaryHeap};

const MAGIC: &[u8; 8] = b"BTMUT002";
const HEADER: usize = 100;
type Result<T> = std::result::Result<T, TwoBitStoreError>;
fn bad(message: &'static str) -> TwoBitStoreError {
    TwoBitStoreError::Invalid(message)
}

/// A logical-ID upsert, or a delete when `vector` is None.
#[derive(Clone, Debug, PartialEq)]
pub struct TwoBitMutation {
    /// Signed application ID, independent of physical row order.
    pub id: i64,
    /// Finite nonzero cosine vector; publication normalizes it once.
    pub vector: Option<Vec<f32>>,
}

/// Logical candidate from an SQ8 base row or normalized pending FP32 put.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TwoBitMutationHit {
    /// Signed application ID; pending puts have no base physical ordinal.
    pub id: i64,
    /// Normalized squared-L2 score, smaller is better. Base SQ8 is approximate.
    pub score: f32,
}

struct HeapHit(TwoBitMutationHit);
impl Ord for HeapHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .score
            .total_cmp(&other.0.score)
            .then(self.0.id.cmp(&other.0.id))
    }
}
impl PartialOrd for HeapHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for HeapHit {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapHit {}

/// Snapshot caps. Concurrent publishers/recovered pins must be charged separately.
#[derive(Clone, Copy)]
pub struct TwoBitMutationLimits {
    /// Maximum serialized immutable latest-state snapshot, including header.
    pub max_snapshot_bytes: usize,
    /// Conservative simultaneous old/new snapshot, body and input payload cap.
    /// Allocator/runtime/transport overhead is outside this payload model.
    pub max_memory_bytes: usize,
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
    sealed: bool,
}
impl TwoBitMutationSnapshot {
    pub(crate) fn binds(&self, root: &str, dimensions: usize) -> bool {
        self.base_root == root && self.dimensions == dimensions
    }
    pub(crate) fn binds_head(&self, base: &TwoBitHead) -> bool {
        self.binds(base.root_sha256(), base.dimensions()) && self.prefix == base.metadata_prefix()
    }
    pub(crate) fn put_rows(&self) -> usize {
        self.rows.iter().filter(|r| r.vector.is_some()).count()
    }
    pub(crate) fn rank_with_base(
        &self,
        base: &[ScoredNominee],
        query: &[f32],
        top_k: usize,
    ) -> std::result::Result<Vec<TwoBitMutationHit>, Sq8ScoreError> {
        if top_k == 0 || query.len() != self.dimensions || query.iter().any(|x| !x.is_finite()) {
            return Err(Sq8ScoreError::InvalidQuery);
        }
        let normalized = cosine_vector(query).map_err(|_| Sq8ScoreError::InvalidQuery)?;
        let capacity = top_k.min(
            base.len()
                .checked_add(self.put_rows())
                .ok_or(Sq8ScoreError::InvalidRoster)?,
        );
        let mut heap = BinaryHeap::<HeapHit>::new();
        heap.try_reserve_exact(capacity)
            .map_err(|_| Sq8ScoreError::InvalidRoster)?;
        let mut offer = |hit: TwoBitMutationHit| {
            let candidate = HeapHit(hit);
            if heap.len() < capacity {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                *heap.peek_mut().unwrap() = candidate;
            }
        };
        for row in base {
            if !row.score.is_finite() || self.excluded_ids.binary_search(&row.id).is_ok() {
                return Err(Sq8ScoreError::InvalidRoster);
            }
            offer(TwoBitMutationHit {
                id: row.id,
                score: row.score,
            });
        }
        // ponytail: scan the capped pending puts; compact into the base before
        // the delta's measured CPU/write amplification becomes a bottleneck.
        for row in &self.rows {
            let Some(vector) = &row.vector else {
                continue;
            };
            let squared = normalized
                .iter()
                .zip(vector)
                .map(|(&q, &v)| (f64::from(q) - f64::from(v)).powi(2))
                .sum::<f64>() as f32;
            offer(TwoBitMutationHit {
                id: row.id,
                score: squared,
            });
        }
        Ok(heap.into_sorted_vec().into_iter().map(|h| h.0).collect())
    }
    /// Closed to further writes; the snapshot remains readable during compaction.
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }
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

pub(crate) fn admit(limits: TwoBitMutationLimits, input_bytes: usize) -> Result<()> {
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

pub(crate) fn decode(
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
        || u64::from_le_bytes(bytes[92..100].try_into().unwrap()) == 0
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
        sealed: false,
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
    let Some((head, version)) = read_mutation_head(store, base).await? else {
        return Ok(None);
    };
    let prefix = base.metadata_prefix();
    let (bytes, _) = small_object(
        store,
        &prefix.join("mutations").join(head.sha256.as_str()),
        limits.max_snapshot_bytes as u64,
    )
    .await?;
    let mut snapshot = decode(
        &bytes,
        &head.sha256,
        base,
        dimensions,
        head.revision,
        version,
        limits,
    )?;
    snapshot.sealed = head.sealed;
    Ok(Some(snapshot))
}

async fn mutation_authority(
    store: &dyn ObjectStore,
    base: &TwoBitHead,
) -> Result<(HeadBody, UpdateVersion)> {
    let (control, version) = read_control(store, base.index_prefix())
        .await?
        .ok_or(bad("mutation index absent"))?;
    if control.root_sha256 != base.root_sha256() || control.generation != base.generation() {
        return Err(bad("mutation base retired"));
    }
    if control.fence.is_some() {
        return Err(bad("writes fenced for reclamation"));
    }
    Ok((control, version))
}
async fn read_mutation_head(
    store: &dyn ObjectStore,
    base: &TwoBitHead,
) -> Result<Option<(MutationState, UpdateVersion)>> {
    let (control, version) = mutation_authority(store, base).await?;
    Ok(control.mutation.map(|state| (state, version)))
}
pub(crate) async fn require_sealed_two_bit_mutations(
    store: &dyn ObjectStore,
    base: &TwoBitHead,
) -> Result<(HeadBody, UpdateVersion)> {
    let authority = mutation_authority(store, base).await?;
    if !authority.0.mutation.as_ref().is_some_and(|s| s.sealed) {
        return Err(bad("generation replacement requires sealed mutations"));
    }
    Ok(authority)
}

/// Irreversibly fence writers to this base before building its replacement.
/// CAS closes even an absent mutation head. Old snapshots stay readable.
/// A crash after sealing pauses writes; resume compaction from the sealed state.
/// This does not validate replacement contents or perform compaction/GC.
pub async fn seal_two_bit_mutations(
    store: &dyn ObjectStore,
    base: &TwoBitHead,
    expected: Option<&TwoBitMutationSnapshot>,
    limits: TwoBitMutationLimits,
) -> Result<TwoBitMutationSnapshot> {
    publish_mutation_state(store, base, base.dimensions(), expected, &[], limits, true).await
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
    publish_mutation_state(store, base, dimensions, expected, updates, limits, false).await
}

async fn publish_mutation_state(
    store: &dyn ObjectStore,
    base: &TwoBitHead,
    dimensions: usize,
    expected: Option<&TwoBitMutationSnapshot>,
    updates: &[TwoBitMutation],
    limits: TwoBitMutationLimits,
    seal: bool,
) -> Result<TwoBitMutationSnapshot> {
    if dimensions == 0
        || dimensions != base.dimensions()
        || u32::try_from(dimensions).is_err()
        || (!seal && updates.is_empty())
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
    if let Some(previous) = expected.filter(|s| s.sealed) {
        if !seal {
            return Err(bad("mutation generation sealed"));
        }
        let current = read_two_bit_mutations(store, base, dimensions, limits)
            .await?
            .ok_or(bad("sealed mutation head missing"))?;
        if !current.sealed
            || current.revision != previous.revision
            || current.sha256 != previous.sha256
        {
            return Err(bad("sealed mutation head changed"));
        }
        return Ok(current);
    }
    let (mut control, _) = mutation_authority(store, base).await?;
    if match (control.mutation.as_ref(), expected) {
        (None, None) => false,
        (Some(current), Some(previous)) => {
            current.revision != previous.revision
                || current.sha256 != previous.sha256
                || current.sealed != previous.sealed
        }
        _ => true,
    } {
        return Err(bad("mutation head changed"));
    }
    control.advance()?;
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
    // A retried GC can release its fence while an earlier DELETE is still in flight.
    // Never reuse that orphan key in a later control epoch.
    bytes.extend_from_slice(&control.epoch.to_le_bytes());
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
    control.mutation = Some(MutationState {
        revision,
        sha256: sha256.clone(),
        sealed: seal,
    });
    let version = commit_control(
        store,
        base.index_prefix(),
        &control,
        Some(expected.map_or_else(|| base.version.clone(), |s| s.version.clone())),
    )
    .await?;
    snapshot.sealed = seal;
    snapshot.version = version;
    Ok(snapshot)
}
