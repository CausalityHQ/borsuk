//! One logical-ID search API for populated and truly empty immutable bases.
use crate::{
    budgeted_page_rank::BudgetedPagePlan,
    sq8_s3_range::{OneAttemptS3, Sq8ReadStats},
    two_bit_generation::{
        TwoBitGeneration, TwoBitGenerationError, TwoBitGenerationLimits, TwoBitMutationSearchResult,
    },
    two_bit_mutations::{TwoBitMutationHit, TwoBitMutationSnapshot},
    two_bit_store::{EmptyRoot, TwoBitHead, TwoBitStoreError, small_object},
};
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::sync::Semaphore;

/// Pinned immutable base; vectors remain object-native. Pending deltas are
/// caller-owned authenticated snapshots, charged with all other retained pins.
pub struct TwoBitIndex {
    head: TwoBitHead,
    base: Option<TwoBitGeneration>,
    limits: TwoBitGenerationLimits,
    modeled_bytes: u64,
    slots: Semaphore,
    _lifecycle: Option<std::fs::File>,
}
impl TwoBitIndex {
    /// Open latest head while holding a shared lifetime lock until this index drops.
    /// Use the SAME maintenance directory as compaction and future exclusive GC.
    /// Only cooperating readers/writers on one host/shared local filesystem are
    /// protected; other hosts and direct open_remote callers remain unmanaged.
    /// Lockfiles must never be replaced/unlinked while participants are alive.
    pub async fn open_coordinated(
        store: &dyn ObjectStore,
        prefix: &object_store::path::Path,
        maintenance_directory: &Path,
        limits: TwoBitGenerationLimits,
        scratch_parent: &Path,
    ) -> Result<Self, TwoBitStoreError> {
        let lifetime = crate::two_bit_compaction::shared_lifecycle(prefix, maintenance_directory)?;
        let head = crate::two_bit_store::read_two_bit_head(store, prefix)
            .await?
            .ok_or(TwoBitStoreError::Invalid("index absent"))?;
        let mut index = Self::open_remote(store, head, limits, scratch_parent).await?;
        index._lifecycle = Some(lifetime);
        Ok(index)
    }

    /// Open populated routing metadata or one small empty root, without vector
    /// hydration. The head must come from the same authorized store/prefix.
    pub async fn open_remote(
        store: &dyn ObjectStore,
        head: TwoBitHead,
        limits: TwoBitGenerationLimits,
        scratch_parent: &Path,
    ) -> Result<Self, TwoBitStoreError> {
        let bad = TwoBitStoreError::Invalid;
        if limits.max_active_queries == 0
            || limits.max_query_bytes == 0
            || limits.max_query_gets == 0
            || limits.max_parallel_gets == 0
            || limits.max_query_scratch_bytes == 0
        {
            return Err(bad("index admission"));
        }
        let fixed = (head.metadata_prefix().as_ref().len() as u64)
            .checked_mul(2)
            .and_then(|n| n.checked_add(4096))
            .ok_or(bad("index memory"))?;
        let (base, modeled_bytes) = if head.is_empty() {
            let query_bytes = (head.dimensions() as u64)
                .checked_mul(16)
                .and_then(|n| n.checked_add(limits.max_query_scratch_bytes as u64))
                .and_then(|n| n.checked_add(4096))
                .and_then(|n| n.checked_mul(limits.max_active_queries as u64))
                .ok_or(bad("empty query memory"))?;
            let modeled = fixed
                .checked_add(131072)
                .and_then(|n| n.checked_add(query_bytes))
                .and_then(|n| n.checked_add(limits.already_pinned_bytes))
                .ok_or(bad("empty index memory"))?;
            if modeled > limits.max_memory_bytes {
                return Err(bad("empty index memory"));
            }
            let (bytes, _) =
                small_object(store, &head.metadata_prefix().join("manifest.json"), 1024).await?;
            if format!("{:x}", Sha256::digest(&bytes)) != head.root_sha256() {
                return Err(bad("empty root identity"));
            }
            let root: EmptyRoot =
                serde_json::from_slice(&bytes).map_err(|_| bad("empty root schema"))?;
            if !root.valid()
                || root.generation != head.generation()
                || root.dimensions != head.dimensions()
            {
                return Err(bad("empty root binding"));
            }
            (None, modeled)
        } else {
            // Charge wrapper/head payload before the existing loader allocates.
            let loader_limits = TwoBitGenerationLimits {
                already_pinned_bytes: limits
                    .already_pinned_bytes
                    .checked_add(fixed)
                    .ok_or(bad("index memory"))?,
                ..limits
            };
            let base = TwoBitGeneration::open_remote(
                store,
                &head.metadata_prefix(),
                head.root_sha256(),
                loader_limits,
                scratch_parent,
            )
            .await?;
            let modeled = base.modeled_memory_bytes();
            (Some(base), modeled)
        };
        Ok(Self {
            head,
            base,
            limits,
            modeled_bytes,
            slots: Semaphore::new(limits.max_active_queries),
            _lifecycle: None,
        })
    }

    /// Pinned identity and conditional token for mutation recovery/publication.
    pub fn head(&self) -> &TwoBitHead {
        &self.head
    }

    /// Up to k logical IDs, including a truly empty result. The same query slot
    /// spans nomination, I/O and pending scoring. No base GET on an empty root.
    /// Snapshot namespace/root/dimensions must match this index; charge all pins.
    pub async fn search(
        &self,
        reader: &OneAttemptS3,
        query: &[f32],
        top_k: usize,
        mutations: Option<&TwoBitMutationSnapshot>,
    ) -> Result<TwoBitMutationSearchResult, TwoBitGenerationError> {
        let bad = TwoBitGenerationError::Invalid;
        if top_k == 0
            || query.len() != self.head.dimensions()
            || query
                .len()
                .checked_mul(4)
                .is_none_or(|n| n > self.limits.max_query_scratch_bytes)
            || mutations.is_some_and(|m| {
                !m.binds_head(&self.head)
                    || m.resident_payload_bytes() as u64 > self.limits.already_pinned_bytes
            })
        {
            return Err(bad("index query/binding/admission"));
        }
        let fetched = self.base.as_ref().map_or(0, |b| {
            b.rows()
                .min(self.limits.max_query_bytes / (self.head.dimensions() + 12))
        });
        let capacity = top_k.min(
            fetched
                .checked_add(mutations.map_or(0, |m| m.put_rows()))
                .ok_or(bad("index query memory"))?,
        );
        let modeled = (capacity as u64)
            .checked_mul(32)
            .and_then(|n| n.checked_add(4096))
            .and_then(|n| n.checked_mul(self.limits.max_active_queries as u64))
            .and_then(|n| n.checked_add(self.modeled_bytes))
            .ok_or(bad("index query memory"))?;
        if modeled > self.limits.max_memory_bytes {
            return Err(bad("index query memory"));
        }
        let _slot = self
            .slots
            .acquire()
            .await
            .map_err(|_| bad("index query admission"))?;
        if let Some(base) = &self.base {
            if let Some(mutations) = mutations {
                return base
                    .search_with_mutations(reader, query, top_k, mutations)
                    .await;
            }
            let result = base
                .search_excluding(reader, query, top_k.min(base.rows()), &[])
                .await?;
            return Ok(TwoBitMutationSearchResult {
                plan: result.plan,
                candidates: result
                    .ranked
                    .candidates
                    .into_iter()
                    .map(|h| TwoBitMutationHit {
                        id: h.id,
                        score: h.score,
                    })
                    .collect(),
                stats: result.ranked.stats,
                source_stats: result.source_stats,
                mutation_rows_scanned: 0,
                mutation_put_rows_scored: 0,
                mutation_revision: 0,
                mutation_sha256: String::new(),
            });
        }
        let candidates = if let Some(mutations) = mutations {
            mutations
                .rank_with_base(&[], query, top_k)
                .map_err(|_| bad("empty mutation scoring"))?
        } else {
            crate::sq8_source::cosine_vector(query).map_err(TwoBitGenerationError::Plane)?;
            Vec::new()
        };
        Ok(TwoBitMutationSearchResult {
            plan: BudgetedPagePlan {
                selected_pages: Vec::new(),
                ranges: Vec::new(),
                planned_bytes: 0,
                primary_pages_retained: 0,
                target_pages: 0,
                target_shortfall: 0,
            },
            candidates,
            stats: Sq8ReadStats::default(),
            source_stats: Sq8ReadStats::default(),
            mutation_rows_scanned: mutations.map_or(0, |m| m.rows().len()),
            mutation_put_rows_scored: mutations.map_or(0, |m| m.put_rows()),
            mutation_revision: mutations.map_or(0, |m| m.revision()),
            mutation_sha256: mutations.map_or_else(String::new, |m| m.sha256().to_owned()),
        })
    }
}
