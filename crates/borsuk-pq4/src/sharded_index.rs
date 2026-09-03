use std::path::PathBuf;

use rayon::prelude::*;

use crate::{BorsukError, Pq4Index, Pq4Match, Pq4OpenOptions, Result, merge_pq4_shard_matches};

/// Aggregate memory and concurrency policy for a local PQ4 shard set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pq4ShardedOpenOptions {
    /// Total memory budget divided across every opened shard.
    pub memory_budget_bytes: u64,
    /// Dedicated coordinator threads; must admit every shard concurrently.
    pub fanout_threads: usize,
    /// Dedicated search threads owned by each local shard.
    pub shard_query_threads: usize,
    /// Maximum time a local shard waits for scratch admission.
    pub admission_timeout_ms: u64,
}

/// Authenticated local shard set with deterministic concurrent search.
pub struct Pq4ShardedIndex {
    shards: Vec<(u32, Pq4Index)>,
    fanout: rayon::ThreadPool,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

impl Pq4ShardedIndex {
    /// Open a contiguous local shard set without network or partial admission.
    pub fn open(shards: &[(u32, PathBuf)], options: Pq4ShardedOpenOptions) -> Result<Self> {
        if shards.is_empty()
            || shards.len() > 256
            || options.fanout_threads < shards.len()
            || options.fanout_threads > 256
            || options.shard_query_threads == 0
            || options.shard_query_threads > 256
            || options.admission_timeout_ms == 0
        {
            return Err(invalid("PQ4 sharded open options differ"));
        }
        if shards
            .iter()
            .enumerate()
            .any(|(expected, (ordinal, path))| {
                usize::try_from(*ordinal).ok() != Some(expected) || !path.is_dir()
            })
        {
            return Err(invalid("PQ4 shard authority differs"));
        }
        let shard_count = u64::try_from(shards.len()).unwrap();
        let per_shard_budget = options.memory_budget_bytes / shard_count;
        if per_shard_budget == 0 {
            return Err(invalid("PQ4 sharded memory budget differs"));
        }
        let mut opened = Vec::with_capacity(shards.len());
        for (shard_ordinal, path) in shards {
            opened.push((
                *shard_ordinal,
                Pq4Index::open(
                    path,
                    Pq4OpenOptions {
                        shard_ordinal: *shard_ordinal,
                        memory_budget_bytes: per_shard_budget,
                        query_threads: options.shard_query_threads,
                        admission_timeout_ms: options.admission_timeout_ms,
                    },
                )?,
            ));
        }
        let fanout = rayon::ThreadPoolBuilder::new()
            .num_threads(options.fanout_threads)
            .thread_name(|index| format!("pq4-fanout-{index}"))
            .build()
            .map_err(|error| invalid(&format!("PQ4 fanout pool failed: {error}")))?;
        Ok(Self {
            shards: opened,
            fanout,
        })
    }

    /// Search every shard concurrently and merge exact local top-k results.
    pub fn search(&self, query: &[f32; 96], k: usize) -> Result<Vec<Pq4Match>> {
        self.search_with_shard_observer(query, k, &|_| {})
    }

    fn search_with_shard_observer<F>(
        &self,
        query: &[f32; 96],
        k: usize,
        observe: &F,
    ) -> Result<Vec<Pq4Match>>
    where
        F: Fn(u32) + Sync,
    {
        let local = self.fanout.install(|| {
            self.shards
                .par_iter()
                .map(|(ordinal, shard)| {
                    observe(*ordinal);
                    shard.search(query, k)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        merge_pq4_shard_matches(local, k)
    }
}

#[cfg(test)]
pub(crate) fn search_with_shard_observer_for_test<F>(
    index: &Pq4ShardedIndex,
    query: &[f32; 96],
    k: usize,
    observe: F,
) -> Result<Vec<Pq4Match>>
where
    F: Fn(u32) + Sync,
{
    index.search_with_shard_observer(query, k, &observe)
}
