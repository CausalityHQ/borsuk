use std::collections::BTreeSet;

use crate::{BorsukError, Pq4Match, Result};

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidMetricInput(message.to_owned())
}

/// Merge exact local top-k lists without dependence on shard arrival order.
pub fn merge_pq4_shard_matches(shards: Vec<Vec<Pq4Match>>, k: usize) -> Result<Vec<Pq4Match>> {
    if shards.is_empty() || k == 0 {
        return Err(invalid("PQ4 shard merge request differs"));
    }
    let mut shard_ordinals = BTreeSet::new();
    let mut merged = Vec::new();
    for shard in shards {
        if shard.is_empty() {
            continue;
        }
        let shard_ordinal = shard[0].shard_ordinal;
        if !shard_ordinals.insert(shard_ordinal)
            || shard.iter().any(|item| {
                item.shard_ordinal != shard_ordinal
                    || !item.squared_distance.is_finite()
                    || item.squared_distance < 0.0
            })
            || shard.windows(2).any(|pair| {
                pair[0]
                    .squared_distance
                    .total_cmp(&pair[1].squared_distance)
                    .then_with(|| pair[0].source_ordinal.cmp(&pair[1].source_ordinal))
                    .is_gt()
                    || pair[0].source_ordinal == pair[1].source_ordinal
            })
        {
            return Err(invalid("PQ4 local shard results differ"));
        }
        merged.extend(shard);
    }
    if merged.is_empty() {
        return Err(invalid("PQ4 shard results are empty"));
    }
    merged.sort_unstable_by(|left, right| {
        left.squared_distance
            .total_cmp(&right.squared_distance)
            .then_with(|| left.shard_ordinal.cmp(&right.shard_ordinal))
            .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
    });
    merged.truncate(k);
    Ok(merged)
}
