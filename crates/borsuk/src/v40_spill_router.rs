use crate::error::{BorsukError, Result};
use crate::v37_relation_router::{
    V37BalancedTree, V37FeatureGroundTruth, select_v37_tree_postings_with_limit,
};
use crate::v38_boundary_spill::{
    V38PostingSummary, V38SpillRecord, v38_v40_evaluation_binding,
    v38_v40_owner_rows_from_artifacts, validate_v38_posting_summaries,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, Float32Array, ListArray, RecordBatch, StringArray, UInt8Array, UInt32Array, UInt64Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const V40_MAXIMUM_FRONTIER_POSTINGS: usize = 64;
const V40_MAXIMUM_NODE_POPS: usize = 1_024;
const V40_DIRECT_QUERY_COUNT: u64 = 1_000;
const V40_DIRECT_SELECTED_POSTINGS: usize = 21;
const V40_DEVELOPMENT_CORPUS_ROWS: usize = 1_000_000;
const V40_DEVELOPMENT_POSTING_COUNT: u32 = 123;
const V40_DEVELOPMENT_MAXIMUM_ROWS_PER_POSTING: u32 = 10_240;
const V40_Q24_TOTAL: u32 = 1 << 24;
const V40_MAXIMUM_ALTERNATES_PER_POSTING: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V40SpillCount {
    pub(crate) primary_posting: u32,
    pub(crate) alternate_posting: Option<u32>,
    pub(crate) count: u64,
    pub(crate) primary_population: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40PackedSpillSummary {
    pub(crate) offsets: Vec<u64>,
    pub(crate) alternate_postings: Vec<u32>,
    pub(crate) masses_q24: Vec<u32>,
    pub(crate) residual_masses_q24: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V40RouterArm {
    DirectTree,
    AcceptedSpill,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40Selection {
    pub(crate) arm: V40RouterArm,
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) objective_value: u128,
    pub(crate) candidate_count: u32,
    pub(crate) marginal_recomputations: u64,
}

pub(crate) fn build_v40_spill_counts(
    records: &[V38SpillRecord],
    summaries: &[V38PostingSummary],
) -> Result<Vec<V40SpillCount>> {
    let invalid = || BorsukError::InvalidStorage("V40 spill count authority differs".to_owned());
    validate_v38_posting_summaries(summaries)?;
    let posting_count = u32::try_from(summaries.len()).map_err(|_| invalid())?;
    if posting_count == 0
        || summaries.iter().enumerate().any(|(posting, summary)| {
            summary.posting_ordinal as usize != posting
                || summary.primary_population == 0
                || summary.total_population
                    != summary
                        .primary_population
                        .checked_add(summary.alternate_population)
                        .unwrap_or(u64::MAX)
        })
    {
        return Err(invalid());
    }

    let mut next_primary = vec![0_u64; summaries.len()];
    let mut next_alternate = summaries
        .iter()
        .map(|summary| summary.primary_population)
        .collect::<Vec<_>>();
    let mut previous_key = None;
    let mut row = 0_usize;
    while row < records.len() {
        let primary = records.get(row).ok_or_else(invalid)?;
        let key = (primary.source_ordinal, primary.owner_role);
        if primary.owner_role != 0
            || primary.alternate_violation_bits.is_some()
            || previous_key.is_some_and(|previous| key <= previous)
            || primary.posting_ordinal >= posting_count
            || u64::from(primary.posting_local_ordinal)
                != next_primary[primary.posting_ordinal as usize]
        {
            return Err(invalid());
        }
        next_primary[primary.posting_ordinal as usize] = next_primary
            [primary.posting_ordinal as usize]
            .checked_add(1)
            .ok_or_else(invalid)?;
        let alternate = records.get(row + 1).filter(|record| {
            record.source_ordinal == primary.source_ordinal && record.owner_role == 1
        });
        if let Some(alternate) = alternate {
            let value = alternate
                .alternate_violation_bits
                .map(f32::from_bits)
                .ok_or_else(invalid)?;
            if alternate.feature_row_id != primary.feature_row_id
                || alternate.posting_ordinal >= posting_count
                || alternate.posting_ordinal == primary.posting_ordinal
                || !value.is_finite()
                || value.total_cmp(&0.0).is_lt()
                || u64::from(alternate.posting_local_ordinal)
                    != next_alternate[alternate.posting_ordinal as usize]
            {
                return Err(invalid());
            }
            next_alternate[alternate.posting_ordinal as usize] = next_alternate
                [alternate.posting_ordinal as usize]
                .checked_add(1)
                .ok_or_else(invalid)?;
            previous_key = Some((alternate.source_ordinal, alternate.owner_role));
            row += 2;
        } else {
            previous_key = Some(key);
            row += 1;
        }
    }
    if records.is_empty()
        || summaries.iter().enumerate().any(|(posting, summary)| {
            next_primary[posting] != summary.primary_population
                || next_alternate[posting] != summary.total_population
        })
    {
        return Err(invalid());
    }

    let mut counts = Vec::new();
    for (posting, summary) in summaries.iter().enumerate() {
        let mut alternates = BTreeMap::<u32, u64>::new();
        let mut residual = 0_u64;
        let mut row = 0_usize;
        while row < records.len() {
            let primary = &records[row];
            let alternate = records.get(row + 1).filter(|record| {
                record.source_ordinal == primary.source_ordinal && record.owner_role == 1
            });
            if primary.posting_ordinal as usize == posting {
                if let Some(alternate) = alternate {
                    let count = alternates.entry(alternate.posting_ordinal).or_default();
                    *count = count.checked_add(1).ok_or_else(invalid)?;
                } else {
                    residual = residual.checked_add(1).ok_or_else(invalid)?;
                }
            }
            row += usize::from(alternate.is_some()) + 1;
        }
        let mut alternates = alternates.into_iter().collect::<Vec<_>>();
        alternates.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        let observed = alternates
            .iter()
            .try_fold(residual, |total, (_, count)| total.checked_add(*count))
            .ok_or_else(invalid)?;
        if observed != summary.primary_population || summary.posting_ordinal as usize != posting {
            return Err(invalid());
        }
        counts.extend(
            alternates
                .into_iter()
                .map(|(alternate, count)| V40SpillCount {
                    primary_posting: posting as u32,
                    alternate_posting: Some(alternate),
                    count,
                    primary_population: summary.primary_population,
                }),
        );
        counts.push(V40SpillCount {
            primary_posting: posting as u32,
            alternate_posting: None,
            count: residual,
            primary_population: summary.primary_population,
        });
    }
    Ok(counts)
}

#[cfg(test)]
fn build_v40_spill_counts_from_owners(
    owners: &[(u64, u32, Option<u32>)],
    posting_count: u32,
) -> Result<Vec<V40SpillCount>> {
    let invalid = || BorsukError::InvalidStorage("V40 spill owner authority differs".to_owned());
    if owners.is_empty() || posting_count == 0 {
        return Err(invalid());
    }
    let mut populations = vec![0_u64; posting_count as usize];
    let mut alternates = vec![BTreeMap::<u32, u64>::new(); posting_count as usize];
    let mut previous_feature = None;
    for &(feature, primary, alternate) in owners {
        if previous_feature.is_some_and(|previous| feature <= previous)
            || primary >= posting_count
            || alternate.is_some_and(|value| value >= posting_count || value == primary)
        {
            return Err(invalid());
        }
        previous_feature = Some(feature);
        populations[primary as usize] = populations[primary as usize]
            .checked_add(1)
            .ok_or_else(invalid)?;
        if let Some(alternate) = alternate {
            let count = alternates[primary as usize].entry(alternate).or_default();
            *count = count.checked_add(1).ok_or_else(invalid)?;
        }
    }
    let mut counts = Vec::new();
    for primary in 0..posting_count {
        let population = populations[primary as usize];
        if population == 0 {
            return Err(invalid());
        }
        let mut rows = alternates[primary as usize]
            .iter()
            .map(|(&alternate, &count)| V40SpillCount {
                primary_posting: primary,
                alternate_posting: Some(alternate),
                count,
                primary_population: population,
            })
            .collect::<Vec<_>>();
        rows.sort_unstable_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.alternate_posting.cmp(&right.alternate_posting))
        });
        let alternate_total = rows
            .iter()
            .try_fold(0_u64, |total, row| total.checked_add(row.count))
            .ok_or_else(invalid)?;
        rows.push(V40SpillCount {
            primary_posting: primary,
            alternate_posting: None,
            count: population
                .checked_sub(alternate_total)
                .ok_or_else(invalid)?,
            primary_population: population,
        });
        counts.extend(rows);
    }
    pack_v40_spill_summary(&counts, posting_count)?;
    Ok(counts)
}

fn v40_source_spill_relation_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("owner_role", DataType::UInt8, false),
        Field::new("posting_local_ordinal", DataType::UInt32, false),
        Field::new("alternate_violation", DataType::Float32, true),
    ])
}

fn v40_source_posting_summary_schema() -> Schema {
    Schema::new(vec![
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("primary_population", DataType::UInt64, false),
        Field::new("alternate_population", DataType::UInt64, false),
        Field::new("total_population", DataType::UInt64, false),
        Field::new("projected_payload_bytes", DataType::UInt64, false),
        Field::new("projected_framing_allowance_bytes", DataType::UInt32, false),
    ])
}

fn build_v40_spill_counts_from_parquet_files(
    relation_file: File,
    posting_file: File,
    source_rows: usize,
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V40SpillCount>> {
    let invalid =
        || BorsukError::InvalidStorage("V40 streaming spill count authority differs".to_owned());
    if source_rows == 0 || posting_count == 0 || maximum_rows_per_posting == 0 {
        return Err(invalid());
    }
    let relation_builder = ParquetRecordBatchReaderBuilder::try_new(relation_file)?;
    let relation_rows = usize::try_from(relation_builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid())?;
    if relation_builder.schema().as_ref() != &v40_source_spill_relation_schema()
        || relation_builder.metadata().num_row_groups() == 0
        || relation_rows < source_rows
        || relation_rows > source_rows.checked_mul(2).ok_or_else(invalid)?
    {
        return Err(invalid());
    }
    let postings = posting_count as usize;
    let mut primary_populations = vec![0_u64; postings];
    let mut alternate_populations = vec![0_u64; postings];
    let mut next_primary_local = vec![0_u32; postings];
    let mut first_alternate_local = vec![None; postings];
    let mut next_alternate_local = vec![0_u32; postings];
    let mut alternate_counts = vec![BTreeMap::<u32, u64>::new(); postings];
    let mut expected_source = 0_u64;
    let mut pending_primary: Option<(u64, u64, u32)> = None;
    for batch in relation_builder.build()? {
        let batch = batch?;
        if batch.schema().as_ref() != &v40_source_spill_relation_schema()
            || batch.columns()[..5]
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid());
        }
        let sources = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        let features = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        let posting_ordinals = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let roles = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(invalid)?;
        let local_ordinals = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let violations = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(invalid)?;
        for row in 0..batch.num_rows() {
            let source = sources.value(row);
            let feature = features.value(row);
            let posting = posting_ordinals.value(row);
            let posting_index = usize::try_from(posting).map_err(|_| invalid())?;
            if posting_index >= postings {
                return Err(invalid());
            }
            match roles.value(row) {
                0 => {
                    if source != expected_source
                        || violations.is_valid(row)
                        || local_ordinals.value(row) != next_primary_local[posting_index]
                    {
                        return Err(invalid());
                    }
                    expected_source = expected_source.checked_add(1).ok_or_else(invalid)?;
                    next_primary_local[posting_index] = next_primary_local[posting_index]
                        .checked_add(1)
                        .ok_or_else(invalid)?;
                    primary_populations[posting_index] = primary_populations[posting_index]
                        .checked_add(1)
                        .ok_or_else(invalid)?;
                    pending_primary = Some((source, feature, posting));
                }
                1 => {
                    let (primary_source, primary_feature, primary_posting) =
                        pending_primary.take().ok_or_else(invalid)?;
                    let violation = violations.value(row);
                    let local = local_ordinals.value(row);
                    if source != primary_source
                        || feature != primary_feature
                        || posting == primary_posting
                        || !violations.is_valid(row)
                        || !violation.is_finite()
                        || violation.total_cmp(&0.0).is_lt()
                    {
                        return Err(invalid());
                    }
                    if let Some(expected) = first_alternate_local[posting_index] {
                        if local != next_alternate_local[posting_index] || local < expected {
                            return Err(invalid());
                        }
                    } else {
                        first_alternate_local[posting_index] = Some(local);
                        next_alternate_local[posting_index] = local;
                    }
                    next_alternate_local[posting_index] = next_alternate_local[posting_index]
                        .checked_add(1)
                        .ok_or_else(invalid)?;
                    alternate_populations[posting_index] = alternate_populations[posting_index]
                        .checked_add(1)
                        .ok_or_else(invalid)?;
                    let count = alternate_counts[primary_posting as usize]
                        .entry(posting)
                        .or_default();
                    *count = count.checked_add(1).ok_or_else(invalid)?;
                }
                _ => return Err(invalid()),
            }
        }
    }
    if expected_source != source_rows as u64 {
        return Err(invalid());
    }
    for posting in 0..postings {
        if primary_populations[posting] == 0
            || first_alternate_local[posting].is_some_and(|first| {
                u64::from(first) != primary_populations[posting]
                    || u64::from(next_alternate_local[posting])
                        != primary_populations[posting] + alternate_populations[posting]
            })
            || first_alternate_local[posting].is_none() && alternate_populations[posting] != 0
        {
            return Err(invalid());
        }
    }

    let posting_builder = ParquetRecordBatchReaderBuilder::try_new(posting_file)?;
    if posting_builder.schema().as_ref() != &v40_source_posting_summary_schema()
        || posting_builder.metadata().num_row_groups() == 0
        || posting_builder.metadata().file_metadata().num_rows() != i64::from(posting_count)
    {
        return Err(invalid());
    }
    let mut observed_postings = 0_usize;
    for batch in posting_builder.build()? {
        let batch = batch?;
        if batch.schema().as_ref() != &v40_source_posting_summary_schema()
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid());
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let primary = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        let alternate = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        let totals = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        let payloads = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        let framing = batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        for row in 0..batch.num_rows() {
            let posting = observed_postings.checked_add(row).ok_or_else(invalid)?;
            let total = primary_populations[posting]
                .checked_add(alternate_populations[posting])
                .ok_or_else(invalid)?;
            if ordinals.value(row) as usize != posting
                || primary.value(row) != primary_populations[posting]
                || alternate.value(row) != alternate_populations[posting]
                || totals.value(row) != total
                || total > u64::from(maximum_rows_per_posting)
                || payloads.value(row) != total.checked_mul(48).ok_or_else(invalid)?
                || framing.value(row) != 32_768
            {
                return Err(invalid());
            }
        }
        observed_postings = observed_postings
            .checked_add(batch.num_rows())
            .ok_or_else(invalid)?;
    }
    if observed_postings != postings {
        return Err(invalid());
    }

    let mut counts = Vec::new();
    for primary in 0..posting_count {
        let population = primary_populations[primary as usize];
        let mut rows = alternate_counts[primary as usize]
            .iter()
            .map(|(&alternate, &count)| V40SpillCount {
                primary_posting: primary,
                alternate_posting: Some(alternate),
                count,
                primary_population: population,
            })
            .collect::<Vec<_>>();
        rows.sort_unstable_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.alternate_posting.cmp(&right.alternate_posting))
        });
        let alternate_total = rows
            .iter()
            .try_fold(0_u64, |total, row| total.checked_add(row.count))
            .ok_or_else(invalid)?;
        rows.push(V40SpillCount {
            primary_posting: primary,
            alternate_posting: None,
            count: population
                .checked_sub(alternate_total)
                .ok_or_else(invalid)?,
            primary_population: population,
        });
        counts.extend(rows);
    }
    pack_v40_spill_summary(&counts, posting_count)?;
    Ok(counts)
}

pub(crate) fn pack_v40_spill_summary(
    counts: &[V40SpillCount],
    posting_count: u32,
) -> Result<V40PackedSpillSummary> {
    let invalid = || BorsukError::InvalidStorage("V40 packed spill summary differs".to_owned());
    if posting_count == 0 || counts.is_empty() {
        return Err(invalid());
    }
    let mut offsets = Vec::with_capacity(posting_count as usize + 1);
    let mut alternate_postings = Vec::new();
    let mut masses_q24 = Vec::new();
    let mut residual_masses_q24 = Vec::with_capacity(posting_count as usize);
    offsets.push(0);
    let mut cursor = 0_usize;
    for primary in 0..posting_count {
        let start = cursor;
        while cursor < counts.len() && counts[cursor].primary_posting == primary {
            cursor += 1;
        }
        let rows = &counts[start..cursor];
        if rows.is_empty() {
            return Err(invalid());
        }
        let population = rows[0].primary_population;
        let alternate_rows = &rows[..rows.len() - 1];
        let alternate_ids = alternate_rows
            .iter()
            .filter_map(|row| row.alternate_posting)
            .collect::<BTreeSet<_>>();
        if population == 0
            || rows.iter().any(|row| row.primary_population != population)
            || rows
                .last()
                .is_none_or(|row| row.alternate_posting.is_some())
            || alternate_rows.iter().any(|row| {
                row.alternate_posting
                    .is_none_or(|alternate| alternate >= posting_count || alternate == primary)
                    || row.count == 0
            })
            || alternate_ids.len() != alternate_rows.len()
            || alternate_rows.windows(2).any(|pair| {
                pair[0].count < pair[1].count
                    || pair[0].count == pair[1].count
                        && pair[0].alternate_posting > pair[1].alternate_posting
            })
        {
            return Err(invalid());
        }
        let retained = rows
            .len()
            .saturating_sub(1)
            .min(V40_MAXIMUM_ALTERNATES_PER_POSTING);
        let mut categories = rows[..retained]
            .iter()
            .map(|row| (row.alternate_posting, row.count, 0_u32, 0_u64))
            .collect::<Vec<_>>();
        let residual_count = rows[retained..]
            .iter()
            .try_fold(0_u64, |total, row| total.checked_add(row.count))
            .ok_or_else(invalid)?;
        categories.push((None, residual_count, 0, 0));
        let observed = categories
            .iter()
            .try_fold(0_u64, |total, (_, count, _, _)| total.checked_add(*count))
            .ok_or_else(invalid)?;
        if observed != population {
            return Err(invalid());
        }
        let mut assigned = 0_u32;
        for category in &mut categories {
            let numerator = u128::from(category.1) * u128::from(V40_Q24_TOTAL);
            category.2 =
                u32::try_from(numerator / u128::from(population)).map_err(|_| invalid())?;
            category.3 =
                u64::try_from(numerator % u128::from(population)).map_err(|_| invalid())?;
            assigned = assigned.checked_add(category.2).ok_or_else(invalid)?;
        }
        let remaining = V40_Q24_TOTAL.checked_sub(assigned).ok_or_else(invalid)?;
        let mut order = (0..categories.len()).collect::<Vec<_>>();
        order.sort_unstable_by(|&left, &right| {
            categories[right]
                .3
                .cmp(&categories[left].3)
                .then_with(|| {
                    categories[left]
                        .0
                        .is_none()
                        .cmp(&categories[right].0.is_none())
                })
                .then_with(|| {
                    categories[left]
                        .0
                        .unwrap_or(u32::MAX)
                        .cmp(&categories[right].0.unwrap_or(u32::MAX))
                })
        });
        for index in order.into_iter().take(remaining as usize) {
            categories[index].2 = categories[index].2.checked_add(1).ok_or_else(invalid)?;
        }
        if categories.iter().map(|category| category.2).sum::<u32>() != V40_Q24_TOTAL {
            return Err(invalid());
        }
        for category in &categories[..retained] {
            alternate_postings.push(category.0.ok_or_else(invalid)?);
            masses_q24.push(category.2);
        }
        residual_masses_q24.push(categories[retained].2);
        offsets.push(u64::try_from(alternate_postings.len()).map_err(|_| invalid())?);
    }
    if cursor != counts.len() {
        return Err(invalid());
    }
    Ok(V40PackedSpillSummary {
        offsets,
        alternate_postings,
        masses_q24,
        residual_masses_q24,
    })
}

fn v40_spill_count_schema() -> Schema {
    Schema::new(vec![
        Field::new("primary_posting", DataType::UInt32, false),
        Field::new("category_role", DataType::UInt8, false),
        Field::new("alternate_posting", DataType::UInt32, true),
        Field::new("count", DataType::UInt64, false),
        Field::new("primary_population", DataType::UInt64, false),
    ])
}

pub(crate) fn encode_v40_spill_counts_parquet(
    counts: &[V40SpillCount],
    posting_count: u32,
) -> Result<Vec<u8>> {
    pack_v40_spill_summary(counts, posting_count)?;
    let schema = Arc::new(v40_spill_count_schema());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                counts.iter().map(|row| row.primary_posting),
            )),
            Arc::new(UInt8Array::from_iter_values(
                counts
                    .iter()
                    .map(|row| u8::from(row.alternate_posting.is_some())),
            )),
            Arc::new(UInt32Array::from(
                counts
                    .iter()
                    .map(|row| row.alternate_posting)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from_iter_values(
                counts.iter().map(|row| row.count),
            )),
            Arc::new(UInt64Array::from_iter_values(
                counts.iter().map(|row| row.primary_population),
            )),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(1_048_576))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn decode_v40_spill_counts_parquet(
    bytes: &[u8],
    posting_count: u32,
) -> Result<Vec<V40SpillCount>> {
    let invalid = || BorsukError::InvalidStorage("V40 spill count Parquet differs".to_owned());
    if posting_count == 0 {
        return Err(invalid());
    }
    if bytes.len() < 12 || !bytes.starts_with(b"PAR1") || !bytes.ends_with(b"PAR1") {
        return Err(invalid());
    }
    let footer_length = u32::from_le_bytes(
        bytes[bytes.len() - 8..bytes.len() - 4]
            .try_into()
            .map_err(|_| invalid())?,
    ) as usize;
    if footer_length > bytes.len().saturating_sub(12) {
        return Err(invalid());
    }
    let footer_start = bytes.len() - footer_length - 8;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if builder.schema().as_ref() != &v40_spill_count_schema()
        || builder.metadata().num_row_groups() != 1
        || builder.metadata().file_metadata().num_rows() <= 0
    {
        return Err(invalid());
    }
    let mut ranges = Vec::new();
    let mut push_range = |offset: i64, length: i64| -> Result<()> {
        let start = usize::try_from(offset).map_err(|_| invalid())?;
        let length = usize::try_from(length).map_err(|_| invalid())?;
        let end = start.checked_add(length).ok_or_else(invalid)?;
        if length == 0 {
            return Err(invalid());
        }
        ranges.push((start, end));
        Ok(())
    };
    for column in builder
        .metadata()
        .row_groups()
        .iter()
        .flat_map(|group| group.columns())
    {
        if column.file_path().is_some() {
            return Err(invalid());
        }
        push_range(
            column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset()),
            column.compressed_size(),
        )?;
        for (offset, length) in [
            (column.column_index_offset(), column.column_index_length()),
            (column.offset_index_offset(), column.offset_index_length()),
            (column.bloom_filter_offset(), column.bloom_filter_length()),
        ] {
            match (offset, length) {
                (Some(offset), Some(length)) => push_range(offset, i64::from(length))?,
                (None, None) => {}
                _ => return Err(invalid()),
            }
        }
    }
    ranges.sort_unstable();
    let mut referenced_end = 4_usize;
    for (start, end) in ranges {
        if start != referenced_end || end <= start || end > footer_start {
            return Err(invalid());
        }
        referenced_end = end;
    }
    if referenced_end != footer_start {
        return Err(invalid());
    }
    let mut counts = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.schema().as_ref() != &v40_spill_count_schema()
            || batch.columns()[..2]
                .iter()
                .chain(batch.columns()[3..].iter())
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid());
        }
        let primary = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let role = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(invalid)?;
        let alternate = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let count = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        let population = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        for row in 0..batch.num_rows() {
            let alternate_posting = (!alternate.is_null(row)).then(|| alternate.value(row));
            if role.value(row) != u8::from(alternate_posting.is_some()) {
                return Err(invalid());
            }
            counts.push(V40SpillCount {
                primary_posting: primary.value(row),
                alternate_posting,
                count: count.value(row),
                primary_population: population.value(row),
            });
        }
    }
    pack_v40_spill_summary(&counts, posting_count)?;
    Ok(counts)
}

fn v40_packed_spill_summary_schema() -> Schema {
    let list = |name: &str, data_type| {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new("item", data_type, false))),
            false,
        )
    };
    Schema::new(vec![
        list("offsets", DataType::UInt64),
        list("alternate_postings", DataType::UInt32),
        list("masses_q24", DataType::UInt32),
        list("residual_masses_q24", DataType::UInt32),
    ])
}

fn v40_list_offsets(length: usize) -> Result<OffsetBuffer<i32>> {
    let length = i32::try_from(length)
        .map_err(|_| BorsukError::InvalidStorage("V40 Arrow list length overflows".to_owned()))?;
    Ok(OffsetBuffer::new(vec![0_i32, length].into()))
}

pub(crate) fn encode_v40_packed_spill_summary_arrow(
    summary: &V40PackedSpillSummary,
) -> Result<Vec<u8>> {
    validate_v40_packed_spill_summary(summary)?;
    let schema = Arc::new(v40_packed_spill_summary_schema());
    let child = |index: usize| match schema.field(index).data_type() {
        DataType::List(child) => Ok(Arc::clone(child)),
        _ => Err(BorsukError::InvalidStorage(
            "V40 packed spill Arrow schema differs".to_owned(),
        )),
    };
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(ListArray::new(
                child(0)?,
                v40_list_offsets(summary.offsets.len())?,
                Arc::new(UInt64Array::from(summary.offsets.clone())),
                None,
            )),
            Arc::new(ListArray::new(
                child(1)?,
                v40_list_offsets(summary.alternate_postings.len())?,
                Arc::new(UInt32Array::from(summary.alternate_postings.clone())),
                None,
            )),
            Arc::new(ListArray::new(
                child(2)?,
                v40_list_offsets(summary.masses_q24.len())?,
                Arc::new(UInt32Array::from(summary.masses_q24.clone())),
                None,
            )),
            Arc::new(ListArray::new(
                child(3)?,
                v40_list_offsets(summary.residual_masses_q24.len())?,
                Arc::new(UInt32Array::from(summary.residual_masses_q24.clone())),
                None,
            )),
        ],
    )?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

pub(crate) fn decode_v40_packed_spill_summary_arrow(
    bytes: &[u8],
    posting_count: u32,
) -> Result<V40PackedSpillSummary> {
    let invalid = || BorsukError::InvalidStorage("V40 packed spill Arrow differs".to_owned());
    if bytes.len() < 18 || !bytes.starts_with(b"ARROW1") || !bytes.ends_with(b"ARROW1") {
        return Err(invalid());
    }
    let footer_length = u32::from_le_bytes(
        bytes[bytes.len() - 10..bytes.len() - 6]
            .try_into()
            .map_err(|_| invalid())?,
    ) as usize;
    if footer_length > bytes.len().saturating_sub(18) {
        return Err(invalid());
    }
    let trailer = bytes.len() - 10;
    let footer_start = trailer - footer_length;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer]).map_err(|_| invalid())?;
    if footer
        .dictionaries()
        .is_some_and(|dictionaries| !dictionaries.is_empty())
    {
        return Err(invalid());
    }
    let blocks = footer.recordBatches().ok_or_else(invalid)?;
    if blocks.len() != 1 {
        return Err(invalid());
    }
    let block = blocks.get(0);
    let block_offset = usize::try_from(block.offset()).map_err(|_| invalid())?;
    let metadata_length = usize::try_from(block.metaDataLength()).map_err(|_| invalid())?;
    let body_length = usize::try_from(block.bodyLength()).map_err(|_| invalid())?;
    let referenced_end = block_offset
        .checked_add(metadata_length)
        .and_then(|value| value.checked_add(body_length))
        .ok_or_else(invalid)?;
    if block_offset < 8
        || metadata_length < 8
        || bytes.get(referenced_end..footer_start) != Some(&[255, 255, 255, 255, 0, 0, 0, 0])
    {
        return Err(invalid());
    }
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &v40_packed_spill_summary_schema() || reader.num_batches() != 1 {
        return Err(invalid());
    }
    let batch = reader.next().transpose()?.ok_or_else(invalid)?;
    if reader.next().is_some()
        || batch.num_rows() != 1
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid());
    }
    let u64_values = |index: usize| -> Result<Vec<u64>> {
        let list = batch
            .column(index)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(invalid)?;
        let list_values = list.value(0);
        let values = list_values
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        if values.null_count() != 0 {
            return Err(invalid());
        }
        Ok(values.values().to_vec())
    };
    let u32_values = |index: usize| -> Result<Vec<u32>> {
        let list = batch
            .column(index)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(invalid)?;
        let list_values = list.value(0);
        let values = list_values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        if values.null_count() != 0 {
            return Err(invalid());
        }
        Ok(values.values().to_vec())
    };
    let summary = V40PackedSpillSummary {
        offsets: u64_values(0)?,
        alternate_postings: u32_values(1)?,
        masses_q24: u32_values(2)?,
        residual_masses_q24: u32_values(3)?,
    };
    if validate_v40_packed_spill_summary(&summary)? != posting_count as usize {
        return Err(invalid());
    }
    Ok(summary)
}

fn validate_v40_packed_spill_summary(summary: &V40PackedSpillSummary) -> Result<usize> {
    let invalid = || BorsukError::InvalidStorage("V40 packed spill summary differs".to_owned());
    let posting_count = summary.residual_masses_q24.len();
    if posting_count == 0
        || summary.offsets.len() != posting_count + 1
        || summary.offsets.first() != Some(&0)
        || summary.alternate_postings.len() != summary.masses_q24.len()
        || summary.offsets.last().copied() != Some(summary.alternate_postings.len() as u64)
    {
        return Err(invalid());
    }
    for primary in 0..posting_count {
        let start = usize::try_from(summary.offsets[primary]).map_err(|_| invalid())?;
        let end = usize::try_from(summary.offsets[primary + 1]).map_err(|_| invalid())?;
        if end < start || end > summary.alternate_postings.len() || end - start > 32 {
            return Err(invalid());
        }
        let alternates = &summary.alternate_postings[start..end];
        let unique = alternates.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != alternates.len()
            || alternates.iter().any(|&alternate| {
                alternate as usize >= posting_count || alternate as usize == primary
            })
        {
            return Err(invalid());
        }
        let total = summary.masses_q24[start..end]
            .iter()
            .try_fold(summary.residual_masses_q24[primary], |sum, mass| {
                sum.checked_add(*mass)
            })
            .ok_or_else(invalid)?;
        if total != V40_Q24_TOTAL {
            return Err(invalid());
        }
    }
    Ok(posting_count)
}

pub(crate) fn select_v40_accepted_spill_postings(
    frontier: &V40TreeFrontier,
    summary: &V40PackedSpillSummary,
    selected_postings: usize,
) -> Result<V40Selection> {
    let invalid = || BorsukError::InvalidStorage("V40 marginal selection differs".to_owned());
    let posting_count = validate_v40_packed_spill_summary(summary)?;
    let frontier_unique = frontier
        .posting_ordinals
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if frontier.posting_ordinals.is_empty()
        || frontier.posting_ordinals.len() > V40_MAXIMUM_FRONTIER_POSTINGS
        || frontier_unique.len() != frontier.posting_ordinals.len()
        || frontier_unique
            .iter()
            .any(|posting| *posting as usize >= posting_count)
        || selected_postings == 0
    {
        return Err(invalid());
    }

    let mut candidates = frontier_unique;
    for &primary in &frontier.posting_ordinals {
        let start = usize::try_from(summary.offsets[primary as usize]).map_err(|_| invalid())?;
        let end = usize::try_from(summary.offsets[primary as usize + 1]).map_err(|_| invalid())?;
        candidates.extend(summary.alternate_postings[start..end].iter().copied());
    }
    if candidates.len() < selected_postings
        || candidates.len()
            > V40_MAXIMUM_FRONTIER_POSTINGS * (V40_MAXIMUM_ALTERNATES_PER_POSTING + 1)
    {
        return Err(invalid());
    }
    let best_frontier_rank = frontier
        .posting_ordinals
        .iter()
        .enumerate()
        .map(|(rank, &posting)| (posting, rank as u32))
        .collect::<BTreeMap<_, _>>();
    let mut selected = BTreeSet::new();
    let mut posting_ordinals = Vec::with_capacity(selected_postings);
    let mut objective_value = 0_u128;
    let mut marginal_recomputations = 0_u64;
    while posting_ordinals.len() < selected_postings {
        let mut best = None;
        for &candidate in &candidates {
            if selected.contains(&candidate) {
                continue;
            }
            marginal_recomputations = marginal_recomputations.checked_add(1).ok_or_else(invalid)?;
            let mut gain = 0_u128;
            for (rank, &primary) in frontier.posting_ordinals.iter().enumerate() {
                if selected.contains(&primary) {
                    continue;
                }
                let weight = u128::try_from(frontier.posting_ordinals.len() - rank)
                    .map_err(|_| invalid())?;
                if candidate == primary {
                    gain = gain
                        .checked_add(
                            weight
                                .checked_mul(u128::from(
                                    summary.residual_masses_q24[primary as usize],
                                ))
                                .ok_or_else(invalid)?,
                        )
                        .ok_or_else(invalid)?;
                }
                let start =
                    usize::try_from(summary.offsets[primary as usize]).map_err(|_| invalid())?;
                let end = usize::try_from(summary.offsets[primary as usize + 1])
                    .map_err(|_| invalid())?;
                for edge in start..end {
                    let alternate = summary.alternate_postings[edge];
                    if !selected.contains(&alternate)
                        && (candidate == primary || candidate == alternate)
                    {
                        gain = gain
                            .checked_add(
                                weight
                                    .checked_mul(u128::from(summary.masses_q24[edge]))
                                    .ok_or_else(invalid)?,
                            )
                            .ok_or_else(invalid)?;
                    }
                }
            }
            let rank = best_frontier_rank
                .get(&candidate)
                .copied()
                .unwrap_or(u32::MAX);
            let key = (gain, std::cmp::Reverse(rank), std::cmp::Reverse(candidate));
            if best.as_ref().is_none_or(|(best_key, _)| key > *best_key) {
                best = Some((key, candidate));
            }
        }
        let ((gain, _, _), candidate) = best.ok_or_else(invalid)?;
        selected.insert(candidate);
        posting_ordinals.push(candidate);
        objective_value = objective_value.checked_add(gain).ok_or_else(invalid)?;
    }
    Ok(V40Selection {
        arm: V40RouterArm::AcceptedSpill,
        posting_ordinals,
        objective_value,
        candidate_count: u32::try_from(candidates.len()).map_err(|_| invalid())?,
        marginal_recomputations,
    })
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_s3_uri(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("s3://") else {
        return false;
    };
    let Some((bucket, key)) = rest.split_once('/') else {
        return false;
    };
    !bucket.is_empty()
        && !key.is_empty()
        && !bucket.contains(['?', '#'])
        && !key.contains(['?', '#'])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// One capability-separated local V40 direct-router phase.
#[doc(hidden)]
pub enum V40LocalRunMode {
    /// Select postings from query vectors without ground truth access.
    SelectDirect,
    /// Build query-independent accepted-spill counts and packed summary.
    BuildSpillSummary,
    /// Select postings using queries and the sealed accepted-spill summary.
    SelectAcceptedSpill,
    /// Evaluate sealed selections with ground truth but no query-vector access.
    EvaluateDirect,
    /// Evaluate sealed accepted-spill selections without query-vector access.
    EvaluateAcceptedSpill,
}

impl V40LocalRunMode {
    fn input_roles(self) -> &'static [&'static str] {
        match self {
            Self::SelectDirect => &[
                "cohort-authority",
                "v37-authority",
                "ownership-tree",
                "development-query",
            ],
            Self::BuildSpillSummary => &[
                "cohort-authority",
                "direct-result",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
            ],
            Self::SelectAcceptedSpill => &[
                "cohort-authority",
                "direct-result",
                "v37-authority",
                "ownership-tree",
                "development-query",
                "spill-summary-result",
                "spill-summary",
            ],
            Self::EvaluateDirect => &[
                "cohort-authority",
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "development-ground-truth",
                "direct-selection-result",
                "direct-selection",
            ],
            Self::EvaluateAcceptedSpill => &[
                "cohort-authority",
                "direct-result",
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "development-ground-truth",
                "spill-summary-result",
                "spill-summary",
                "accepted-selection-result",
                "accepted-selection",
            ],
        }
    }

    fn output_roles(self) -> &'static [&'static str] {
        match self {
            Self::SelectDirect => &["direct-selection"],
            Self::BuildSpillSummary => &["spill-counts", "spill-summary", "spill-summary-result"],
            Self::SelectAcceptedSpill => &["accepted-selection", "accepted-selection-result"],
            Self::EvaluateDirect => &["direct-result"],
            Self::EvaluateAcceptedSpill => &["accepted-result"],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One authenticated local input whose URI is evidence, not a network capability.
#[doc(hidden)]
pub struct V40LocalArtifact {
    role: String,
    path: PathBuf,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
}

impl V40LocalArtifact {
    /// Construct one strict phase-local input identity.
    pub fn try_new(
        role: String,
        path: PathBuf,
        uri: String,
        sha256: String,
        blake3: String,
        encoded_bytes: u64,
    ) -> Result<Self> {
        if role.is_empty()
            || path.as_os_str().is_empty()
            || !valid_s3_uri(&uri)
            || !valid_digest(&sha256)
            || !valid_digest(&blake3)
            || encoded_bytes == 0
        {
            return Err(BorsukError::InvalidStorage(
                "V40 local artifact identity differs".to_owned(),
            ));
        }
        Ok(Self {
            role,
            path,
            uri,
            sha256,
            blake3,
            encoded_bytes,
        })
    }

    /// Return the exact phase-local role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Return the local path without opening it.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One explicit create-only V40 output.
#[doc(hidden)]
pub struct V40LocalOutput {
    role: String,
    path: PathBuf,
}

impl V40LocalOutput {
    /// Construct one strict output identity.
    pub fn try_new(role: String, path: PathBuf) -> Result<Self> {
        if role.is_empty() || path.as_os_str().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "V40 local output identity differs".to_owned(),
            ));
        }
        Ok(Self { role, path })
    }

    /// Return the output role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Return the create-only output path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Exact local inputs and output for one V40 direct phase.
#[doc(hidden)]
pub struct V40LocalRunRequest {
    mode: V40LocalRunMode,
    inputs: Vec<V40LocalArtifact>,
    outputs: Vec<V40LocalOutput>,
    workers: u32,
}

impl V40LocalRunRequest {
    /// Validate exact roles, worker values, and path/URI separation.
    pub fn try_new(
        mode: V40LocalRunMode,
        inputs: Vec<V40LocalArtifact>,
        outputs: Vec<V40LocalOutput>,
        workers: u32,
    ) -> Result<Self> {
        let input_roles = inputs
            .iter()
            .map(V40LocalArtifact::role)
            .collect::<Vec<_>>();
        let output_roles = outputs.iter().map(V40LocalOutput::role).collect::<Vec<_>>();
        let mut paths = BTreeSet::new();
        let mut uris = BTreeSet::new();
        if input_roles != mode.input_roles()
            || output_roles != mode.output_roles()
            || !matches!(workers, 1 | 2 | 4 | 8 | 16 | 32)
            || inputs.iter().any(|input| !paths.insert(input.path()))
            || outputs.iter().any(|output| !paths.insert(output.path()))
            || inputs.iter().any(|input| !uris.insert(input.uri.as_str()))
        {
            return Err(BorsukError::InvalidStorage(
                "V40 local phase capability differs".to_owned(),
            ));
        }
        Ok(Self {
            mode,
            inputs,
            outputs,
            workers,
        })
    }

    /// Return input roles in their authoritative order.
    pub fn input_roles(&self) -> Vec<&str> {
        self.inputs.iter().map(V40LocalArtifact::role).collect()
    }

    /// Return output roles in their authoritative order.
    pub fn output_roles(&self) -> Vec<&str> {
        self.outputs.iter().map(V40LocalOutput::role).collect()
    }

    /// Return this request's phase.
    pub fn mode(&self) -> V40LocalRunMode {
        self.mode
    }

    /// Return the frozen worker count.
    pub fn workers(&self) -> u32 {
        self.workers
    }
}

#[derive(Debug)]
pub(crate) struct V40AuthenticatedLocalInputs {
    files: Vec<Option<File>>,
    canonical_inputs: BTreeSet<PathBuf>,
    file_ids: BTreeSet<(u64, u64)>,
}

pub(crate) fn authenticate_v40_local_request(
    request: &V40LocalRunRequest,
) -> Result<V40AuthenticatedLocalInputs> {
    authenticate_v40_local_request_deferred(request, None)
}

fn authenticate_v40_local_request_deferred(
    request: &V40LocalRunRequest,
    deferred_role: Option<&str>,
) -> Result<V40AuthenticatedLocalInputs> {
    const HASH_BUFFER_BYTES: usize = 1_048_576;
    let invalid =
        || BorsukError::InvalidStorage("V40 local input authentication differs".to_owned());
    let mut canonical_inputs = BTreeSet::new();
    let mut file_ids = BTreeSet::new();
    let mut files = Vec::with_capacity(request.inputs.len());
    for input in &request.inputs {
        if deferred_role == Some(input.role()) {
            files.push(None);
            continue;
        }
        let before = fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if before.file_type().is_symlink()
            || !before.file_type().is_file()
            || before.len() != input.encoded_bytes
        {
            return Err(invalid());
        }
        let file = File::open(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        let opened = file.metadata().map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if opened.dev() != before.dev()
            || opened.ino() != before.ino()
            || !file_ids.insert((opened.dev(), opened.ino()))
            || !canonical_inputs.insert(fs::canonicalize(&input.path).map_err(|source| {
                BorsukError::Io {
                    path: input.path.clone(),
                    source,
                }
            })?)
        {
            return Err(invalid());
        }
        let mut reader = BufReader::with_capacity(HASH_BUFFER_BYTES, file);
        let mut sha256 = Sha256::new();
        let mut blake3 = blake3::Hasher::new();
        let mut observed_bytes = 0_u64;
        let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
        loop {
            let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
                path: input.path.clone(),
                source,
            })?;
            if read == 0 {
                break;
            }
            observed_bytes = observed_bytes
                .checked_add(read as u64)
                .ok_or_else(invalid)?;
            sha256.update(&buffer[..read]);
            blake3.update(&buffer[..read]);
        }
        let after = fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if observed_bytes != input.encoded_bytes
            || format!("{:x}", sha256.finalize()) != input.sha256
            || blake3.finalize().to_hex().as_str() != input.blake3
            || after.file_type().is_symlink()
            || after.dev() != before.dev()
            || after.ino() != before.ino()
            || after.len() != before.len()
            || after.mtime() != before.mtime()
            || after.mtime_nsec() != before.mtime_nsec()
            || after.ctime() != before.ctime()
            || after.ctime_nsec() != before.ctime_nsec()
        {
            return Err(invalid());
        }
        files.push(Some(reader.into_inner()));
    }

    let mut canonical_outputs = BTreeSet::new();
    for output in &request.outputs {
        match fs::symlink_metadata(&output.path) {
            Ok(_) => return Err(invalid()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(BorsukError::Io {
                    path: output.path.clone(),
                    source,
                });
            }
        }
        let parent = output.path.parent().ok_or_else(invalid)?;
        let parent_metadata = fs::symlink_metadata(parent).map_err(|source| BorsukError::Io {
            path: parent.to_owned(),
            source,
        })?;
        if parent_metadata.file_type().is_symlink() || !parent_metadata.file_type().is_dir() {
            return Err(invalid());
        }
        let output_name = output.path.file_name().ok_or_else(invalid)?;
        let canonical = fs::canonicalize(parent)
            .map_err(|source| BorsukError::Io {
                path: parent.to_owned(),
                source,
            })?
            .join(output_name);
        if canonical_inputs.contains(&canonical) || !canonical_outputs.insert(canonical) {
            return Err(invalid());
        }
    }
    Ok(V40AuthenticatedLocalInputs {
        files,
        canonical_inputs,
        file_ids,
    })
}

fn authenticate_v40_deferred_input(
    request: &V40LocalRunRequest,
    authenticated: &mut V40AuthenticatedLocalInputs,
    role: &str,
) -> Result<()> {
    const HASH_BUFFER_BYTES: usize = 1_048_576;
    let invalid =
        || BorsukError::InvalidStorage("V40 deferred input authentication differs".to_owned());
    let index = request
        .inputs
        .iter()
        .position(|input| input.role == role)
        .ok_or_else(invalid)?;
    if authenticated.files.get(index).is_none_or(Option::is_some) {
        return Err(invalid());
    }
    let input = &request.inputs[index];
    let before = fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
        path: input.path.clone(),
        source,
    })?;
    if before.file_type().is_symlink()
        || !before.file_type().is_file()
        || before.len() != input.encoded_bytes
    {
        return Err(invalid());
    }
    let file = File::open(&input.path).map_err(|source| BorsukError::Io {
        path: input.path.clone(),
        source,
    })?;
    let opened = file.metadata().map_err(|source| BorsukError::Io {
        path: input.path.clone(),
        source,
    })?;
    let canonical = fs::canonicalize(&input.path).map_err(|source| BorsukError::Io {
        path: input.path.clone(),
        source,
    })?;
    let file_id = (opened.dev(), opened.ino());
    if opened.dev() != before.dev()
        || opened.ino() != before.ino()
        || authenticated.file_ids.contains(&file_id)
        || authenticated.canonical_inputs.contains(&canonical)
    {
        return Err(invalid());
    }
    let mut reader = BufReader::with_capacity(HASH_BUFFER_BYTES, file);
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    let mut observed_bytes = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
        if read == 0 {
            break;
        }
        observed_bytes = observed_bytes
            .checked_add(read as u64)
            .ok_or_else(invalid)?;
        sha256.update(&buffer[..read]);
        blake3.update(&buffer[..read]);
    }
    let after = fs::symlink_metadata(&input.path).map_err(|source| BorsukError::Io {
        path: input.path.clone(),
        source,
    })?;
    if observed_bytes != input.encoded_bytes
        || format!("{:x}", sha256.finalize()) != input.sha256
        || blake3.finalize().to_hex().as_str() != input.blake3
        || after.file_type().is_symlink()
        || after.dev() != before.dev()
        || after.ino() != before.ino()
        || after.len() != before.len()
        || after.mtime() != before.mtime()
        || after.mtime_nsec() != before.mtime_nsec()
        || after.ctime() != before.ctime()
        || after.ctime_nsec() != before.ctime_nsec()
    {
        return Err(invalid());
    }
    authenticated.file_ids.insert(file_id);
    authenticated.canonical_inputs.insert(canonical);
    authenticated.files[index] = Some(reader.into_inner());
    Ok(())
}

fn v40_local_input<'a>(
    request: &'a V40LocalRunRequest,
    role: &str,
) -> Result<&'a V40LocalArtifact> {
    request
        .inputs
        .iter()
        .find(|input| input.role == role)
        .ok_or_else(|| BorsukError::InvalidStorage("V40 local input role differs".to_owned()))
}

fn v40_authenticated_input_file(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
    role: &str,
) -> Result<File> {
    let index = request
        .inputs
        .iter()
        .position(|input| input.role == role)
        .ok_or_else(|| BorsukError::InvalidStorage("V40 local input role differs".to_owned()))?;
    let mut file = authenticated
        .files
        .get(index)
        .and_then(Option::as_ref)
        .ok_or_else(|| BorsukError::InvalidStorage("V40 authenticated input differs".to_owned()))?
        .try_clone()
        .map_err(|source| BorsukError::Io {
            path: request.inputs[index].path.clone(),
            source,
        })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| BorsukError::Io {
            path: request.inputs[index].path.clone(),
            source,
        })?;
    Ok(file)
}

fn read_v40_authenticated_input(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
    role: &str,
) -> Result<Vec<u8>> {
    let input = v40_local_input(request, role)?;
    let capacity = usize::try_from(input.encoded_bytes).map_err(|_| {
        BorsukError::InvalidStorage("V40 local input exceeds address space".to_owned())
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    v40_authenticated_input_file(request, authenticated, role)?
        .take(input.encoded_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| BorsukError::Io {
            path: input.path.clone(),
            source,
        })?;
    if bytes.len() != capacity {
        return Err(BorsukError::InvalidStorage(
            "V40 authenticated input length differs".to_owned(),
        ));
    }
    Ok(bytes)
}

fn v40_canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, v40_canonical_json(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(v40_canonical_json).collect())
        }
        value => value,
    }
}

fn v40_selection_receipt_bytes(
    request: &V40LocalRunRequest,
    selections: &[V40DirectSelectionRecord],
    output_bytes: &[u8],
) -> Result<Vec<u8>> {
    let inputs = request
        .inputs
        .iter()
        .map(|input| {
            serde_json::json!({
                "blake3": input.blake3,
                "encoded_bytes": input.encoded_bytes,
                "role": input.role,
                "sha256": input.sha256,
                "uri": input.uri,
            })
        })
        .collect::<Vec<_>>();
    let output = request.outputs.first().ok_or_else(|| {
        BorsukError::InvalidStorage("V40 direct selection output differs".to_owned())
    })?;
    let total_node_pops = selections.iter().try_fold(0_u64, |total, selection| {
        total.checked_add(u64::from(selection.node_pops))
    });
    let total_node_pops = total_node_pops.ok_or_else(|| {
        BorsukError::InvalidStorage("V40 direct selection work overflows".to_owned())
    })?;
    let value = serde_json::json!({
        "artifact": {
            "blake3": blake3::hash(output_bytes).to_hex().to_string(),
            "encoded_bytes": output_bytes.len(),
            "role": output.role,
            "sha256": format!("{:x}", Sha256::digest(output_bytes)),
            "uri": format!("file://{}", output.path.display()),
        },
        "claim_eligible": false,
        "evidence": {
            "fma_backend": selections[0].fma_backend,
            "maximum_node_pops": V40_MAXIMUM_NODE_POPS,
            "query_count": selections.len(),
            "selected_postings": V40_DIRECT_SELECTED_POSTINGS,
            "total_node_pops": total_node_pops,
        },
        "inputs": inputs,
        "mode": "select-direct",
        "schema": "borsuk-v40-local-result-v1",
    });
    let mut bytes = serde_json::to_vec(&v40_canonical_json(value)).map_err(|error| {
        BorsukError::InvalidStorage(format!("V40 result serialization failed: {error}"))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40SelectionReceiptArtifact {
    blake3: String,
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40DirectCohortAuthority {
    development_ground_truth: V40SelectionReceiptArtifact,
    development_query: V40SelectionReceiptArtifact,
    ownership_tree: V40SelectionReceiptArtifact,
    query_count: u64,
    schema: String,
    v37_authority: V40SelectionReceiptArtifact,
}

fn valid_v40_cohort_identity(identity: &V40SelectionReceiptArtifact, role: &str) -> bool {
    identity.role == role
        && valid_s3_uri(&identity.uri)
        && valid_digest(&identity.sha256)
        && valid_digest(&identity.blake3)
        && identity.encoded_bytes > 0
}

fn parse_v40_direct_cohort_authority_bytes(bytes: &[u8]) -> Result<V40DirectCohortAuthority> {
    let invalid = || BorsukError::InvalidStorage("V40 direct cohort authority differs".to_owned());
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let mut canonical = serde_json::to_vec(&v40_canonical_json(value)).map_err(|_| invalid())?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(invalid());
    }
    let authority: V40DirectCohortAuthority =
        serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let identities = [
        (&authority.v37_authority, "v37-authority"),
        (&authority.ownership_tree, "ownership-tree"),
        (&authority.development_query, "development-query"),
        (
            &authority.development_ground_truth,
            "development-ground-truth",
        ),
    ];
    let uris = identities
        .iter()
        .map(|(identity, _)| identity.uri.as_str())
        .collect::<BTreeSet<_>>();
    if authority.schema != "borsuk-v40-direct-cohort-authority-v1"
        || authority.query_count != V40_DIRECT_QUERY_COUNT
        || identities
            .iter()
            .any(|(identity, role)| !valid_v40_cohort_identity(identity, role))
        || uris.len() != identities.len()
    {
        return Err(invalid());
    }
    Ok(authority)
}

fn v40_cohort_identity_matches_local(
    expected: &V40SelectionReceiptArtifact,
    observed: &V40LocalArtifact,
) -> bool {
    expected.role == observed.role
        && expected.uri == observed.uri
        && expected.sha256 == observed.sha256
        && expected.blake3 == observed.blake3
        && expected.encoded_bytes == observed.encoded_bytes
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40SelectionReceiptEvidence {
    fma_backend: String,
    maximum_node_pops: u64,
    query_count: u64,
    selected_postings: u64,
    total_node_pops: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40SelectionReceipt {
    artifact: V40SelectionReceiptArtifact,
    claim_eligible: bool,
    evidence: V40SelectionReceiptEvidence,
    inputs: Vec<V40SelectionReceiptArtifact>,
    mode: String,
    schema: String,
}

fn valid_v40_local_file_uri(value: &str) -> bool {
    value
        .strip_prefix("file://")
        .is_some_and(|path| path.starts_with('/') && path.len() > 1 && !path.contains(['?', '#']))
}

fn parse_v40_selection_receipt_bytes(
    bytes: &[u8],
    selection: &V40LocalArtifact,
    cohort_input: &V40LocalArtifact,
    cohort: &V40DirectCohortAuthority,
) -> Result<String> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct selection receipt authority differs".to_owned());
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let mut canonical = serde_json::to_vec(&v40_canonical_json(value)).map_err(|_| invalid())?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(invalid());
    }
    let receipt: V40SelectionReceipt = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let artifact = &receipt.artifact;
    let evidence = &receipt.evidence;
    let input_roles = receipt
        .inputs
        .iter()
        .map(|input| input.role.as_str())
        .collect::<Vec<_>>();
    let input_uris = receipt
        .inputs
        .iter()
        .map(|input| input.uri.as_str())
        .collect::<BTreeSet<_>>();
    let maximum_total_node_pops = evidence
        .query_count
        .checked_mul(evidence.maximum_node_pops)
        .ok_or_else(invalid)?;
    if receipt.schema != "borsuk-v40-local-result-v1"
        || receipt.claim_eligible
        || receipt.mode != "select-direct"
        || artifact.role != "direct-selection"
        || !valid_v40_local_file_uri(&artifact.uri)
        || artifact.sha256 != selection.sha256
        || artifact.blake3 != selection.blake3
        || artifact.encoded_bytes != selection.encoded_bytes
        || evidence.query_count != V40_DIRECT_QUERY_COUNT
        || evidence.selected_postings != V40_DIRECT_SELECTED_POSTINGS as u64
        || evidence.maximum_node_pops != V40_MAXIMUM_NODE_POPS as u64
        || evidence.total_node_pops < evidence.query_count
        || evidence.total_node_pops > maximum_total_node_pops
        || !matches!(
            evidence.fma_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || input_roles
            != [
                "cohort-authority",
                "v37-authority",
                "ownership-tree",
                "development-query",
            ]
        || input_uris.len() != receipt.inputs.len()
        || !v40_cohort_identity_matches_local(&receipt.inputs[0], cohort_input)
        || receipt.inputs[1] != cohort.v37_authority
        || receipt.inputs[2] != cohort.ownership_tree
        || receipt.inputs[3] != cohort.development_query
        || receipt.inputs.iter().any(|input| {
            !valid_s3_uri(&input.uri)
                || !valid_digest(&input.sha256)
                || !valid_digest(&input.blake3)
                || input.encoded_bytes == 0
        })
    {
        return Err(invalid());
    }
    Ok(evidence.fma_backend.clone())
}

fn v40_evaluation_result_bytes(
    request: &V40LocalRunRequest,
    spec: &V40EvaluationSpec,
    evaluation: &V40DirectEvaluation,
    expected_backend: &str,
) -> Result<Vec<u8>> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct evaluation result authority differs".to_owned());
    let (mode, passed_disposition, failed_disposition) = match request.mode {
        V40LocalRunMode::EvaluateDirect => ("evaluate-direct", "direct-passed", "direct-failed"),
        V40LocalRunMode::EvaluateAcceptedSpill => (
            "evaluate-accepted-spill",
            "challenger-passed",
            "router-rejected",
        ),
        _ => return Err(invalid()),
    };
    if request.input_roles() != request.mode.input_roles()
        || !matches!(expected_backend, "aarch64-neon-fma" | "x86-avx-fma")
        || evaluation.samples.is_empty()
        || spec.selected_postings == 0
        || spec.gt_neighbors == 0
        || spec.aggregate_gate_ppm > 1_000_000
        || spec.minimum_gate_ppm > 1_000_000
    {
        return Err(invalid());
    }
    let mut total_hits = 0_u64;
    let mut minimum_recall_ppm = 1_000_000_u32;
    let selected_postings = usize::try_from(spec.selected_postings).map_err(|_| invalid())?;
    for (query_ordinal, sample) in evaluation.samples.iter().enumerate() {
        let unique = sample
            .selected_postings
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let expected_recall = u32::try_from(
            u64::from(sample.hits)
                .checked_mul(1_000_000)
                .ok_or_else(invalid)?
                / u64::from(spec.gt_neighbors),
        )
        .map_err(|_| invalid())?;
        if sample.query_ordinal != u32::try_from(query_ordinal).map_err(|_| invalid())?
            || sample.selected_postings.len() != selected_postings
            || unique.len() != selected_postings
            || sample.node_pops == 0
            || sample.node_pops as usize > V40_MAXIMUM_NODE_POPS
            || sample.scored_internal_nodes == 0
            || sample.scored_internal_nodes > sample.node_pops
            || sample.hits > spec.gt_neighbors
            || sample.recall_ppm != expected_recall
        {
            return Err(invalid());
        }
        total_hits = total_hits
            .checked_add(u64::from(sample.hits))
            .ok_or_else(invalid)?;
        minimum_recall_ppm = minimum_recall_ppm.min(sample.recall_ppm);
    }
    let possible_hits = u64::try_from(evaluation.samples.len())
        .map_err(|_| invalid())?
        .checked_mul(u64::from(spec.gt_neighbors))
        .ok_or_else(invalid)?;
    let aggregate_recall_ppm =
        u32::try_from(total_hits.checked_mul(1_000_000).ok_or_else(invalid)? / possible_hits)
            .map_err(|_| invalid())?;
    let passed = aggregate_recall_ppm >= spec.aggregate_gate_ppm
        && minimum_recall_ppm >= spec.minimum_gate_ppm;
    let disposition = if passed {
        passed_disposition
    } else {
        failed_disposition
    };
    if evaluation.total_hits != total_hits
        || evaluation.aggregate_recall_ppm != aggregate_recall_ppm
        || evaluation.minimum_recall_ppm != minimum_recall_ppm
        || evaluation.passed != passed
        || evaluation.disposition != disposition
    {
        return Err(invalid());
    }
    let inputs = request
        .inputs
        .iter()
        .map(|input| {
            serde_json::json!({
                "blake3": input.blake3,
                "encoded_bytes": input.encoded_bytes,
                "role": input.role,
                "sha256": input.sha256,
                "uri": input.uri,
            })
        })
        .collect::<Vec<_>>();
    let samples = evaluation
        .samples
        .iter()
        .map(|sample| {
            serde_json::json!({
                "hits": sample.hits,
                "node_pops": sample.node_pops,
                "query_ordinal": sample.query_ordinal,
                "recall_ppm": sample.recall_ppm,
                "scored_internal_nodes": sample.scored_internal_nodes,
                "selected_postings": sample.selected_postings,
            })
        })
        .collect::<Vec<_>>();
    let value = serde_json::json!({
        "claim_eligible": false,
        "evidence": {
            "aggregate_gate_ppm": spec.aggregate_gate_ppm,
            "aggregate_recall_ppm": aggregate_recall_ppm,
            "disposition": disposition,
            "fma_backend": expected_backend,
            "gt_neighbors": spec.gt_neighbors,
            "minimum_gate_ppm": spec.minimum_gate_ppm,
            "minimum_recall_ppm": minimum_recall_ppm,
            "passed": passed,
            "query_count": evaluation.samples.len(),
            "samples": samples,
            "selected_postings": spec.selected_postings,
            "total_hits": total_hits,
        },
        "inputs": inputs,
        "mode": mode,
        "schema": "borsuk-v40-local-result-v1",
    });
    let mut bytes = serde_json::to_vec(&v40_canonical_json(value)).map_err(|_| invalid())?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn v40_accepted_evaluation_result_bytes(
    request: &V40LocalRunRequest,
    spec: &V40EvaluationSpec,
    evaluation: &V40DirectEvaluation,
    selections: &[V40AcceptedSelectionRecord],
    expected_backend: &str,
) -> Result<Vec<u8>> {
    let invalid =
        || BorsukError::InvalidStorage("V40 accepted evaluation result differs".to_owned());
    let selected_postings = usize::try_from(spec.selected_postings).map_err(|_| invalid())?;
    validate_v40_accepted_selections(selections, selected_postings, Some(expected_backend))?;
    if selections.len() != evaluation.samples.len()
        || selections
            .iter()
            .zip(&evaluation.samples)
            .any(|(selection, sample)| {
                selection.query_ordinal != sample.query_ordinal
                    || selection.posting_ordinals != sample.selected_postings
                    || selection.node_pops != sample.node_pops
                    || selection.scored_internal_nodes != sample.scored_internal_nodes
            })
    {
        return Err(invalid());
    }
    let base = v40_evaluation_result_bytes(request, spec, evaluation, expected_backend)?;
    let mut value: serde_json::Value = serde_json::from_slice(&base).map_err(|_| invalid())?;
    let samples = value
        .get_mut("evidence")
        .and_then(|evidence| evidence.get_mut("samples"))
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(invalid)?;
    for (sample, selection) in samples.iter_mut().zip(selections) {
        let object = sample.as_object_mut().ok_or_else(invalid)?;
        object.insert(
            "candidate_count".to_owned(),
            serde_json::json!(selection.candidate_count),
        );
        object.insert(
            "marginal_recomputations".to_owned(),
            serde_json::json!(selection.marginal_recomputations),
        );
        object.insert(
            "objective_value".to_owned(),
            serde_json::json!(selection.objective_value),
        );
    }
    let mut bytes = serde_json::to_vec(&v40_canonical_json(value)).map_err(|_| invalid())?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn publish_v40_output(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
    output.write_all(bytes).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    output.sync_all().map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })
}

fn v40_local_output<'a>(request: &'a V40LocalRunRequest, role: &str) -> Result<&'a V40LocalOutput> {
    request
        .outputs
        .iter()
        .find(|output| output.role == role)
        .ok_or_else(|| BorsukError::InvalidStorage("V40 local output role differs".to_owned()))
}

fn v40_input_receipt_values(request: &V40LocalRunRequest) -> Vec<serde_json::Value> {
    request
        .inputs
        .iter()
        .map(|input| {
            serde_json::json!({
                "blake3": input.blake3,
                "encoded_bytes": input.encoded_bytes,
                "role": input.role,
                "sha256": input.sha256,
                "uri": input.uri,
            })
        })
        .collect()
}

fn v40_output_receipt_value(output: &V40LocalOutput, bytes: &[u8]) -> serde_json::Value {
    serde_json::json!({
        "blake3": blake3::hash(bytes).to_hex().to_string(),
        "encoded_bytes": bytes.len(),
        "role": output.role,
        "sha256": format!("{:x}", Sha256::digest(bytes)),
        "uri": format!("file://{}", output.path.display()),
    })
}

fn v40_phase_receipt_bytes(
    request: &V40LocalRunRequest,
    mode: &str,
    artifacts: &[(&V40LocalOutput, &[u8])],
    evidence: serde_json::Value,
) -> Result<Vec<u8>> {
    let value = serde_json::json!({
        "artifacts": artifacts
            .iter()
            .map(|(output, bytes)| v40_output_receipt_value(output, bytes))
            .collect::<Vec<_>>(),
        "claim_eligible": false,
        "evidence": evidence,
        "inputs": v40_input_receipt_values(request),
        "mode": mode,
        "schema": "borsuk-v40-local-result-v1",
    });
    let mut bytes = serde_json::to_vec(&v40_canonical_json(value)).map_err(|_| {
        BorsukError::InvalidStorage("V40 phase receipt serialization differs".to_owned())
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40PhaseReceipt {
    artifacts: Vec<V40SelectionReceiptArtifact>,
    claim_eligible: bool,
    evidence: serde_json::Value,
    inputs: Vec<V40SelectionReceiptArtifact>,
    mode: String,
    schema: String,
}

fn parse_v40_phase_receipt(bytes: &[u8], expected_mode: &str) -> Result<V40PhaseReceipt> {
    let invalid = || BorsukError::InvalidStorage("V40 phase receipt authority differs".to_owned());
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let mut canonical = serde_json::to_vec(&v40_canonical_json(value)).map_err(|_| invalid())?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(invalid());
    }
    let receipt: V40PhaseReceipt = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let input_uris = receipt
        .inputs
        .iter()
        .map(|identity| identity.uri.as_str())
        .collect::<BTreeSet<_>>();
    if receipt.schema != "borsuk-v40-local-result-v1"
        || receipt.claim_eligible
        || receipt.mode != expected_mode
        || receipt.artifacts.is_empty()
        || receipt
            .evidence
            .as_object()
            .is_none_or(|value| value.is_empty())
        || input_uris.len() != receipt.inputs.len()
        || receipt.inputs.iter().any(|identity| {
            !valid_s3_uri(&identity.uri)
                || !valid_digest(&identity.sha256)
                || !valid_digest(&identity.blake3)
                || identity.encoded_bytes == 0
        })
        || receipt.artifacts.iter().any(|identity| {
            !valid_v40_local_file_uri(&identity.uri)
                || !valid_digest(&identity.sha256)
                || !valid_digest(&identity.blake3)
                || identity.encoded_bytes == 0
        })
    {
        return Err(invalid());
    }
    Ok(receipt)
}

fn v40_direct_selection_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("selection_rank", DataType::UInt32, false),
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("node_pops", DataType::UInt32, false),
        Field::new("scored_internal_nodes", DataType::UInt32, false),
        Field::new("fma_backend", DataType::Utf8, false),
    ])
}

fn v40_accepted_selection_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("selection_rank", DataType::UInt32, false),
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("node_pops", DataType::UInt32, false),
        Field::new("scored_internal_nodes", DataType::UInt32, false),
        Field::new("fma_backend", DataType::Utf8, false),
        Field::new("objective_value", DataType::UInt64, false),
        Field::new("candidate_count", DataType::UInt32, false),
        Field::new("marginal_recomputations", DataType::UInt64, false),
    ])
}

pub(crate) fn load_v40_projected_queries(
    path: &Path,
    expected_queries: u64,
) -> Result<Vec<Vec<f32>>> {
    let file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    load_v40_projected_queries_file(file, path, expected_queries)
}

pub(crate) fn load_v40_projected_queries_file(
    file: File,
    display_path: &Path,
    expected_queries: u64,
) -> Result<Vec<Vec<f32>>> {
    if expected_queries == 0 {
        return Err(BorsukError::InvalidStorage(
            "V40 direct query count differs".to_owned(),
        ));
    }
    let capacity = usize::try_from(expected_queries).map_err(|_| {
        BorsukError::InvalidStorage("V40 direct query count exceeds address space".to_owned())
    })?;
    let projection = crate::v36_funnel_geometry::build_v36_srht192_control()?;
    let mut projected_queries = Vec::with_capacity(capacity);
    crate::v36_prefix_dataset::scan_v36_prefix_query_parquet_file(
        file,
        display_path,
        expected_queries,
        |batch| {
            for row in crate::v36_prefix_dataset::v36_prefix_query_rows_from_batch(
                &batch,
                0,
                batch.num_rows(),
            )? {
                let projected =
                    crate::v35_projection::project_v35_query_simd(&projection, &row.embedding)?;
                projected_queries.push(
                    projected
                        .coordinates()
                        .iter()
                        .map(|value| {
                            let value = *value as f32;
                            if value == 0.0 { 0.0 } else { value }
                        })
                        .collect(),
                );
            }
            Ok(())
        },
    )?;
    if projected_queries.len() != capacity {
        return Err(BorsukError::InvalidStorage(
            "V40 direct projected query count differs".to_owned(),
        ));
    }
    Ok(projected_queries)
}

fn validate_v40_direct_selections(
    records: &[V40DirectSelectionRecord],
    selected_postings: usize,
    expected_backend: Option<&str>,
) -> Result<()> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct selection authority differs".to_owned());
    if records.is_empty() || selected_postings == 0 {
        return Err(invalid());
    }
    for (query, record) in records.iter().enumerate() {
        let expected_query = u32::try_from(query).map_err(|_| invalid())?;
        let postings = record
            .posting_ordinals
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if record.query_ordinal != expected_query
            || record.posting_ordinals.len() != selected_postings
            || postings.len() != selected_postings
            || record.node_pops == 0
            || record.scored_internal_nodes == 0
            || record.scored_internal_nodes > record.node_pops
            || !matches!(
                record.fma_backend.as_str(),
                "aarch64-neon-fma" | "x86-avx-fma"
            )
            || expected_backend.is_some_and(|backend| record.fma_backend != backend)
        {
            return Err(invalid());
        }
    }
    if records
        .windows(2)
        .any(|pair| pair[0].fma_backend != pair[1].fma_backend)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn encode_v40_direct_selections_parquet(
    records: &[V40DirectSelectionRecord],
    selected_postings: usize,
) -> Result<Vec<u8>> {
    validate_v40_direct_selections(records, selected_postings, None)?;
    let rows = records
        .len()
        .checked_mul(selected_postings)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V40 direct selection row count overflows".to_owned())
        })?;
    let mut queries = Vec::with_capacity(rows);
    let mut ranks = Vec::with_capacity(rows);
    let mut postings = Vec::with_capacity(rows);
    let mut node_pops = Vec::with_capacity(rows);
    let mut scores = Vec::with_capacity(rows);
    let mut backends = Vec::with_capacity(rows);
    for record in records {
        for (rank, posting) in record.posting_ordinals.iter().copied().enumerate() {
            queries.push(record.query_ordinal);
            ranks.push(u32::try_from(rank).map_err(|_| {
                BorsukError::InvalidStorage("V40 direct selection rank overflows".to_owned())
            })?);
            postings.push(posting);
            node_pops.push(record.node_pops);
            scores.push(record.scored_internal_nodes);
            backends.push(record.fma_backend.clone());
        }
    }
    let schema = Arc::new(v40_direct_selection_schema());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(queries)),
            Arc::new(UInt32Array::from(ranks)),
            Arc::new(UInt32Array::from(postings)),
            Arc::new(UInt32Array::from(node_pops)),
            Arc::new(UInt32Array::from(scores)),
            Arc::new(StringArray::from(backends)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(4_096))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn decode_v40_direct_selections_parquet(
    bytes: &[u8],
    expected_queries: u32,
    selected_postings: usize,
    expected_backend: &str,
) -> Result<Vec<V40DirectSelectionRecord>> {
    let invalid = || BorsukError::InvalidStorage("V40 direct selection Parquet differs".to_owned());
    if expected_queries == 0 || selected_postings == 0 {
        return Err(invalid());
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if builder.schema().as_ref() != &v40_direct_selection_schema() {
        return Err(invalid());
    }
    let expected_rows = usize::try_from(expected_queries)
        .map_err(|_| invalid())?
        .checked_mul(selected_postings)
        .ok_or_else(invalid)?;
    let mut records = Vec::with_capacity(expected_queries as usize);
    let mut observed_rows = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.schema().as_ref() != &v40_direct_selection_schema()
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid());
        }
        let queries = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let ranks = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let postings = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let node_pops = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let scores = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let backends = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        for row in 0..batch.num_rows() {
            let flat_row = observed_rows.checked_add(row).ok_or_else(invalid)?;
            let query = flat_row / selected_postings;
            let rank = flat_row % selected_postings;
            if queries.value(row) as usize != query
                || ranks.value(row) as usize != rank
                || backends.value(row) != expected_backend
            {
                return Err(invalid());
            }
            if rank == 0 {
                records.push(V40DirectSelectionRecord {
                    query_ordinal: queries.value(row),
                    posting_ordinals: Vec::with_capacity(selected_postings),
                    node_pops: node_pops.value(row),
                    scored_internal_nodes: scores.value(row),
                    fma_backend: backends.value(row).to_owned(),
                });
            }
            let record = records.get_mut(query).ok_or_else(invalid)?;
            if record.node_pops != node_pops.value(row)
                || record.scored_internal_nodes != scores.value(row)
                || record.fma_backend != backends.value(row)
            {
                return Err(invalid());
            }
            record.posting_ordinals.push(postings.value(row));
        }
        observed_rows = observed_rows
            .checked_add(batch.num_rows())
            .ok_or_else(invalid)?;
    }
    if observed_rows != expected_rows || records.len() != expected_queries as usize {
        return Err(invalid());
    }
    validate_v40_direct_selections(
        records.as_slice(),
        selected_postings,
        Some(expected_backend),
    )?;
    Ok(records)
}

fn validate_v40_accepted_selections(
    records: &[V40AcceptedSelectionRecord],
    selected_postings: usize,
    expected_backend: Option<&str>,
) -> Result<()> {
    let direct = records
        .iter()
        .map(|record| V40DirectSelectionRecord {
            query_ordinal: record.query_ordinal,
            posting_ordinals: record.posting_ordinals.clone(),
            node_pops: record.node_pops,
            scored_internal_nodes: record.scored_internal_nodes,
            fma_backend: record.fma_backend.clone(),
        })
        .collect::<Vec<_>>();
    validate_v40_direct_selections(&direct, selected_postings, expected_backend)?;
    if records.iter().any(|record| {
        record.objective_value == 0
            || record.candidate_count < selected_postings as u32
            || record.marginal_recomputations < u64::from(record.candidate_count)
    }) {
        return Err(BorsukError::InvalidStorage(
            "V40 accepted selection authority differs".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn encode_v40_accepted_selections_parquet(
    records: &[V40AcceptedSelectionRecord],
    selected_postings: usize,
) -> Result<Vec<u8>> {
    validate_v40_accepted_selections(records, selected_postings, None)?;
    let rows = records
        .len()
        .checked_mul(selected_postings)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V40 accepted selection row count overflows".to_owned())
        })?;
    let mut queries = Vec::with_capacity(rows);
    let mut ranks = Vec::with_capacity(rows);
    let mut postings = Vec::with_capacity(rows);
    let mut node_pops = Vec::with_capacity(rows);
    let mut scores = Vec::with_capacity(rows);
    let mut backends = Vec::with_capacity(rows);
    let mut objectives = Vec::with_capacity(rows);
    let mut candidate_counts = Vec::with_capacity(rows);
    let mut recomputations = Vec::with_capacity(rows);
    for record in records {
        for (rank, posting) in record.posting_ordinals.iter().copied().enumerate() {
            queries.push(record.query_ordinal);
            ranks.push(u32::try_from(rank).map_err(|_| {
                BorsukError::InvalidStorage("V40 accepted selection rank overflows".to_owned())
            })?);
            postings.push(posting);
            node_pops.push(record.node_pops);
            scores.push(record.scored_internal_nodes);
            backends.push(record.fma_backend.clone());
            objectives.push(record.objective_value);
            candidate_counts.push(record.candidate_count);
            recomputations.push(record.marginal_recomputations);
        }
    }
    let schema = Arc::new(v40_accepted_selection_schema());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(queries)),
            Arc::new(UInt32Array::from(ranks)),
            Arc::new(UInt32Array::from(postings)),
            Arc::new(UInt32Array::from(node_pops)),
            Arc::new(UInt32Array::from(scores)),
            Arc::new(StringArray::from(backends)),
            Arc::new(UInt64Array::from(objectives)),
            Arc::new(UInt32Array::from(candidate_counts)),
            Arc::new(UInt64Array::from(recomputations)),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(4_096))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn decode_v40_accepted_selections_parquet(
    bytes: &[u8],
    expected_queries: u32,
    selected_postings: usize,
    expected_backend: &str,
) -> Result<Vec<V40AcceptedSelectionRecord>> {
    let invalid =
        || BorsukError::InvalidStorage("V40 accepted selection Parquet differs".to_owned());
    if expected_queries == 0 || selected_postings == 0 {
        return Err(invalid());
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if builder.schema().as_ref() != &v40_accepted_selection_schema() {
        return Err(invalid());
    }
    let expected_rows = usize::try_from(expected_queries)
        .map_err(|_| invalid())?
        .checked_mul(selected_postings)
        .ok_or_else(invalid)?;
    let mut records = Vec::with_capacity(expected_queries as usize);
    let mut observed_rows = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.schema().as_ref() != &v40_accepted_selection_schema()
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid());
        }
        let queries = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let ranks = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let postings = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let node_pops = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let scores = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let backends = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        let objectives = batch
            .column(6)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        let candidate_counts = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(invalid)?;
        let recomputations = batch
            .column(8)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(invalid)?;
        for row in 0..batch.num_rows() {
            let flat_row = observed_rows.checked_add(row).ok_or_else(invalid)?;
            let query = flat_row / selected_postings;
            let rank = flat_row % selected_postings;
            if queries.value(row) as usize != query
                || ranks.value(row) as usize != rank
                || backends.value(row) != expected_backend
            {
                return Err(invalid());
            }
            if rank == 0 {
                records.push(V40AcceptedSelectionRecord {
                    query_ordinal: queries.value(row),
                    posting_ordinals: Vec::with_capacity(selected_postings),
                    node_pops: node_pops.value(row),
                    scored_internal_nodes: scores.value(row),
                    fma_backend: backends.value(row).to_owned(),
                    objective_value: objectives.value(row),
                    candidate_count: candidate_counts.value(row),
                    marginal_recomputations: recomputations.value(row),
                });
            }
            let record = records.get_mut(query).ok_or_else(invalid)?;
            if record.node_pops != node_pops.value(row)
                || record.scored_internal_nodes != scores.value(row)
                || record.fma_backend != backends.value(row)
                || record.objective_value != objectives.value(row)
                || record.candidate_count != candidate_counts.value(row)
                || record.marginal_recomputations != recomputations.value(row)
            {
                return Err(invalid());
            }
            record.posting_ordinals.push(postings.value(row));
        }
        observed_rows = observed_rows
            .checked_add(batch.num_rows())
            .ok_or_else(invalid)?;
    }
    if observed_rows != expected_rows || records.len() != expected_queries as usize {
        return Err(invalid());
    }
    validate_v40_accepted_selections(&records, selected_postings, Some(expected_backend))?;
    Ok(records)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40TreeFrontier {
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) fma_backend: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V40EvaluationSpec {
    pub(crate) selected_postings: u32,
    pub(crate) gt_neighbors: u32,
    pub(crate) aggregate_gate_ppm: u32,
    pub(crate) minimum_gate_ppm: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectSelectionRecord {
    pub(crate) query_ordinal: u32,
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) fma_backend: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40AcceptedSelectionRecord {
    pub(crate) query_ordinal: u32,
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) fma_backend: String,
    pub(crate) objective_value: u64,
    pub(crate) candidate_count: u32,
    pub(crate) marginal_recomputations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectSample {
    pub(crate) query_ordinal: u32,
    pub(crate) selected_postings: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) hits: u32,
    pub(crate) recall_ppm: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectEvaluation {
    pub(crate) samples: Vec<V40DirectSample>,
    pub(crate) total_hits: u64,
    pub(crate) aggregate_recall_ppm: u32,
    pub(crate) minimum_recall_ppm: u32,
    pub(crate) passed: bool,
    pub(crate) disposition: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectFailurePrerequisite {
    pub(crate) aggregate_recall_ppm: u32,
    pub(crate) minimum_recall_ppm: u32,
    pub(crate) fma_backend: String,
    query_count: u64,
    selected_postings: u32,
    gt_neighbors: u32,
    aggregate_gate_ppm: u32,
    minimum_gate_ppm: u32,
    inputs: Vec<V40SelectionReceiptArtifact>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40EvaluationResultSample {
    hits: u32,
    node_pops: u32,
    query_ordinal: u32,
    recall_ppm: u32,
    scored_internal_nodes: u32,
    selected_postings: Vec<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40EvaluationResultEvidence {
    aggregate_gate_ppm: u32,
    aggregate_recall_ppm: u32,
    disposition: String,
    fma_backend: String,
    gt_neighbors: u32,
    minimum_gate_ppm: u32,
    minimum_recall_ppm: u32,
    passed: bool,
    query_count: u64,
    samples: Vec<V40EvaluationResultSample>,
    selected_postings: u32,
    total_hits: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V40EvaluationResult {
    claim_eligible: bool,
    evidence: V40EvaluationResultEvidence,
    inputs: Vec<V40SelectionReceiptArtifact>,
    mode: String,
    schema: String,
}

pub(crate) fn parse_v40_direct_failure_result_bytes(
    bytes: &[u8],
) -> Result<V40DirectFailurePrerequisite> {
    let invalid = || BorsukError::InvalidStorage("V40 direct failure authority differs".to_owned());
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let mut canonical = serde_json::to_vec(&v40_canonical_json(value)).map_err(|_| invalid())?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(invalid());
    }
    let result: V40EvaluationResult = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let evidence = &result.evidence;
    let input_roles = result
        .inputs
        .iter()
        .map(|input| input.role.as_str())
        .collect::<Vec<_>>();
    let input_uris = result
        .inputs
        .iter()
        .map(|input| input.uri.as_str())
        .collect::<BTreeSet<_>>();
    if result.schema != "borsuk-v40-local-result-v1"
        || result.claim_eligible
        || result.mode != "evaluate-direct"
        || evidence.passed
        || evidence.disposition != "direct-failed"
        || evidence.aggregate_gate_ppm > 1_000_000
        || evidence.minimum_gate_ppm > 1_000_000
        || evidence.aggregate_recall_ppm >= evidence.aggregate_gate_ppm
            && evidence.minimum_recall_ppm >= evidence.minimum_gate_ppm
        || evidence.gt_neighbors == 0
        || evidence.selected_postings == 0
        || evidence.query_count == 0
        || evidence.query_count != evidence.samples.len() as u64
        || input_roles != V40LocalRunMode::EvaluateDirect.input_roles()
        || input_uris.len() != result.inputs.len()
        || !matches!(
            evidence.fma_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || result.inputs.is_empty()
        || result.inputs.iter().any(|input| {
            !valid_s3_uri(&input.uri)
                || !valid_digest(&input.sha256)
                || !valid_digest(&input.blake3)
                || input.encoded_bytes == 0
        })
    {
        return Err(invalid());
    }
    let selected_postings = usize::try_from(evidence.selected_postings).map_err(|_| invalid())?;
    let mut total_hits = 0_u64;
    let mut minimum = 1_000_000_u32;
    for (query, sample) in evidence.samples.iter().enumerate() {
        let unique = sample
            .selected_postings
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let recall = u32::try_from(
            u64::from(sample.hits)
                .checked_mul(1_000_000)
                .ok_or_else(invalid)?
                / u64::from(evidence.gt_neighbors),
        )
        .map_err(|_| invalid())?;
        if sample.query_ordinal != u32::try_from(query).map_err(|_| invalid())?
            || sample.selected_postings.len() != selected_postings
            || unique.len() != selected_postings
            || sample.node_pops == 0
            || sample.node_pops as usize > V40_MAXIMUM_NODE_POPS
            || sample.scored_internal_nodes == 0
            || sample.scored_internal_nodes > sample.node_pops
            || sample.hits > evidence.gt_neighbors
            || sample.recall_ppm != recall
        {
            return Err(invalid());
        }
        total_hits = total_hits
            .checked_add(u64::from(sample.hits))
            .ok_or_else(invalid)?;
        minimum = minimum.min(sample.recall_ppm);
    }
    let possible = evidence
        .query_count
        .checked_mul(u64::from(evidence.gt_neighbors))
        .ok_or_else(invalid)?;
    let aggregate =
        u32::try_from(total_hits.checked_mul(1_000_000).ok_or_else(invalid)? / possible)
            .map_err(|_| invalid())?;
    if total_hits != evidence.total_hits
        || aggregate != evidence.aggregate_recall_ppm
        || minimum != evidence.minimum_recall_ppm
    {
        return Err(invalid());
    }
    Ok(V40DirectFailurePrerequisite {
        aggregate_recall_ppm: aggregate,
        minimum_recall_ppm: minimum,
        fma_backend: evidence.fma_backend.clone(),
        query_count: evidence.query_count,
        selected_postings: evidence.selected_postings,
        gt_neighbors: evidence.gt_neighbors,
        aggregate_gate_ppm: evidence.aggregate_gate_ppm,
        minimum_gate_ppm: evidence.minimum_gate_ppm,
        inputs: result.inputs,
    })
}

pub(crate) fn evaluate_v40_accepted_recall(
    spec: &V40EvaluationSpec,
    owners: &[(u64, u32, Option<u32>)],
    selections: &[V40AcceptedSelectionRecord],
    truth: &[V37FeatureGroundTruth],
    expected_backend: &str,
) -> Result<V40DirectEvaluation> {
    validate_v40_accepted_selections(
        selections,
        usize::try_from(spec.selected_postings).map_err(|_| {
            BorsukError::InvalidStorage("V40 accepted evaluation authority differs".to_owned())
        })?,
        Some(expected_backend),
    )?;
    let direct = selections
        .iter()
        .map(|selection| V40DirectSelectionRecord {
            query_ordinal: selection.query_ordinal,
            posting_ordinals: selection.posting_ordinals.clone(),
            node_pops: selection.node_pops,
            scored_internal_nodes: selection.scored_internal_nodes,
            fma_backend: selection.fma_backend.clone(),
        })
        .collect::<Vec<_>>();
    let mut evaluation =
        evaluate_v40_direct_recall(spec, owners, &direct, truth, expected_backend)?;
    evaluation.disposition = if evaluation.passed {
        "challenger-passed"
    } else {
        "router-rejected"
    }
    .to_owned();
    Ok(evaluation)
}

pub(crate) fn select_v40_tree_frontier(
    tree: &V37BalancedTree,
    expected_backend: &str,
    query: &[f32],
    posting_limit: usize,
    maximum_node_pops: usize,
) -> Result<V40TreeFrontier> {
    if posting_limit == 0
        || posting_limit > V40_MAXIMUM_FRONTIER_POSTINGS
        || maximum_node_pops == 0
        || maximum_node_pops > V40_MAXIMUM_NODE_POPS
    {
        return Err(BorsukError::InvalidStorage(
            "V40 tree frontier authority differs".to_owned(),
        ));
    }
    let selection = select_v37_tree_postings_with_limit(
        tree,
        expected_backend,
        maximum_node_pops,
        posting_limit,
        query,
    )?;
    Ok(V40TreeFrontier {
        posting_ordinals: selection.selected_postings,
        node_pops: selection.node_visits,
        scored_internal_nodes: selection.scored_internal_nodes,
        fma_backend: selection.fma_backend,
    })
}

pub(crate) fn select_v40_direct_queries(
    tree: &V37BalancedTree,
    expected_backend: &str,
    queries: &[Vec<f32>],
    selected_postings: usize,
    maximum_node_pops: usize,
) -> Result<Vec<V40DirectSelectionRecord>> {
    let invalid = || BorsukError::InvalidStorage("V40 direct query authority differs".to_owned());
    if queries.is_empty() {
        return Err(invalid());
    }
    let mut records = Vec::with_capacity(queries.len());
    for (query_ordinal, query) in queries.iter().enumerate() {
        let frontier = select_v40_tree_frontier(
            tree,
            expected_backend,
            query,
            selected_postings,
            maximum_node_pops,
        )?;
        records.push(V40DirectSelectionRecord {
            query_ordinal: u32::try_from(query_ordinal).map_err(|_| invalid())?,
            posting_ordinals: frontier.posting_ordinals,
            node_pops: frontier.node_pops,
            scored_internal_nodes: frontier.scored_internal_nodes,
            fma_backend: frontier.fma_backend,
        });
    }
    validate_v40_direct_selections(&records, selected_postings, Some(expected_backend))?;
    Ok(records)
}

/// Run one authenticated local V40 direct phase without any storage client.
#[doc(hidden)]
pub fn run_v40_local_request(request: V40LocalRunRequest) -> Result<Vec<u8>> {
    let deferred_role = matches!(
        request.mode,
        V40LocalRunMode::EvaluateDirect | V40LocalRunMode::EvaluateAcceptedSpill
    )
    .then_some("development-ground-truth");
    let mut authenticated = authenticate_v40_local_request_deferred(&request, deferred_role)?;
    match request.mode {
        V40LocalRunMode::BuildSpillSummary => {
            return run_v40_build_spill_summary(&request, &authenticated);
        }
        V40LocalRunMode::SelectAcceptedSpill => {
            return run_v40_select_accepted_spill(&request, &authenticated);
        }
        V40LocalRunMode::EvaluateDirect => {
            return run_v40_evaluate_direct(&request, &mut authenticated);
        }
        V40LocalRunMode::EvaluateAcceptedSpill => {
            return run_v40_evaluate_accepted_spill(&request, &mut authenticated);
        }
        V40LocalRunMode::SelectDirect => {}
    }
    let cohort_bytes = read_v40_authenticated_input(&request, &authenticated, "cohort-authority")?;
    let cohort = parse_v40_direct_cohort_authority_bytes(&cohort_bytes)?;
    for (role, expected) in [
        ("v37-authority", &cohort.v37_authority),
        ("ownership-tree", &cohort.ownership_tree),
        ("development-query", &cohort.development_query),
    ] {
        if !v40_cohort_identity_matches_local(expected, v40_local_input(&request, role)?) {
            return Err(BorsukError::InvalidStorage(
                "V40 direct cohort input binding differs".to_owned(),
            ));
        }
    }
    let authority_bytes = read_v40_authenticated_input(&request, &authenticated, "v37-authority")?;
    let binding = crate::v37_relation_router::v37_v40_selection_binding(&authority_bytes)?;
    if binding.workers != request.workers {
        return Err(BorsukError::InvalidStorage(
            "V40 direct worker authority differs".to_owned(),
        ));
    }
    let tree_input = v40_local_input(&request, "ownership-tree")?;
    let tree_bytes = read_v40_authenticated_input(&request, &authenticated, "ownership-tree")?;
    let tree = crate::v37_relation_router::decode_v37_tree_arrow(
        &tree_bytes,
        tree_input.encoded_bytes,
        &tree_input.sha256,
        &tree_input.blake3,
    )?;
    let corpus_rows = tree
        .leaf_populations
        .iter()
        .try_fold(0_u64, |total, population| total.checked_add(*population))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V40 direct tree population overflows".to_owned())
        })?;
    if tree.dimensions as u64 != binding.dimensions
        || tree.seed != binding.tree_seed
        || tree.fma_backend != binding.fma_backend
        || tree.leaf_populations.len() as u64 != binding.leaf_count
        || corpus_rows != binding.corpus_rows
    {
        return Err(BorsukError::InvalidStorage(
            "V40 direct tree binding differs".to_owned(),
        ));
    }
    let query_input = v40_local_input(&request, "development-query")?;
    let queries = load_v40_projected_queries_file(
        v40_authenticated_input_file(&request, &authenticated, "development-query")?,
        &query_input.path,
        V40_DIRECT_QUERY_COUNT,
    )?;
    let maximum_node_pops = V40_MAXIMUM_NODE_POPS.min(tree.nodes.len());
    let selections = select_v40_direct_queries(
        &tree,
        &binding.fma_backend,
        &queries,
        V40_DIRECT_SELECTED_POSTINGS,
        maximum_node_pops,
    )?;
    let output_bytes =
        encode_v40_direct_selections_parquet(&selections, V40_DIRECT_SELECTED_POSTINGS)?;
    let output = request.outputs.first().ok_or_else(|| {
        BorsukError::InvalidStorage("V40 direct selection output differs".to_owned())
    })?;
    publish_v40_output(&output.path, &output_bytes)?;
    v40_selection_receipt_bytes(&request, &selections, &output_bytes)
}

fn require_v40_direct_failure_bindings(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
) -> Result<V40DirectFailurePrerequisite> {
    let invalid = || BorsukError::InvalidStorage("V40 direct failure binding differs".to_owned());
    let direct_bytes = read_v40_authenticated_input(request, authenticated, "direct-result")?;
    let prerequisite = parse_v40_direct_failure_result_bytes(&direct_bytes)?;
    if prerequisite.query_count != V40_DIRECT_QUERY_COUNT
        || prerequisite.selected_postings != V40_DIRECT_SELECTED_POSTINGS as u32
        || prerequisite.gt_neighbors != 100
        || prerequisite.aggregate_gate_ppm != 998_000
        || prerequisite.minimum_gate_ppm != 800_000
    {
        return Err(invalid());
    }
    for (receipt_index, role) in [
        (0, "cohort-authority"),
        (2, "v38-construction-result"),
        (3, "spill-relation"),
        (4, "spill-postings"),
    ] {
        if let Some(observed) = request.inputs.iter().find(|input| input.role == role) {
            let expected = prerequisite.inputs.get(receipt_index).ok_or_else(invalid)?;
            if !v40_cohort_identity_matches_local(expected, observed) {
                return Err(invalid());
            }
        }
    }
    Ok(prerequisite)
}

fn run_v40_build_spill_summary(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
) -> Result<Vec<u8>> {
    let invalid = || BorsukError::InvalidStorage("V40 spill summary build differs".to_owned());
    let _failure = require_v40_direct_failure_bindings(request, authenticated)?;
    let counts = build_v40_spill_counts_from_parquet_files(
        v40_authenticated_input_file(request, authenticated, "spill-relation")?,
        v40_authenticated_input_file(request, authenticated, "spill-postings")?,
        V40_DEVELOPMENT_CORPUS_ROWS,
        V40_DEVELOPMENT_POSTING_COUNT,
        V40_DEVELOPMENT_MAXIMUM_ROWS_PER_POSTING,
    )?;
    let summary = pack_v40_spill_summary(&counts, V40_DEVELOPMENT_POSTING_COUNT)?;
    let counts_bytes = encode_v40_spill_counts_parquet(&counts, V40_DEVELOPMENT_POSTING_COUNT)?;
    let summary_bytes = encode_v40_packed_spill_summary_arrow(&summary)?;
    let counts_output = v40_local_output(request, "spill-counts")?;
    let summary_output = v40_local_output(request, "spill-summary")?;
    publish_v40_output(&counts_output.path, &counts_bytes)?;
    publish_v40_output(&summary_output.path, &summary_bytes)?;
    let receipt = v40_phase_receipt_bytes(
        request,
        "build-spill-summary",
        &[
            (counts_output, &counts_bytes),
            (summary_output, &summary_bytes),
        ],
        serde_json::json!({
            "posting_count": V40_DEVELOPMENT_POSTING_COUNT,
            "retained_alternate_categories": summary.alternate_postings.len(),
        }),
    )?;
    publish_v40_output(
        &v40_local_output(request, "spill-summary-result")?.path,
        &receipt,
    )?;
    if receipt.is_empty() {
        return Err(invalid());
    }
    Ok(receipt)
}

fn validate_v40_summary_receipt(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
) -> Result<()> {
    let invalid = || BorsukError::InvalidStorage("V40 spill summary receipt differs".to_owned());
    let receipt_bytes =
        read_v40_authenticated_input(request, authenticated, "spill-summary-result")?;
    let receipt = parse_v40_phase_receipt(&receipt_bytes, "build-spill-summary")?;
    let direct_bytes = read_v40_authenticated_input(request, authenticated, "direct-result")?;
    let direct = parse_v40_direct_failure_result_bytes(&direct_bytes)?;
    let summary = v40_local_input(request, "spill-summary")?;
    let summary_bytes = read_v40_authenticated_input(request, authenticated, "spill-summary")?;
    let decoded =
        decode_v40_packed_spill_summary_arrow(&summary_bytes, V40_DEVELOPMENT_POSTING_COUNT)?;
    let artifact = receipt
        .artifacts
        .iter()
        .find(|artifact| artifact.role == "spill-summary")
        .ok_or_else(invalid)?;
    let input_roles = receipt
        .inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>();
    let artifact_roles = receipt
        .artifacts
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>();
    if input_roles != V40LocalRunMode::BuildSpillSummary.input_roles()
        || artifact_roles != ["spill-counts", "spill-summary"]
        || !v40_cohort_identity_matches_local(
            receipt.inputs.first().ok_or_else(invalid)?,
            v40_local_input(request, "cohort-authority")?,
        )
        || !v40_cohort_identity_matches_local(
            receipt.inputs.get(1).ok_or_else(invalid)?,
            v40_local_input(request, "direct-result")?,
        )
        || receipt.inputs.get(2..5) != direct.inputs.get(2..5)
        || receipt
            .evidence
            .get("posting_count")
            .and_then(serde_json::Value::as_u64)
            != Some(V40_DEVELOPMENT_POSTING_COUNT as u64)
        || receipt
            .evidence
            .get("retained_alternate_categories")
            .and_then(serde_json::Value::as_u64)
            != Some(decoded.alternate_postings.len() as u64)
        || artifact.sha256 != summary.sha256
        || artifact.blake3 != summary.blake3
        || artifact.encoded_bytes != summary.encoded_bytes
    {
        return Err(invalid());
    }
    Ok(())
}

fn run_v40_select_accepted_spill(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
) -> Result<Vec<u8>> {
    let invalid = || BorsukError::InvalidStorage("V40 accepted spill selection differs".to_owned());
    let prerequisite = require_v40_direct_failure_bindings(request, authenticated)?;
    validate_v40_summary_receipt(request, authenticated)?;
    let cohort_bytes = read_v40_authenticated_input(request, authenticated, "cohort-authority")?;
    let cohort = parse_v40_direct_cohort_authority_bytes(&cohort_bytes)?;
    for (role, expected) in [
        ("v37-authority", &cohort.v37_authority),
        ("ownership-tree", &cohort.ownership_tree),
        ("development-query", &cohort.development_query),
    ] {
        if !v40_cohort_identity_matches_local(expected, v40_local_input(request, role)?) {
            return Err(invalid());
        }
    }
    let authority = read_v40_authenticated_input(request, authenticated, "v37-authority")?;
    let binding = crate::v37_relation_router::v37_v40_selection_binding(&authority)?;
    if binding.workers != request.workers || binding.fma_backend != prerequisite.fma_backend {
        return Err(invalid());
    }
    let tree_input = v40_local_input(request, "ownership-tree")?;
    let tree_bytes = read_v40_authenticated_input(request, authenticated, "ownership-tree")?;
    let tree = crate::v37_relation_router::decode_v37_tree_arrow(
        &tree_bytes,
        tree_input.encoded_bytes,
        &tree_input.sha256,
        &tree_input.blake3,
    )?;
    let corpus_rows = tree
        .leaf_populations
        .iter()
        .try_fold(0_u64, |total, population| total.checked_add(*population))
        .ok_or_else(invalid)?;
    if tree.dimensions as u64 != binding.dimensions
        || tree.seed != binding.tree_seed
        || tree.fma_backend != binding.fma_backend
        || tree.leaf_populations.len() as u64 != binding.leaf_count
        || corpus_rows != binding.corpus_rows
    {
        return Err(invalid());
    }
    let query_input = v40_local_input(request, "development-query")?;
    let queries = load_v40_projected_queries_file(
        v40_authenticated_input_file(request, authenticated, "development-query")?,
        &query_input.path,
        V40_DIRECT_QUERY_COUNT,
    )?;
    let summary_bytes = read_v40_authenticated_input(request, authenticated, "spill-summary")?;
    let summary =
        decode_v40_packed_spill_summary_arrow(&summary_bytes, V40_DEVELOPMENT_POSTING_COUNT)?;
    let mut selections = Vec::with_capacity(queries.len());
    for (query_ordinal, query) in queries.iter().enumerate() {
        let frontier = select_v40_tree_frontier(
            &tree,
            &binding.fma_backend,
            query,
            V40_MAXIMUM_FRONTIER_POSTINGS.min(V40_DEVELOPMENT_POSTING_COUNT as usize),
            V40_MAXIMUM_NODE_POPS.min(tree.nodes.len()),
        )?;
        let selected =
            select_v40_accepted_spill_postings(&frontier, &summary, V40_DIRECT_SELECTED_POSTINGS)?;
        selections.push(V40AcceptedSelectionRecord {
            query_ordinal: u32::try_from(query_ordinal).map_err(|_| invalid())?,
            posting_ordinals: selected.posting_ordinals,
            node_pops: frontier.node_pops,
            scored_internal_nodes: frontier.scored_internal_nodes,
            fma_backend: frontier.fma_backend,
            objective_value: u64::try_from(selected.objective_value).map_err(|_| invalid())?,
            candidate_count: selected.candidate_count,
            marginal_recomputations: selected.marginal_recomputations,
        });
    }
    let selection_bytes =
        encode_v40_accepted_selections_parquet(&selections, V40_DIRECT_SELECTED_POSTINGS)?;
    let selection_output = v40_local_output(request, "accepted-selection")?;
    publish_v40_output(&selection_output.path, &selection_bytes)?;
    let receipt = v40_phase_receipt_bytes(
        request,
        "select-accepted-spill",
        &[(selection_output, &selection_bytes)],
        serde_json::json!({
            "fma_backend": prerequisite.fma_backend,
            "frontier_postings": V40_MAXIMUM_FRONTIER_POSTINGS,
            "query_count": selections.len(),
            "selected_postings": V40_DIRECT_SELECTED_POSTINGS,
        }),
    )?;
    publish_v40_output(
        &v40_local_output(request, "accepted-selection-result")?.path,
        &receipt,
    )?;
    Ok(receipt)
}

fn validate_v40_accepted_selection_receipt(
    request: &V40LocalRunRequest,
    authenticated: &V40AuthenticatedLocalInputs,
    cohort: &V40DirectCohortAuthority,
) -> Result<String> {
    let invalid =
        || BorsukError::InvalidStorage("V40 accepted selection receipt differs".to_owned());
    let bytes = read_v40_authenticated_input(request, authenticated, "accepted-selection-result")?;
    let receipt = parse_v40_phase_receipt(&bytes, "select-accepted-spill")?;
    let selection = v40_local_input(request, "accepted-selection")?;
    let artifact = receipt.artifacts.first().ok_or_else(invalid)?;
    let evidence = receipt.evidence.as_object().ok_or_else(invalid)?;
    let backend = evidence
        .get("fma_backend")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(invalid)?;
    if receipt.artifacts.len() != 1
        || artifact.role != "accepted-selection"
        || artifact.sha256 != selection.sha256
        || artifact.blake3 != selection.blake3
        || artifact.encoded_bytes != selection.encoded_bytes
        || receipt.inputs.len() != V40LocalRunMode::SelectAcceptedSpill.input_roles().len()
        || evidence.len() != 4
        || evidence
            .get("frontier_postings")
            .and_then(serde_json::Value::as_u64)
            != Some(V40_MAXIMUM_FRONTIER_POSTINGS as u64)
        || evidence
            .get("query_count")
            .and_then(serde_json::Value::as_u64)
            != Some(V40_DIRECT_QUERY_COUNT)
        || evidence
            .get("selected_postings")
            .and_then(serde_json::Value::as_u64)
            != Some(V40_DIRECT_SELECTED_POSTINGS as u64)
        || !matches!(backend, "aarch64-neon-fma" | "x86-avx-fma")
        || !v40_cohort_identity_matches_local(
            receipt.inputs.first().ok_or_else(invalid)?,
            v40_local_input(request, "cohort-authority")?,
        )
        || !v40_cohort_identity_matches_local(
            receipt.inputs.get(1).ok_or_else(invalid)?,
            v40_local_input(request, "direct-result")?,
        )
        || receipt.inputs.get(2) != Some(&cohort.v37_authority)
        || receipt.inputs.get(3) != Some(&cohort.ownership_tree)
        || receipt.inputs.get(4) != Some(&cohort.development_query)
        || !v40_cohort_identity_matches_local(
            receipt.inputs.get(5).ok_or_else(invalid)?,
            v40_local_input(request, "spill-summary-result")?,
        )
        || !v40_cohort_identity_matches_local(
            receipt.inputs.get(6).ok_or_else(invalid)?,
            v40_local_input(request, "spill-summary")?,
        )
    {
        return Err(invalid());
    }
    Ok(backend.to_owned())
}

fn run_v40_evaluate_accepted_spill(
    request: &V40LocalRunRequest,
    authenticated: &mut V40AuthenticatedLocalInputs,
) -> Result<Vec<u8>> {
    let invalid = || BorsukError::InvalidStorage("V40 accepted evaluation differs".to_owned());
    let prerequisite = require_v40_direct_failure_bindings(request, authenticated)?;
    validate_v40_summary_receipt(request, authenticated)?;
    let ceiling = read_v40_authenticated_input(request, authenticated, "v38-ceiling-authority")?;
    let construction =
        read_v40_authenticated_input(request, authenticated, "v38-construction-result")?;
    let binding = v38_v40_evaluation_binding(&ceiling, &construction)?;
    let cohort_bytes = read_v40_authenticated_input(request, authenticated, "cohort-authority")?;
    let cohort = parse_v40_direct_cohort_authority_bytes(&cohort_bytes)?;
    let backend = validate_v40_accepted_selection_receipt(request, authenticated, &cohort)?;
    if backend != prerequisite.fma_backend
        || cohort.query_count != u64::from(binding.query_count)
        || !v40_cohort_identity_matches_local(
            &cohort.development_ground_truth,
            v40_local_input(request, "development-ground-truth")?,
        )
    {
        return Err(invalid());
    }
    for (role, uri, sha256, blake3, encoded_bytes) in [
        (
            "spill-relation",
            binding.relation_uri.as_str(),
            binding.relation_sha256.as_str(),
            binding.relation_blake3.as_str(),
            binding.relation_bytes,
        ),
        (
            "spill-postings",
            binding.postings_uri.as_str(),
            binding.postings_sha256.as_str(),
            binding.postings_blake3.as_str(),
            binding.postings_bytes,
        ),
        (
            "development-ground-truth",
            binding.truth_uri.as_str(),
            binding.truth_sha256.as_str(),
            binding.truth_blake3.as_str(),
            binding.truth_bytes,
        ),
    ] {
        if !v40_local_identity_matches(
            v40_local_input(request, role)?,
            uri,
            sha256,
            blake3,
            encoded_bytes,
        ) {
            return Err(invalid());
        }
    }
    let relation = read_v40_authenticated_input(request, authenticated, "spill-relation")?;
    let postings = read_v40_authenticated_input(request, authenticated, "spill-postings")?;
    let owners = v38_v40_owner_rows_from_artifacts(
        &relation,
        &postings,
        usize::try_from(binding.corpus_rows).map_err(|_| invalid())?,
        binding.posting_count,
        binding.maximum_rows_per_posting,
    )?;
    let selection_bytes =
        read_v40_authenticated_input(request, authenticated, "accepted-selection")?;
    let selections = decode_v40_accepted_selections_parquet(
        &selection_bytes,
        binding.query_count,
        usize::try_from(binding.selected_postings).map_err(|_| invalid())?,
        &backend,
    )?;
    authenticate_v40_deferred_input(request, authenticated, "development-ground-truth")?;
    let truth_input = v40_local_input(request, "development-ground-truth")?;
    let truth = crate::v37_relation_router::load_v37_feature_ground_truth_file(
        v40_authenticated_input_file(request, authenticated, "development-ground-truth")?,
        &truth_input.path,
        binding.query_count,
    )?;
    let spec = V40EvaluationSpec {
        selected_postings: binding.selected_postings,
        gt_neighbors: binding.gt_neighbors,
        aggregate_gate_ppm: binding.aggregate_gate_ppm,
        minimum_gate_ppm: binding.minimum_gate_ppm,
    };
    let evaluation = evaluate_v40_accepted_recall(&spec, &owners, &selections, &truth, &backend)?;
    let result =
        v40_accepted_evaluation_result_bytes(request, &spec, &evaluation, &selections, &backend)?;
    publish_v40_output(&v40_local_output(request, "accepted-result")?.path, &result)?;
    Ok(result)
}

fn v40_local_identity_matches(
    observed: &V40LocalArtifact,
    uri: &str,
    sha256: &str,
    blake3: &str,
    encoded_bytes: u64,
) -> bool {
    observed.uri == uri
        && observed.sha256 == sha256
        && observed.blake3 == blake3
        && observed.encoded_bytes == encoded_bytes
}

fn run_v40_evaluate_direct(
    request: &V40LocalRunRequest,
    authenticated: &mut V40AuthenticatedLocalInputs,
) -> Result<Vec<u8>> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct evaluation binding differs".to_owned());
    let ceiling_bytes =
        read_v40_authenticated_input(request, authenticated, "v38-ceiling-authority")?;
    let construction_bytes =
        read_v40_authenticated_input(request, authenticated, "v38-construction-result")?;
    let binding = v38_v40_evaluation_binding(&ceiling_bytes, &construction_bytes)?;
    let cohort_input = v40_local_input(request, "cohort-authority")?;
    let cohort_bytes = read_v40_authenticated_input(request, authenticated, "cohort-authority")?;
    let cohort = parse_v40_direct_cohort_authority_bytes(&cohort_bytes)?;
    if cohort.query_count != u64::from(binding.query_count)
        || !v40_cohort_identity_matches_local(
            &cohort.development_ground_truth,
            v40_local_input(request, "development-ground-truth")?,
        )
        || cohort.v37_authority.uri != binding.v37_authority_uri
        || cohort.v37_authority.sha256 != binding.v37_authority_sha256
        || cohort.v37_authority.blake3 != binding.v37_authority_blake3
        || cohort.v37_authority.encoded_bytes != binding.v37_authority_bytes
        || cohort.ownership_tree.uri != binding.ownership_tree_uri
        || cohort.ownership_tree.sha256 != binding.ownership_tree_sha256
        || cohort.ownership_tree.blake3 != binding.ownership_tree_blake3
        || cohort.ownership_tree.encoded_bytes != binding.ownership_tree_bytes
    {
        return Err(invalid());
    }
    for (role, uri, sha256, blake3, encoded_bytes) in [
        (
            "spill-relation",
            binding.relation_uri.as_str(),
            binding.relation_sha256.as_str(),
            binding.relation_blake3.as_str(),
            binding.relation_bytes,
        ),
        (
            "spill-postings",
            binding.postings_uri.as_str(),
            binding.postings_sha256.as_str(),
            binding.postings_blake3.as_str(),
            binding.postings_bytes,
        ),
        (
            "development-ground-truth",
            binding.truth_uri.as_str(),
            binding.truth_sha256.as_str(),
            binding.truth_blake3.as_str(),
            binding.truth_bytes,
        ),
    ] {
        if !v40_local_identity_matches(
            v40_local_input(request, role)?,
            uri,
            sha256,
            blake3,
            encoded_bytes,
        ) {
            return Err(invalid());
        }
    }
    let relation_bytes = read_v40_authenticated_input(request, authenticated, "spill-relation")?;
    let posting_bytes = read_v40_authenticated_input(request, authenticated, "spill-postings")?;
    let owners = v38_v40_owner_rows_from_artifacts(
        &relation_bytes,
        &posting_bytes,
        usize::try_from(binding.corpus_rows).map_err(|_| invalid())?,
        binding.posting_count,
        binding.maximum_rows_per_posting,
    )?;
    let selection = v40_local_input(request, "direct-selection")?;
    let selection_receipt =
        read_v40_authenticated_input(request, authenticated, "direct-selection-result")?;
    let backend =
        parse_v40_selection_receipt_bytes(&selection_receipt, selection, cohort_input, &cohort)?;
    let selection_bytes = read_v40_authenticated_input(request, authenticated, "direct-selection")?;
    let selections = decode_v40_direct_selections_parquet(
        &selection_bytes,
        binding.query_count,
        usize::try_from(binding.selected_postings).map_err(|_| invalid())?,
        &backend,
    )?;
    authenticate_v40_deferred_input(request, authenticated, "development-ground-truth")?;
    let truth_input = v40_local_input(request, "development-ground-truth")?;
    let truth = crate::v37_relation_router::load_v37_feature_ground_truth_file(
        v40_authenticated_input_file(request, authenticated, "development-ground-truth")?,
        &truth_input.path,
        binding.query_count,
    )?;
    let spec = V40EvaluationSpec {
        selected_postings: binding.selected_postings,
        gt_neighbors: binding.gt_neighbors,
        aggregate_gate_ppm: binding.aggregate_gate_ppm,
        minimum_gate_ppm: binding.minimum_gate_ppm,
    };
    let evaluation = evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, &backend)?;
    let result_bytes = v40_evaluation_result_bytes(request, &spec, &evaluation, &backend)?;
    let output = request.outputs.first().ok_or_else(invalid)?;
    publish_v40_output(&output.path, &result_bytes)?;
    Ok(result_bytes)
}

pub(crate) fn evaluate_v40_direct_recall(
    spec: &V40EvaluationSpec,
    owners: &[(u64, u32, Option<u32>)],
    selections: &[V40DirectSelectionRecord],
    truth: &[V37FeatureGroundTruth],
    expected_backend: &str,
) -> Result<V40DirectEvaluation> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct evaluation authority differs".to_owned());
    if spec.selected_postings == 0
        || spec.gt_neighbors == 0
        || spec.aggregate_gate_ppm > 1_000_000
        || spec.minimum_gate_ppm > 1_000_000
        || owners.is_empty()
        || selections.is_empty()
        || selections.len() != truth.len()
        || !matches!(expected_backend, "aarch64-neon-fma" | "x86-avx-fma")
    {
        return Err(invalid());
    }

    let mut ownership = BTreeMap::new();
    let mut known_postings = BTreeSet::new();
    for &(feature_id, primary, alternate) in owners {
        if alternate == Some(primary)
            || ownership.insert(feature_id, (primary, alternate)).is_some()
        {
            return Err(invalid());
        }
        known_postings.insert(primary);
        if let Some(posting) = alternate {
            known_postings.insert(posting);
        }
    }
    if ownership
        .keys()
        .copied()
        .ne(owners.iter().map(|owner| owner.0))
    {
        return Err(invalid());
    }

    let selected_count = usize::try_from(spec.selected_postings).map_err(|_| invalid())?;
    let truth_count = usize::try_from(spec.gt_neighbors).map_err(|_| invalid())?;
    let mut samples = Vec::with_capacity(selections.len());
    let mut total_hits = 0_u64;
    let mut minimum_recall_ppm = 1_000_000_u32;

    for (expected_query, (selection, ground_truth)) in selections.iter().zip(truth).enumerate() {
        let expected_query = u32::try_from(expected_query).map_err(|_| invalid())?;
        let selected: BTreeSet<_> = selection.posting_ordinals.iter().copied().collect();
        let truth_ids: BTreeSet<_> = ground_truth.feature_row_ids.iter().copied().collect();
        if selection.query_ordinal != expected_query
            || ground_truth.query_ordinal != expected_query
            || selection.posting_ordinals.len() != selected_count
            || selected.len() != selected_count
            || !selected.is_subset(&known_postings)
            || selection.node_pops == 0
            || selection.scored_internal_nodes == 0
            || selection.scored_internal_nodes > selection.node_pops
            || selection.fma_backend != expected_backend
            || ground_truth.feature_row_ids.len() != truth_count
            || truth_ids.len() != truth_count
        {
            return Err(invalid());
        }

        let mut hits = 0_u32;
        for feature_id in &ground_truth.feature_row_ids {
            let (primary, alternate) = ownership.get(feature_id).ok_or_else(invalid)?;
            if selected.contains(primary)
                || alternate.is_some_and(|posting| selected.contains(&posting))
            {
                hits = hits.checked_add(1).ok_or_else(invalid)?;
            }
        }
        let recall_ppm = u32::try_from(
            u64::from(hits).checked_mul(1_000_000).ok_or_else(invalid)?
                / u64::from(spec.gt_neighbors),
        )
        .map_err(|_| invalid())?;
        total_hits = total_hits
            .checked_add(u64::from(hits))
            .ok_or_else(invalid)?;
        minimum_recall_ppm = minimum_recall_ppm.min(recall_ppm);
        samples.push(V40DirectSample {
            query_ordinal: expected_query,
            selected_postings: selection.posting_ordinals.clone(),
            node_pops: selection.node_pops,
            scored_internal_nodes: selection.scored_internal_nodes,
            hits,
            recall_ppm,
        });
    }

    let possible_hits = u64::try_from(selections.len())
        .map_err(|_| invalid())?
        .checked_mul(u64::from(spec.gt_neighbors))
        .ok_or_else(invalid)?;
    let aggregate_recall_ppm =
        u32::try_from(total_hits.checked_mul(1_000_000).ok_or_else(invalid)? / possible_hits)
            .map_err(|_| invalid())?;
    let passed = aggregate_recall_ppm >= spec.aggregate_gate_ppm
        && minimum_recall_ppm >= spec.minimum_gate_ppm;
    Ok(V40DirectEvaluation {
        samples,
        total_hits,
        aggregate_recall_ppm,
        minimum_recall_ppm,
        passed,
        disposition: if passed {
            "direct-passed"
        } else {
            "direct-failed"
        }
        .to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, fs::File, path::PathBuf, sync::Arc};

    use super::super::v37_relation_router::{
        V37BalancedNode, V37BalancedTree, score_v37_hyperplane_fused,
    };
    use super::{
        V40AcceptedSelectionRecord, V40DirectSelectionRecord, V40EvaluationSpec, V40LocalArtifact,
        V40LocalOutput, V40LocalRunMode, V40LocalRunRequest, V40PackedSpillSummary,
        authenticate_v40_local_request, authenticate_v40_local_request_deferred,
        build_v40_spill_counts_from_owners, decode_v40_accepted_selections_parquet,
        decode_v40_direct_selections_parquet, decode_v40_packed_spill_summary_arrow,
        encode_v40_accepted_selections_parquet, encode_v40_direct_selections_parquet,
        encode_v40_packed_spill_summary_arrow, evaluate_v40_accepted_recall,
        evaluate_v40_direct_recall, load_v40_projected_queries, load_v40_projected_queries_file,
        parse_v40_direct_cohort_authority_bytes, parse_v40_direct_failure_result_bytes,
        parse_v40_phase_receipt, parse_v40_selection_receipt_bytes,
        select_v40_accepted_spill_postings, select_v40_direct_queries, select_v40_tree_frontier,
        v40_accepted_evaluation_result_bytes, v40_evaluation_result_bytes, v40_phase_receipt_bytes,
    };
    use crate::v35_projection::project_v35_query_simd;
    use crate::v36_funnel_geometry::build_v36_srht192_control;
    use crate::v36_prefix_dataset::{v36_prefix_query_schema, write_v36_prefix_query_parquet};
    use crate::v37_relation_router::V37FeatureGroundTruth;
    use crate::v38_boundary_spill::{
        V38SpillRecord, encode_v38_posting_summary_parquet, encode_v38_spill_relation_parquet,
        summarize_v38_spill_relation,
    };
    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, UInt64Array};
    use arrow_schema::{DataType, Field};
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    fn four_leaf_tree() -> V37BalancedTree {
        let normal = vec![1.0_f32, 0.0, 0.0];
        let leaf = |posting_ordinal| V37BalancedNode {
            normal: Vec::new(),
            boundary_score_bits: 0.0_f32.to_bits(),
            boundary_source_ordinal: 0,
            left_node: None,
            right_node: None,
            posting_ordinal: Some(posting_ordinal),
            population: 1,
        };
        let branch = |population, left_node, right_node| V37BalancedNode {
            normal: normal.clone(),
            boundary_score_bits: 0.0_f32.to_bits(),
            boundary_source_ordinal: 0,
            left_node: Some(left_node),
            right_node: Some(right_node),
            posting_ordinal: None,
            population,
        };
        let backend = score_v37_hyperplane_fused(&[0.0, 0.0, 0.0], &[1.0, 0.0, 0.0])
            .unwrap()
            .1
            .to_owned();
        V37BalancedTree {
            dimensions: 3,
            seed: 40,
            fma_backend: backend,
            nodes: vec![
                branch(4, 1, 4),
                branch(2, 2, 3),
                leaf(0),
                leaf(1),
                branch(2, 5, 6),
                leaf(2),
                leaf(3),
            ],
            leaf_populations: vec![1; 4],
            assignments: Vec::new(),
        }
    }

    #[test]
    fn v40_tree_frontier_matches_exhaustive_order_and_fails_closed() {
        let tree = four_leaf_tree();
        let frontier =
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 4, 7).unwrap();
        assert_eq!(frontier.posting_ordinals, vec![3, 0, 1, 2]);
        assert_eq!(frontier.node_pops, 7);
        assert_eq!(frontier.scored_internal_nodes, 3);
        assert_eq!(frontier.fma_backend, tree.fma_backend);

        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 5, 7).is_err()
        );
        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 4, 6).is_err()
        );
    }

    #[test]
    fn v40_tree_frontier_handles_equal_margins_signed_zero_and_bad_inputs() {
        let tree = four_leaf_tree();
        let frontier =
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[-0.0, 0.0, 0.0], 4, 7).unwrap();
        assert_eq!(frontier.posting_ordinals, vec![0, 1, 2, 3]);

        assert!(select_v40_tree_frontier(&tree, "scalar-control", &[0.0; 3], 4, 7).is_err());
        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[f32::NAN, 0.0, 0.0], 4, 7)
                .is_err()
        );
        assert!(select_v40_tree_frontier(&tree, &tree.fma_backend, &[0.0; 3], 0, 7).is_err());
        assert!(select_v40_tree_frontier(&tree, &tree.fma_backend, &[0.0; 3], 4, 0).is_err());
    }

    #[test]
    fn v40_marginal_selector_chooses_distinct_coverage_and_zero_gain_ties() {
        let frontier = super::V40TreeFrontier {
            posting_ordinals: vec![0, 1],
            node_pops: 5,
            scored_internal_nodes: 3,
            fma_backend: "aarch64-neon-fma".to_owned(),
        };
        let summary = V40PackedSpillSummary {
            offsets: vec![0, 1, 2, 2],
            alternate_postings: vec![2, 2],
            masses_q24: vec![1 << 24, 1 << 24],
            residual_masses_q24: vec![0, 0, 1 << 24],
        };
        let selection = select_v40_accepted_spill_postings(&frontier, &summary, 2).unwrap();
        assert_eq!(selection.posting_ordinals, vec![2, 0]);
        assert_eq!(selection.objective_value, 3 * (1 << 24));
        assert_eq!(selection.candidate_count, 3);
        assert_eq!(selection.marginal_recomputations, 5);
    }

    #[test]
    fn v40_marginal_selector_rejects_summary_and_candidate_shortage() {
        let frontier = super::V40TreeFrontier {
            posting_ordinals: vec![0, 1],
            node_pops: 5,
            scored_internal_nodes: 3,
            fma_backend: "aarch64-neon-fma".to_owned(),
        };
        let summary = V40PackedSpillSummary {
            offsets: vec![0, 1, 2],
            alternate_postings: vec![1, 0],
            masses_q24: vec![8, 8],
            residual_masses_q24: vec![(1 << 24) - 8; 2],
        };
        assert!(select_v40_accepted_spill_postings(&frontier, &summary, 3).is_err());
        let mut drifted = summary.clone();
        drifted.offsets[2] = 1;
        assert!(select_v40_accepted_spill_postings(&frontier, &drifted, 2).is_err());
        let mut drifted = summary;
        drifted.masses_q24[0] = 9;
        assert!(select_v40_accepted_spill_postings(&frontier, &drifted, 2).is_err());
    }

    #[test]
    fn v40_marginal_selector_matches_exhaustive_tiny_reference() {
        let frontier = super::V40TreeFrontier {
            posting_ordinals: vec![0, 1, 2, 3],
            node_pops: 9,
            scored_internal_nodes: 5,
            fma_backend: "aarch64-neon-fma".to_owned(),
        };
        let summary = V40PackedSpillSummary {
            offsets: vec![0, 1, 2, 3, 4, 4, 4, 4, 4],
            alternate_postings: vec![4, 4, 5, 5],
            masses_q24: vec![5, 6, 7, 8],
            residual_masses_q24: vec![
                (1 << 24) - 5,
                (1 << 24) - 6,
                (1 << 24) - 7,
                (1 << 24) - 8,
                1 << 24,
                1 << 24,
                1 << 24,
                1 << 24,
            ],
        };
        let objective = |selected: &BTreeSet<u32>| {
            frontier
                .posting_ordinals
                .iter()
                .enumerate()
                .map(|(rank, &primary)| {
                    let weight = (frontier.posting_ordinals.len() - rank) as u128;
                    let mut covered = u128::from(summary.residual_masses_q24[primary as usize])
                        * u128::from(selected.contains(&primary));
                    let start = summary.offsets[primary as usize] as usize;
                    let end = summary.offsets[primary as usize + 1] as usize;
                    for edge in start..end {
                        covered += u128::from(summary.masses_q24[edge])
                            * u128::from(
                                selected.contains(&primary)
                                    || selected.contains(&summary.alternate_postings[edge]),
                            );
                    }
                    weight * covered
                })
                .sum::<u128>()
        };
        let candidates = [0_u32, 1, 2, 3, 4, 5];
        let mut selected = BTreeSet::new();
        let mut expected = Vec::new();
        while expected.len() < 4 {
            let before = objective(&selected);
            let candidate = candidates
                .iter()
                .copied()
                .filter(|candidate| !selected.contains(candidate))
                .max_by_key(|candidate| {
                    let mut with_candidate = selected.clone();
                    with_candidate.insert(*candidate);
                    let rank = frontier
                        .posting_ordinals
                        .iter()
                        .position(|posting| posting == candidate)
                        .unwrap_or(usize::MAX);
                    (
                        objective(&with_candidate) - before,
                        std::cmp::Reverse(rank),
                        std::cmp::Reverse(*candidate),
                    )
                })
                .unwrap();
            selected.insert(candidate);
            expected.push(candidate);
        }
        let actual = select_v40_accepted_spill_postings(&frontier, &summary, 4).unwrap();
        assert_eq!(actual.posting_ordinals, expected);
        assert_eq!(actual.objective_value, objective(&selected));
    }

    #[test]
    fn v40_spill_summary_arrow_is_strict_and_round_trips() {
        let summary = V40PackedSpillSummary {
            offsets: vec![0, 2, 3, 3],
            alternate_postings: vec![1, 2, 0],
            masses_q24: vec![8, 7, 1 << 23],
            residual_masses_q24: vec![(1 << 24) - 15, 1 << 23, 1 << 24],
        };
        let bytes = encode_v40_packed_spill_summary_arrow(&summary).unwrap();
        assert_eq!(
            decode_v40_packed_spill_summary_arrow(&bytes, 3).unwrap(),
            summary
        );

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_v40_packed_spill_summary_arrow(&trailing, 3).is_err());
        let footer_length =
            u32::from_le_bytes(bytes[bytes.len() - 10..bytes.len() - 6].try_into().unwrap())
                as usize;
        let footer = bytes[bytes.len() - footer_length - 10..].to_vec();
        let mut copied_footer = bytes.clone();
        copied_footer.extend_from_slice(b"hidden-arrow-payload");
        copied_footer.extend_from_slice(&footer);
        assert!(decode_v40_packed_spill_summary_arrow(&copied_footer, 3).is_err());
        assert!(decode_v40_packed_spill_summary_arrow(&bytes, 2).is_err());

        let mut malformed = summary.clone();
        malformed.offsets[2] = 2;
        assert!(encode_v40_packed_spill_summary_arrow(&malformed).is_err());
        let mut malformed = summary;
        malformed.residual_masses_q24[0] += 1;
        assert!(encode_v40_packed_spill_summary_arrow(&malformed).is_err());
    }

    fn direct_evaluation_fixture() -> (
        V40EvaluationSpec,
        Vec<(u64, u32, Option<u32>)>,
        Vec<V40DirectSelectionRecord>,
        Vec<V37FeatureGroundTruth>,
    ) {
        let spec = V40EvaluationSpec {
            selected_postings: 2,
            gt_neighbors: 4,
            aggregate_gate_ppm: 750_000,
            minimum_gate_ppm: 750_000,
        };
        let owners = vec![
            (10, 0, Some(1)),
            (11, 2, Some(1)),
            (12, 3, Some(0)),
            (13, 4, None),
            (20, 2, Some(0)),
            (21, 3, None),
            (22, 4, Some(5)),
            (23, 0, None),
        ];
        let selections = vec![
            V40DirectSelectionRecord {
                query_ordinal: 0,
                posting_ordinals: vec![0, 1],
                node_pops: 5,
                scored_internal_nodes: 3,
                fma_backend: "aarch64-neon-fma".to_owned(),
            },
            V40DirectSelectionRecord {
                query_ordinal: 1,
                posting_ordinals: vec![0, 3],
                node_pops: 6,
                scored_internal_nodes: 4,
                fma_backend: "aarch64-neon-fma".to_owned(),
            },
        ];
        let truth = vec![
            V37FeatureGroundTruth {
                query_ordinal: 0,
                feature_row_ids: vec![10, 11, 12, 13],
            },
            V37FeatureGroundTruth {
                query_ordinal: 1,
                feature_row_ids: vec![20, 21, 22, 23],
            },
        ];
        (spec, owners, selections, truth)
    }

    #[test]
    fn v40_direct_evaluation_counts_two_owner_hits_once_and_enforces_gates() {
        let (spec, owners, selections, truth) = direct_evaluation_fixture();
        let result =
            evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, "aarch64-neon-fma")
                .unwrap();
        assert_eq!(result.total_hits, 6);
        assert_eq!(result.aggregate_recall_ppm, 750_000);
        assert_eq!(result.minimum_recall_ppm, 750_000);
        assert_eq!(result.samples[0].hits, 3);
        assert_eq!(result.samples[0].recall_ppm, 750_000);
        assert_eq!(result.samples[1].hits, 3);
        assert!(result.passed);
        assert_eq!(result.disposition, "direct-passed");
    }

    #[test]
    fn v40_direct_evaluation_rejects_selection_truth_and_backend_drift() {
        let (spec, owners, selections, truth) = direct_evaluation_fixture();

        let mut duplicated = selections.clone();
        duplicated[0].posting_ordinals = vec![0, 0];
        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &duplicated, &truth, "aarch64-neon-fma")
                .is_err()
        );

        let mut skipped_query = selections.clone();
        skipped_query[1].query_ordinal = 2;
        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &skipped_query, &truth, "aarch64-neon-fma")
                .is_err()
        );

        let mut unknown_truth = truth.clone();
        unknown_truth[0].feature_row_ids[0] = 99;
        assert!(
            evaluate_v40_direct_recall(
                &spec,
                &owners,
                &selections,
                &unknown_truth,
                "aarch64-neon-fma"
            )
            .is_err()
        );

        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, "x86-avx-fma").is_err()
        );
    }

    #[test]
    fn v40_direct_evaluation_receipt_recomputes_quality_and_binds_inputs() {
        let (spec, owners, selections, truth) = direct_evaluation_fixture();
        let evaluation =
            evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, "aarch64-neon-fma")
                .unwrap();
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::EvaluateDirect,
            [
                "cohort-authority",
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "development-ground-truth",
                "direct-selection-result",
                "direct-selection",
            ]
            .map(local_artifact)
            .to_vec(),
            vec![local_output("direct-result")],
            4,
        )
        .unwrap();
        let bytes =
            v40_evaluation_result_bytes(&request, &spec, &evaluation, "aarch64-neon-fma").unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["mode"], "evaluate-direct");
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["evidence"]["aggregate_recall_ppm"], 750_000);
        assert_eq!(value["evidence"]["minimum_recall_ppm"], 750_000);
        assert_eq!(value["evidence"]["passed"], true);
        assert_eq!(value["inputs"].as_array().unwrap().len(), 8);

        let mut drifted = evaluation;
        drifted.aggregate_recall_ppm -= 1;
        assert!(
            v40_evaluation_result_bytes(&request, &spec, &drifted, "aarch64-neon-fma").is_err()
        );
    }

    #[test]
    fn v40_challenger_selection_codec_and_evaluation_recompute_every_claim() {
        let (spec, owners, direct, truth) = direct_evaluation_fixture();
        let accepted = direct
            .iter()
            .enumerate()
            .map(|(query, record)| V40AcceptedSelectionRecord {
                query_ordinal: record.query_ordinal,
                posting_ordinals: record.posting_ordinals.clone(),
                node_pops: record.node_pops,
                scored_internal_nodes: record.scored_internal_nodes,
                fma_backend: record.fma_backend.clone(),
                objective_value: 100 + query as u64,
                candidate_count: 7,
                marginal_recomputations: 13,
            })
            .collect::<Vec<_>>();
        let bytes = encode_v40_accepted_selections_parquet(&accepted, 2).unwrap();
        assert_eq!(
            decode_v40_accepted_selections_parquet(&bytes, 2, 2, "aarch64-neon-fma").unwrap(),
            accepted
        );
        let evaluation =
            evaluate_v40_accepted_recall(&spec, &owners, &accepted, &truth, "aarch64-neon-fma")
                .unwrap();
        assert!(evaluation.passed);
        assert_eq!(evaluation.total_hits, 6);
        assert_eq!(evaluation.disposition, "challenger-passed");
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::EvaluateAcceptedSpill,
            [
                "cohort-authority",
                "direct-result",
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "development-ground-truth",
                "spill-summary-result",
                "spill-summary",
                "accepted-selection-result",
                "accepted-selection",
            ]
            .map(local_artifact)
            .to_vec(),
            vec![local_output("accepted-result")],
            1,
        )
        .unwrap();
        let result_bytes = v40_accepted_evaluation_result_bytes(
            &request,
            &spec,
            &evaluation,
            &accepted,
            "aarch64-neon-fma",
        )
        .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&result_bytes).unwrap();
        assert_eq!(result["mode"], "evaluate-accepted-spill");
        assert_eq!(result["evidence"]["disposition"], "challenger-passed");
        assert_eq!(result["evidence"]["samples"][0]["objective_value"], 100);
        assert_eq!(result["evidence"]["samples"][0]["candidate_count"], 7);
        assert_eq!(
            result["evidence"]["samples"][0]["marginal_recomputations"],
            13
        );

        let mut drifted = accepted.clone();
        drifted[0].objective_value = 0;
        assert!(encode_v40_accepted_selections_parquet(&drifted, 2).is_err());
        let mut drifted = accepted;
        drifted[1].candidate_count = 1;
        assert!(
            evaluate_v40_accepted_recall(&spec, &owners, &drifted, &truth, "aarch64-neon-fma")
                .is_err()
        );
    }

    #[test]
    fn v40_challenger_requires_authenticated_direct_failure_predecessor() {
        let (mut spec, owners, selections, truth) = direct_evaluation_fixture();
        spec.aggregate_gate_ppm = 800_000;
        spec.minimum_gate_ppm = 800_000;
        let evaluation =
            evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, "aarch64-neon-fma")
                .unwrap();
        assert_eq!(evaluation.disposition, "direct-failed");
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::EvaluateDirect,
            [
                "cohort-authority",
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "development-ground-truth",
                "direct-selection-result",
                "direct-selection",
            ]
            .map(local_artifact)
            .to_vec(),
            vec![local_output("direct-result")],
            1,
        )
        .unwrap();
        let bytes =
            v40_evaluation_result_bytes(&request, &spec, &evaluation, "aarch64-neon-fma").unwrap();
        let prerequisite = parse_v40_direct_failure_result_bytes(&bytes).unwrap();
        assert_eq!(prerequisite.aggregate_recall_ppm, 750_000);
        assert_eq!(prerequisite.minimum_recall_ppm, 750_000);
        assert_eq!(prerequisite.fma_backend, "aarch64-neon-fma");

        let mut passing: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        passing["evidence"]["disposition"] = serde_json::json!("direct-passed");
        passing["evidence"]["passed"] = serde_json::json!(true);
        let mut passing = serde_json::to_vec(&super::v40_canonical_json(passing)).unwrap();
        passing.push(b'\n');
        assert!(parse_v40_direct_failure_result_bytes(&passing).is_err());

        let mut empty: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        empty["evidence"]["query_count"] = serde_json::json!(0);
        empty["evidence"]["samples"] = serde_json::json!([]);
        empty["evidence"]["total_hits"] = serde_json::json!(0);
        let mut empty = serde_json::to_vec(&super::v40_canonical_json(empty)).unwrap();
        empty.push(b'\n');
        assert!(parse_v40_direct_failure_result_bytes(&empty).is_err());
    }

    #[test]
    fn v40_challenger_spill_owner_counts_are_complete_and_deterministic() {
        let owners = vec![
            (10, 0, Some(1)),
            (11, 0, Some(1)),
            (12, 0, None),
            (13, 1, Some(0)),
            (14, 1, None),
        ];
        let counts = build_v40_spill_counts_from_owners(&owners, 2).unwrap();
        assert_eq!(counts.len(), 4);
        assert_eq!(counts[0].primary_posting, 0);
        assert_eq!(counts[0].alternate_posting, Some(1));
        assert_eq!(counts[0].count, 2);
        assert_eq!(counts[1].alternate_posting, None);
        assert_eq!(counts[1].count, 1);
        assert_eq!(counts[2].primary_posting, 1);
        assert_eq!(counts[2].alternate_posting, Some(0));
        assert_eq!(counts[3].alternate_posting, None);

        let mut unordered = owners;
        unordered.swap(0, 1);
        assert!(build_v40_spill_counts_from_owners(&unordered, 2).is_err());
    }

    #[test]
    fn v40_challenger_spill_counts_stream_relation_without_row_materialization() {
        let records = vec![
            V38SpillRecord {
                source_ordinal: 0,
                feature_row_id: 10,
                posting_ordinal: 0,
                owner_role: 0,
                posting_local_ordinal: 0,
                alternate_violation_bits: None,
            },
            V38SpillRecord {
                source_ordinal: 0,
                feature_row_id: 10,
                posting_ordinal: 1,
                owner_role: 1,
                posting_local_ordinal: 2,
                alternate_violation_bits: Some(0.25_f32.to_bits()),
            },
            V38SpillRecord {
                source_ordinal: 1,
                feature_row_id: 11,
                posting_ordinal: 1,
                owner_role: 0,
                posting_local_ordinal: 0,
                alternate_violation_bits: None,
            },
            V38SpillRecord {
                source_ordinal: 1,
                feature_row_id: 11,
                posting_ordinal: 0,
                owner_role: 1,
                posting_local_ordinal: 2,
                alternate_violation_bits: Some(0.5_f32.to_bits()),
            },
            V38SpillRecord {
                source_ordinal: 2,
                feature_row_id: 12,
                posting_ordinal: 0,
                owner_role: 0,
                posting_local_ordinal: 1,
                alternate_violation_bits: None,
            },
            V38SpillRecord {
                source_ordinal: 3,
                feature_row_id: 13,
                posting_ordinal: 1,
                owner_role: 0,
                posting_local_ordinal: 1,
                alternate_violation_bits: None,
            },
        ];
        let summaries = summarize_v38_spill_relation(&records, 2, 3).unwrap();
        let relation_bytes = encode_v38_spill_relation_parquet(&records).unwrap();
        let posting_bytes = encode_v38_posting_summary_parquet(&summaries).unwrap();
        let root = tempdir().unwrap();
        let relation_path = root.path().join("relation.parquet");
        let posting_path = root.path().join("postings.parquet");
        fs::write(&relation_path, relation_bytes).unwrap();
        fs::write(&posting_path, posting_bytes).unwrap();
        let counts = super::build_v40_spill_counts_from_parquet_files(
            File::open(relation_path).unwrap(),
            File::open(posting_path).unwrap(),
            4,
            2,
            3,
        )
        .unwrap();
        assert_eq!(counts.len(), 4);
        assert_eq!(counts[0].alternate_posting, Some(1));
        assert_eq!(counts[0].count, 1);
        assert_eq!(counts[1].alternate_posting, None);
        assert_eq!(counts[1].count, 1);
        assert_eq!(counts[2].alternate_posting, Some(0));
        assert_eq!(counts[3].alternate_posting, None);
    }

    #[test]
    fn v40_challenger_phase_receipt_binds_inputs_outputs_and_mode() {
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::BuildSpillSummary,
            [
                "cohort-authority",
                "direct-result",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
            ]
            .map(local_artifact)
            .to_vec(),
            ["spill-counts", "spill-summary", "spill-summary-result"]
                .map(local_output)
                .to_vec(),
            1,
        )
        .unwrap();
        let bytes = v40_phase_receipt_bytes(
            &request,
            "build-spill-summary",
            &[
                (&request.outputs[0], b"counts".as_slice()),
                (&request.outputs[1], b"summary".as_slice()),
            ],
            serde_json::json!({"posting_count": 2}),
        )
        .unwrap();
        let receipt = parse_v40_phase_receipt(&bytes, "build-spill-summary").unwrap();
        assert_eq!(receipt.inputs.len(), 5);
        assert_eq!(receipt.artifacts.len(), 2);
        assert_eq!(receipt.artifacts[0].role, "spill-counts");
        assert!(parse_v40_phase_receipt(&bytes, "select-accepted-spill").is_err());

        let mut drifted: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        drifted["artifacts"][0]["encoded_bytes"] = serde_json::json!(0);
        let mut drifted = serde_json::to_vec(&super::v40_canonical_json(drifted)).unwrap();
        drifted.push(b'\n');
        assert!(parse_v40_phase_receipt(&drifted, "build-spill-summary").is_err());
    }

    fn local_artifact(role: &str) -> V40LocalArtifact {
        V40LocalArtifact::try_new(
            role.to_owned(),
            PathBuf::from(format!("/tmp/v40-{role}")),
            format!("s3://fixture/v40/{role}"),
            "1".repeat(64),
            "2".repeat(64),
            17,
        )
        .unwrap()
    }

    fn local_output(role: &str) -> V40LocalOutput {
        V40LocalOutput::try_new(role.to_owned(), PathBuf::from(format!("/tmp/v40-{role}"))).unwrap()
    }

    fn cohort_authority_bytes() -> Vec<u8> {
        let identity = |role: &str| {
            serde_json::json!({
                "blake3": "2".repeat(64),
                "encoded_bytes": 17,
                "role": role,
                "sha256": "1".repeat(64),
                "uri": format!("s3://fixture/v40/{role}"),
            })
        };
        let value = serde_json::json!({
            "development_ground_truth": identity("development-ground-truth"),
            "development_query": identity("development-query"),
            "ownership_tree": identity("ownership-tree"),
            "query_count": 1_000,
            "schema": "borsuk-v40-direct-cohort-authority-v1",
            "v37_authority": identity("v37-authority"),
        });
        let mut bytes = serde_json::to_vec(&super::v40_canonical_json(value)).unwrap();
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn v40_direct_cohort_authority_binds_router_query_and_truth_identities() {
        let bytes = cohort_authority_bytes();
        let authority = parse_v40_direct_cohort_authority_bytes(&bytes).unwrap();
        assert_eq!(authority.query_count, 1_000);
        assert_eq!(authority.v37_authority.role, "v37-authority");
        assert_eq!(authority.ownership_tree.role, "ownership-tree");
        assert_eq!(authority.development_query.role, "development-query");
        assert_eq!(
            authority.development_ground_truth.role,
            "development-ground-truth"
        );

        let mut drifted: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        drifted["development_query"]["sha256"] = serde_json::json!("a".repeat(63));
        let mut drifted = serde_json::to_vec(&super::v40_canonical_json(drifted)).unwrap();
        drifted.push(b'\n');
        assert!(parse_v40_direct_cohort_authority_bytes(&drifted).is_err());
    }

    #[test]
    fn v40_authority_direct_modes_separate_query_and_truth_capabilities() {
        let selection_inputs = [
            "cohort-authority",
            "v37-authority",
            "ownership-tree",
            "development-query",
        ]
        .map(local_artifact)
        .to_vec();
        let selection = V40LocalRunRequest::try_new(
            V40LocalRunMode::SelectDirect,
            selection_inputs,
            vec![local_output("direct-selection")],
            16,
        )
        .unwrap();
        assert_eq!(
            selection.input_roles(),
            vec![
                "cohort-authority",
                "v37-authority",
                "ownership-tree",
                "development-query",
            ]
        );
        assert_eq!(selection.output_roles(), vec!["direct-selection"]);

        let evaluation_inputs = [
            "cohort-authority",
            "v38-ceiling-authority",
            "v38-construction-result",
            "spill-relation",
            "spill-postings",
            "development-ground-truth",
            "direct-selection-result",
            "direct-selection",
        ]
        .map(local_artifact)
        .to_vec();
        let evaluation = V40LocalRunRequest::try_new(
            V40LocalRunMode::EvaluateDirect,
            evaluation_inputs,
            vec![local_output("direct-result")],
            1,
        )
        .unwrap();
        assert_eq!(evaluation.output_roles(), vec!["direct-result"]);
        assert!(!evaluation.input_roles().contains(&"development-query"));
        assert!(
            !selection
                .input_roles()
                .contains(&"development-ground-truth")
        );
    }

    #[test]
    fn v40_authority_challenger_modes_separate_build_query_and_truth_capabilities() {
        let build = V40LocalRunRequest::try_new(
            V40LocalRunMode::BuildSpillSummary,
            [
                "cohort-authority",
                "direct-result",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
            ]
            .map(local_artifact)
            .to_vec(),
            ["spill-counts", "spill-summary", "spill-summary-result"]
                .map(local_output)
                .to_vec(),
            1,
        )
        .unwrap();
        assert!(!build.input_roles().contains(&"development-query"));
        assert!(!build.input_roles().contains(&"development-ground-truth"));

        let selection = V40LocalRunRequest::try_new(
            V40LocalRunMode::SelectAcceptedSpill,
            [
                "cohort-authority",
                "direct-result",
                "v37-authority",
                "ownership-tree",
                "development-query",
                "spill-summary-result",
                "spill-summary",
            ]
            .map(local_artifact)
            .to_vec(),
            ["accepted-selection", "accepted-selection-result"]
                .map(local_output)
                .to_vec(),
            16,
        )
        .unwrap();
        assert!(
            !selection
                .input_roles()
                .contains(&"development-ground-truth")
        );

        let evaluation = V40LocalRunRequest::try_new(
            V40LocalRunMode::EvaluateAcceptedSpill,
            [
                "cohort-authority",
                "direct-result",
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "development-ground-truth",
                "spill-summary-result",
                "spill-summary",
                "accepted-selection-result",
                "accepted-selection",
            ]
            .map(local_artifact)
            .to_vec(),
            vec![local_output("accepted-result")],
            1,
        )
        .unwrap();
        assert!(!evaluation.input_roles().contains(&"development-query"));

        for (mode, mut inputs, outputs, workers) in [
            (
                V40LocalRunMode::BuildSpillSummary,
                build.inputs.clone(),
                build.outputs.clone(),
                1,
            ),
            (
                V40LocalRunMode::SelectAcceptedSpill,
                selection.inputs.clone(),
                selection.outputs.clone(),
                16,
            ),
            (
                V40LocalRunMode::EvaluateAcceptedSpill,
                evaluation.inputs.clone(),
                evaluation.outputs.clone(),
                1,
            ),
        ] {
            inputs.swap(0, 1);
            assert!(V40LocalRunRequest::try_new(mode, inputs, outputs, workers).is_err());
        }
    }

    #[test]
    fn v40_authority_direct_modes_reject_identity_role_and_path_drift() {
        assert!(
            V40LocalArtifact::try_new(
                "ownership-tree".to_owned(),
                PathBuf::from("/tmp/tree"),
                "file:///tmp/tree".to_owned(),
                "1".repeat(64),
                "2".repeat(64),
                17,
            )
            .is_err()
        );
        assert!(
            V40LocalArtifact::try_new(
                "ownership-tree".to_owned(),
                PathBuf::from("/tmp/tree"),
                "s3://fixture/tree".to_owned(),
                "1".repeat(63),
                "2".repeat(64),
                17,
            )
            .is_err()
        );

        let mut inputs = [
            "cohort-authority",
            "v37-authority",
            "ownership-tree",
            "development-query",
        ]
        .map(local_artifact)
        .to_vec();
        inputs.swap(0, 1);
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                inputs,
                vec![local_output("direct-selection")],
                16,
            )
            .is_err()
        );

        let mut overlap = [
            "cohort-authority",
            "v37-authority",
            "ownership-tree",
            "development-query",
        ]
        .map(local_artifact)
        .to_vec();
        overlap[1] = overlap[0].clone();
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                overlap,
                vec![local_output("direct-selection")],
                16,
            )
            .is_err()
        );

        let inputs = [
            "cohort-authority",
            "v37-authority",
            "ownership-tree",
            "development-query",
        ]
        .map(local_artifact)
        .to_vec();
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                inputs,
                vec![
                    V40LocalOutput::try_new(
                        "direct-selection".to_owned(),
                        PathBuf::from("/tmp/v40-ownership-tree"),
                    )
                    .unwrap()
                ],
                3,
            )
            .is_err()
        );
    }

    fn local_artifact_bytes(root: &std::path::Path, role: &str, bytes: &[u8]) -> V40LocalArtifact {
        let path = root.join(role);
        fs::write(&path, bytes).unwrap();
        V40LocalArtifact::try_new(
            role.to_owned(),
            path,
            format!("s3://fixture/v40/{role}"),
            format!("{:x}", Sha256::digest(bytes)),
            blake3::hash(bytes).to_hex().to_string(),
            bytes.len() as u64,
        )
        .unwrap()
    }

    #[test]
    fn v40_direct_artifact_authentication_rejects_byte_and_output_drift() {
        let root = tempdir().unwrap();
        let inputs = [
            ("cohort-authority", b"cohort".as_slice()),
            ("v37-authority", b"authority".as_slice()),
            ("ownership-tree", b"tree".as_slice()),
            ("development-query", b"query".as_slice()),
        ]
        .map(|(role, bytes)| local_artifact_bytes(root.path(), role, bytes))
        .to_vec();
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::SelectDirect,
            inputs,
            vec![
                V40LocalOutput::try_new(
                    "direct-selection".to_owned(),
                    root.path().join("selection.parquet"),
                )
                .unwrap(),
            ],
            16,
        )
        .unwrap();
        assert!(authenticate_v40_local_request(&request).is_ok());

        fs::write(root.path().join("ownership-tree"), b"drift").unwrap();
        assert!(authenticate_v40_local_request(&request).is_err());

        fs::write(root.path().join("ownership-tree"), b"tree").unwrap();
        fs::write(root.path().join("selection.parquet"), b"occupied").unwrap();
        assert!(authenticate_v40_local_request(&request).is_err());
    }

    #[test]
    fn v40_challenger_evaluation_defers_ground_truth_authentication() {
        let root = tempdir().unwrap();
        let roles = V40LocalRunMode::EvaluateAcceptedSpill.input_roles();
        let inputs = roles
            .iter()
            .map(|role| {
                if *role == "development-ground-truth" {
                    V40LocalArtifact::try_new(
                        (*role).to_owned(),
                        root.path().join(role),
                        format!("s3://fixture/v40/{role}"),
                        "1".repeat(64),
                        "2".repeat(64),
                        17,
                    )
                    .unwrap()
                } else {
                    local_artifact_bytes(root.path(), role, role.as_bytes())
                }
            })
            .collect::<Vec<_>>();
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::EvaluateAcceptedSpill,
            inputs,
            vec![
                V40LocalOutput::try_new(
                    "accepted-result".to_owned(),
                    root.path().join("accepted-result"),
                )
                .unwrap(),
            ],
            1,
        )
        .unwrap();
        assert!(
            authenticate_v40_local_request_deferred(&request, Some("development-ground-truth"))
                .is_ok()
        );
        assert!(authenticate_v40_local_request(&request).is_err());
    }

    #[test]
    fn v40_direct_artifact_selection_parquet_round_trips_and_fails_closed() {
        let (_, _, selections, _) = direct_evaluation_fixture();
        let bytes = encode_v40_direct_selections_parquet(&selections, 2).unwrap();
        assert_eq!(
            decode_v40_direct_selections_parquet(&bytes, 2, 2, "aarch64-neon-fma").unwrap(),
            selections
        );
        assert!(decode_v40_direct_selections_parquet(&bytes, 2, 3, "aarch64-neon-fma").is_err());
        assert!(
            decode_v40_direct_selections_parquet(
                &bytes[..bytes.len() - 1],
                2,
                2,
                "aarch64-neon-fma"
            )
            .is_err()
        );

        let mut reordered = selections.clone();
        reordered.swap(0, 1);
        assert!(encode_v40_direct_selections_parquet(&reordered, 2).is_err());
        let mut backend_drift = selections;
        backend_drift[1].fma_backend = "x86-avx-fma".to_owned();
        assert!(encode_v40_direct_selections_parquet(&backend_drift, 2).is_err());
    }

    #[test]
    fn v40_direct_selection_receipt_binds_artifact_and_backend() {
        let root = tempdir().unwrap();
        let selections = (0..1_000_u32)
            .map(|query_ordinal| V40DirectSelectionRecord {
                query_ordinal,
                posting_ordinals: (0..21_u32).collect(),
                node_pops: 41,
                scored_internal_nodes: 20,
                fma_backend: "aarch64-neon-fma".to_owned(),
            })
            .collect::<Vec<_>>();
        let selection_bytes = encode_v40_direct_selections_parquet(&selections, 21).unwrap();
        let output_path = root.path().join("selection.parquet");
        let request = V40LocalRunRequest::try_new(
            V40LocalRunMode::SelectDirect,
            [
                "cohort-authority",
                "v37-authority",
                "ownership-tree",
                "development-query",
            ]
            .map(local_artifact)
            .to_vec(),
            vec![
                V40LocalOutput::try_new("direct-selection".to_owned(), output_path.clone())
                    .unwrap(),
            ],
            4,
        )
        .unwrap();
        let receipt =
            super::v40_selection_receipt_bytes(&request, &selections, &selection_bytes).unwrap();
        let selection = V40LocalArtifact::try_new(
            "direct-selection".to_owned(),
            output_path,
            "s3://fixture/v40/direct-selection".to_owned(),
            format!("{:x}", Sha256::digest(&selection_bytes)),
            blake3::hash(&selection_bytes).to_hex().to_string(),
            u64::try_from(selection_bytes.len()).unwrap(),
        )
        .unwrap();
        let cohort = parse_v40_direct_cohort_authority_bytes(&cohort_authority_bytes()).unwrap();

        assert_eq!(
            parse_v40_selection_receipt_bytes(&receipt, &selection, &request.inputs[0], &cohort,)
                .unwrap(),
            "aarch64-neon-fma"
        );
        assert!(
            parse_v40_selection_receipt_bytes(
                &receipt[..receipt.len() - 1],
                &selection,
                &request.inputs[0],
                &cohort,
            )
            .is_err()
        );

        let mut drifted: serde_json::Value = serde_json::from_slice(&receipt).unwrap();
        drifted["inputs"][3]["sha256"] = serde_json::json!("a".repeat(64));
        let mut drifted = serde_json::to_vec(&super::v40_canonical_json(drifted)).unwrap();
        drifted.push(b'\n');
        assert!(
            parse_v40_selection_receipt_bytes(&drifted, &selection, &request.inputs[0], &cohort,)
                .is_err()
        );
    }

    #[test]
    fn v40_direct_query_loader_reuses_frozen_srht_projection() {
        let root = tempdir().unwrap();
        let path = root.path().join("queries.parquet");
        let first = (0..768)
            .map(|index| (index as f32 + 1.0) / 1_024.0)
            .collect::<Vec<_>>();
        let second = first.iter().map(|value| -*value).collect::<Vec<_>>();
        let values = first.iter().chain(&second).copied().collect::<Vec<_>>();
        let embeddings = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            768,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(v36_prefix_query_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1])),
                Arc::new(UInt64Array::from(vec![10, 11])),
                Arc::new(embeddings),
            ],
        )
        .unwrap();
        write_v36_prefix_query_parquet(&path, [batch]).unwrap();

        let observed = load_v40_projected_queries(&path, 2).unwrap();
        let projection = build_v36_srht192_control().unwrap();
        let expected = [first, second]
            .iter()
            .map(|query| {
                project_v35_query_simd(&projection, query)
                    .unwrap()
                    .coordinates()
                    .iter()
                    .map(|value| {
                        let value = *value as f32;
                        if value == 0.0 { 0.0 } else { value }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(observed, expected);
        assert!(load_v40_projected_queries(&path, 1).is_err());
    }

    #[test]
    fn v40_direct_query_loader_consumes_the_authenticated_open_inode() {
        let root = tempdir().unwrap();
        let path = root.path().join("queries.parquet");
        let values = (0..2 * 768)
            .map(|index| (index as f32 + 1.0) / 2_048.0)
            .collect::<Vec<_>>();
        let embeddings = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            768,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(v36_prefix_query_schema()),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1])),
                Arc::new(UInt64Array::from(vec![10, 11])),
                Arc::new(embeddings),
            ],
        )
        .unwrap();
        write_v36_prefix_query_parquet(&path, [batch]).unwrap();
        let file = File::open(&path).unwrap();
        fs::remove_file(&path).unwrap();

        let observed = load_v40_projected_queries_file(file, &path, 2).unwrap();
        assert_eq!(observed.len(), 2);
    }

    #[test]
    fn v40_direct_query_selector_seals_backend_and_work_evidence() {
        let tree = four_leaf_tree();
        let selections = select_v40_direct_queries(
            &tree,
            &tree.fma_backend,
            &[vec![1.0, 0.0, 0.0], vec![-1.0, 0.0, 0.0]],
            2,
            7,
        )
        .unwrap();
        assert_eq!(selections.len(), 2);
        assert_eq!(selections[0].query_ordinal, 0);
        assert_eq!(selections[0].posting_ordinals, vec![3, 0]);
        assert_eq!(selections[1].query_ordinal, 1);
        assert_eq!(selections[1].posting_ordinals, vec![0, 1]);
        assert!(
            selections
                .iter()
                .all(|selection| selection.fma_backend == tree.fma_backend)
        );
        assert!(select_v40_direct_queries(&tree, &tree.fma_backend, &[], 2, 7).is_err());
    }
}
