//! Adaptive Arrow/Parquet lexical block construction.

use std::collections::BTreeMap;

use crate::{
    BorsukError, Result,
    format::{
        bm25_posting_blocks_to_parquet, lexical_row_metadata_blocks_to_parquet,
        sparse_posting_blocks_to_parquet_typed,
    },
    lexical_root::{
        Bm25Posting, LexicalKind, LexicalRowMetadata, LexicalRunRef, LexicalTermBlock,
        LexicalTermPage, SparsePosting, bm25_postings_checksum, row_metadata_checksum,
        sparse_postings_checksum,
    },
    record::VectorElementType,
};

/// Target decoded working-set contribution of one postings+metadata row block.
/// The builder uses actual ids and non-zero counts, so dimensions and data
/// density affect rows per block without dataset-specific presets.
pub(crate) const DEFAULT_LEXICAL_BLOCK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct LexicalInputRow {
    pub record_id: Vec<u8>,
    pub generation: u64,
    pub mutation_stamp: Option<crate::mutation::MutationStamp>,
    /// Sorted unique `(term, value)` pairs. BM25 values are integral TFs.
    pub terms: Vec<(u32, f32)>,
    /// Positive for BM25, zero for sparse.
    pub document_length: u32,
}

#[derive(Debug)]
pub(crate) struct LexicalObject {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct LexicalSegmentBuild {
    pub kind: LexicalKind,
    pub dimensions: u32,
    pub document_count: u64,
    pub total_document_length: u64,
    /// Segment-local DFs; global root publication replaces them with sums
    /// across active shards.
    pub term_page: LexicalTermPage,
    pub objects: Vec<LexicalObject>,
}

pub(crate) fn build_lexical_segment(
    kind: LexicalKind,
    value_type: VectorElementType,
    dimensions: u32,
    field_name: &str,
    segment_key: &str,
    rows: &[LexicalInputRow],
    target_decoded_bytes: usize,
) -> Result<LexicalSegmentBuild> {
    validate_inputs(
        kind,
        dimensions,
        field_name,
        segment_key,
        rows,
        target_decoded_bytes,
    )?;
    if rows.is_empty() {
        return Ok(LexicalSegmentBuild {
            kind,
            dimensions,
            document_count: 0,
            total_document_length: 0,
            term_page: LexicalTermPage {
                kind,
                entries: Vec::new(),
            },
            objects: Vec::new(),
        });
    }

    let mut ranges = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let mut end = start;
        let mut estimated = 0_usize;
        while end < rows.len() {
            let row_bytes = estimated_row_bytes(&rows[end]);
            if end > start && estimated.saturating_add(row_bytes) > target_decoded_bytes {
                break;
            }
            estimated = estimated.saturating_add(row_bytes);
            end += 1;
        }
        ranges.push(start..end);
        start = end;
    }

    let mut bm25_blocks = Vec::<(Vec<Bm25Posting>, u32)>::new();
    let mut sparse_blocks = Vec::<(Vec<SparsePosting>, u32)>::new();
    let mut metadata_blocks = Vec::<Vec<LexicalRowMetadata>>::new();
    let mut block_stats = Vec::with_capacity(ranges.len());
    let mut segment_df = BTreeMap::<u32, u64>::new();
    for row in rows {
        for &(term, _) in &row.terms {
            *segment_df.entry(term).or_default() += 1;
        }
    }

    for range in &ranges {
        let block_rows = &rows[range.clone()];
        let row_count = u32::try_from(block_rows.len()).map_err(|_| {
            BorsukError::InvalidStorage("lexical block row count exceeds u32".to_string())
        })?;
        let metadata = block_rows
            .iter()
            .enumerate()
            .map(|(local_row, row)| LexicalRowMetadata {
                row: u32::try_from(local_row).expect("block row count checked"),
                record_id: row.record_id.clone(),
                generation: row.generation,
                mutation_stamp: row.mutation_stamp,
                document_length: row.document_length,
            })
            .collect::<Vec<_>>();
        metadata_blocks.push(metadata);

        let mut stats = BTreeMap::<u32, (u32, f32, f32, u32)>::new();
        match kind {
            LexicalKind::Bm25 => {
                let mut postings = Vec::new();
                for (local_row, row) in block_rows.iter().enumerate() {
                    for &(term, value) in &row.terms {
                        let tf = value as u32;
                        postings.push(Bm25Posting {
                            term,
                            row: u32::try_from(local_row).expect("block row count checked"),
                            term_frequency: tf,
                        });
                        update_stats(&mut stats, term, value, row.document_length);
                    }
                }
                postings.sort_unstable_by_key(|posting| (posting.term, posting.row));
                bm25_blocks.push((postings, row_count));
            }
            LexicalKind::Sparse => {
                let mut postings = Vec::new();
                for (local_row, row) in block_rows.iter().enumerate() {
                    for &(term, value) in &row.terms {
                        postings.push(SparsePosting {
                            term,
                            row: u32::try_from(local_row).expect("block row count checked"),
                            value,
                        });
                        update_stats(&mut stats, term, value, 0);
                    }
                }
                postings.sort_unstable_by_key(|posting| (posting.term, posting.row));
                sparse_blocks.push((postings, row_count));
            }
        }
        block_stats.push(stats);
    }

    let postings_bytes = match kind {
        LexicalKind::Bm25 => bm25_posting_blocks_to_parquet(&bm25_blocks)?,
        LexicalKind::Sparse => sparse_posting_blocks_to_parquet_typed(&sparse_blocks, value_type)?,
    };
    let metadata_bytes = lexical_row_metadata_blocks_to_parquet(kind, &metadata_blocks)?;
    let postings_checksum = blake3::hash(&postings_bytes).to_hex().to_string();
    let metadata_checksum = blake3::hash(&metadata_bytes).to_hex().to_string();
    let postings_path = content_path(
        "postings",
        kind,
        field_name,
        segment_key,
        &postings_checksum,
    );
    let metadata_path = content_path("rows", kind, field_name, segment_key, &metadata_checksum);
    let mut entries = Vec::new();
    for (block_index, (range, stats)) in ranges.iter().zip(block_stats).enumerate() {
        let postings_group_checksum = match kind {
            LexicalKind::Bm25 => bm25_postings_checksum(&bm25_blocks[block_index].0),
            LexicalKind::Sparse => sparse_postings_checksum(&sparse_blocks[block_index].0),
        };
        let run = LexicalRunRef {
            segment_key: segment_key.to_string(),
            row_start: u32::try_from(range.start).map_err(|_| {
                BorsukError::InvalidStorage("lexical segment row offset exceeds u32".to_string())
            })?,
            row_count: u32::try_from(range.len()).map_err(|_| {
                BorsukError::InvalidStorage("lexical block row count exceeds u32".to_string())
            })?,
            decoded_bytes: rows[range.clone()]
                .iter()
                .map(estimated_row_bytes)
                .fold(0_u64, |total, bytes| total.saturating_add(bytes as u64)),
            postings_path: postings_path.clone(),
            postings_checksum: postings_checksum.clone(),
            postings_bytes: postings_bytes.len() as u64,
            postings_row_group: u32::try_from(block_index).map_err(|_| {
                BorsukError::InvalidStorage(
                    "lexical postings row-group ordinal exceeds u32".to_string(),
                )
            })?,
            postings_group_checksum,
            metadata_path: metadata_path.clone(),
            metadata_checksum: metadata_checksum.clone(),
            metadata_bytes: metadata_bytes.len() as u64,
            metadata_row_group: u32::try_from(block_index).map_err(|_| {
                BorsukError::InvalidStorage(
                    "lexical metadata row-group ordinal exceeds u32".to_string(),
                )
            })?,
            metadata_group_checksum: row_metadata_checksum(&metadata_blocks[block_index]),
        };
        for (term, (posting_count, min_value, max_value, min_doc_length)) in stats {
            entries.push(LexicalTermBlock {
                term,
                document_frequency: segment_df[&term],
                run: run.clone(),
                posting_count,
                min_value,
                max_value,
                min_doc_length,
            });
        }
    }
    let objects = vec![
        LexicalObject {
            path: postings_path,
            bytes: postings_bytes,
        },
        LexicalObject {
            path: metadata_path,
            bytes: metadata_bytes,
        },
    ];
    entries.sort_by(|left, right| {
        left.term
            .cmp(&right.term)
            .then_with(|| left.run.segment_key.cmp(&right.run.segment_key))
            .then_with(|| left.run.row_start.cmp(&right.run.row_start))
    });
    let total_document_length = rows.iter().map(|row| u64::from(row.document_length)).sum();
    Ok(LexicalSegmentBuild {
        kind,
        dimensions,
        document_count: rows.len() as u64,
        total_document_length,
        term_page: LexicalTermPage { kind, entries },
        objects,
    })
}

fn validate_inputs(
    kind: LexicalKind,
    dimensions: u32,
    field_name: &str,
    segment_key: &str,
    rows: &[LexicalInputRow],
    target_decoded_bytes: usize,
) -> Result<()> {
    if field_name.is_empty()
        || segment_key.is_empty()
        || target_decoded_bytes == 0
        || (kind == LexicalKind::Sparse && dimensions == 0)
        || (kind == LexicalKind::Bm25 && dimensions != 0)
    {
        return Err(BorsukError::InvalidStorage(
            "invalid lexical segment build configuration".to_string(),
        ));
    }
    for row in rows {
        if row.record_id.is_empty()
            || (kind == LexicalKind::Bm25 && row.document_length == 0)
            || (kind == LexicalKind::Sparse && row.document_length != 0)
        {
            return Err(BorsukError::InvalidStorage(
                "invalid lexical input row".to_string(),
            ));
        }
        let mut previous = None;
        for &(term, value) in &row.terms {
            if previous.is_some_and(|prior| prior >= term)
                || !value.is_finite()
                || value == 0.0
                || (kind == LexicalKind::Sparse && term >= dimensions)
                || (kind == LexicalKind::Bm25
                    && (value < 1.0 || value.fract() != 0.0 || value > u32::MAX as f32))
            {
                return Err(BorsukError::InvalidStorage(
                    "lexical input terms are invalid or unsorted".to_string(),
                ));
            }
            previous = Some(term);
        }
    }
    Ok(())
}

fn estimated_row_bytes(row: &LexicalInputRow) -> usize {
    // Row id + generation + length/offset, plus term,row,value per posting.
    row.record_id
        .len()
        .saturating_add(24)
        .saturating_add(if row.mutation_stamp.is_some() { 56 } else { 0 })
        .saturating_add(row.terms.len().saturating_mul(12))
}

fn update_stats(
    stats: &mut BTreeMap<u32, (u32, f32, f32, u32)>,
    term: u32,
    value: f32,
    document_length: u32,
) {
    stats
        .entry(term)
        .and_modify(|(count, min, max, min_dl)| {
            *count += 1;
            *min = min.min(value);
            *max = max.max(value);
            if document_length > 0 {
                *min_dl = (*min_dl).min(document_length);
            }
        })
        .or_insert((1, value, value, document_length));
}

fn content_path(
    table: &str,
    kind: LexicalKind,
    field_name: &str,
    segment_key: &str,
    checksum: &str,
) -> String {
    let field_key = blake3::hash(field_name.as_bytes()).to_hex().to_string();
    format!(
        "lexical/{}/{}/{}/{}/{}-{}.parquet",
        kind.as_str(),
        &field_key[..12],
        &segment_key[..segment_key.len().min(2)],
        segment_key,
        table,
        &checksum[..12],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn adaptive_builder_emits_only_parquet_and_splits_by_measured_bytes() {
        let rows = (0..10)
            .map(|row| LexicalInputRow {
                record_id: format!("document-{row}").into_bytes(),
                generation: row,
                mutation_stamp: None,
                terms: vec![(1, 1.0), (3, 2.0)],
                document_length: 3,
            })
            .collect::<Vec<_>>();
        let build = build_lexical_segment(
            LexicalKind::Bm25,
            VectorElementType::Float32,
            0,
            "text",
            "abcdef",
            &rows,
            100,
        )
        .unwrap();
        assert_eq!(build.objects.len(), 2);
        assert!(
            build
                .objects
                .iter()
                .all(|object| object.bytes.starts_with(b"PAR1"))
        );
        assert_eq!(build.document_count, 10);
        assert_eq!(build.total_document_length, 30);
        assert!(
            build
                .term_page
                .entries
                .iter()
                .all(|entry| entry.document_frequency == 10)
        );

        let run = &build.term_page.entries[0].run;
        let postings = build
            .objects
            .iter()
            .find(|object| object.path == run.postings_path)
            .unwrap();
        let postings_reader =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&postings.bytes))
                .unwrap();
        assert!(postings_reader.metadata().num_row_groups() > 1);
        let metadata = build
            .objects
            .iter()
            .find(|object| object.path == run.metadata_path)
            .unwrap();
        let metadata_reader =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&metadata.bytes))
                .unwrap();
        assert_eq!(
            metadata_reader.metadata().num_row_groups(),
            postings_reader.metadata().num_row_groups()
        );
    }
}
