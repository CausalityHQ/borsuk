/// One text-bearing row: record-id bytes, MVCC generation, and `(term_id, tf)`
/// pairs. Persisted lexical data uses typed Parquet; this compact structure is
/// retained only for unflushed WAL rows.
pub(crate) type TextRow = (Vec<u8>, u64, Vec<(u32, u32)>);

/// Compact in-memory BM25 inverted index for unflushed WAL rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Bm25IndexSidecar {
    term_ids: Vec<u32>,
    posting_offsets: Vec<u32>,
    postings: Vec<(u32, u32)>,
    doc_lengths: Vec<u32>,
    row_id_offsets: Vec<u32>,
    row_id_bytes: Vec<u8>,
    generations: Vec<u64>,
}

impl Bm25IndexSidecar {
    /// Build an inverted index from text-bearing rows in row order.
    #[must_use]
    pub(crate) fn from_text_rows(rows: &[TextRow]) -> Self {
        let posting_count = rows.iter().map(|(_, _, terms)| terms.len()).sum::<usize>();
        let mut entries = Vec::<(u32, u32, u32)>::with_capacity(posting_count);
        let mut doc_lengths = Vec::with_capacity(rows.len());
        let id_bytes = rows.iter().map(|(id, _, _)| id.len()).sum::<usize>();
        let mut row_id_offsets = Vec::with_capacity(rows.len().saturating_add(1));
        let mut row_id_bytes = Vec::with_capacity(id_bytes);
        let mut generations = Vec::with_capacity(rows.len());
        row_id_offsets.push(0);

        for (row, (id, generation, term_tfs)) in rows.iter().enumerate() {
            let row = u32::try_from(row).expect("bm25 row index exceeds u32");
            row_id_bytes.extend_from_slice(id);
            row_id_offsets
                .push(u32::try_from(row_id_bytes.len()).expect("bm25 row-id bytes exceed u32"));
            generations.push(*generation);
            let mut doc_length = 0_u32;
            for &(term, tf) in term_tfs {
                doc_length = doc_length
                    .checked_add(tf)
                    .expect("bm25 document length exceeds u32");
                entries.push((term, row, tf));
            }
            doc_lengths.push(doc_length);
        }

        entries.sort_unstable_by_key(|(term, row, _)| (*term, *row));
        let mut term_ids = Vec::new();
        let mut posting_offsets = Vec::new();
        let mut postings = Vec::with_capacity(entries.len());
        let mut previous_term = None;
        for (term, row, tf) in entries {
            if previous_term != Some(term) {
                term_ids.push(term);
                posting_offsets
                    .push(u32::try_from(postings.len()).expect("bm25 posting count exceeds u32"));
                previous_term = Some(term);
            }
            postings.push((row, tf));
        }
        posting_offsets
            .push(u32::try_from(postings.len()).expect("bm25 posting count exceeds u32"));

        Self {
            term_ids,
            posting_offsets,
            postings,
            doc_lengths,
            row_id_offsets,
            row_id_bytes,
            generations,
        }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    #[must_use]
    pub(crate) fn doc_count(&self) -> u32 {
        u32::try_from(self.doc_lengths.len()).expect("bm25 doc count exceeds u32")
    }

    #[must_use]
    pub(crate) fn total_doc_length(&self) -> u64 {
        self.doc_lengths
            .iter()
            .map(|length| u64::from(*length))
            .sum()
    }

    #[must_use]
    pub(crate) fn df(&self, term: u32) -> u32 {
        u32::try_from(self.postings(term).len()).expect("bm25 df exceeds u32")
    }

    #[must_use]
    pub(crate) fn postings(&self, term: u32) -> &[(u32, u32)] {
        let Ok(index) = self.term_ids.binary_search(&term) else {
            return &[];
        };
        &self.postings
            [self.posting_offsets[index] as usize..self.posting_offsets[index + 1] as usize]
    }

    #[must_use]
    pub(crate) fn doc_lengths(&self) -> &[u32] {
        &self.doc_lengths
    }

    #[must_use]
    pub(crate) fn row_id(&self, row: u32) -> Option<&[u8]> {
        let row = row as usize;
        let start = *self.row_id_offsets.get(row)? as usize;
        let end = *self.row_id_offsets.get(row.checked_add(1)?)? as usize;
        self.row_id_bytes.get(start..end)
    }

    #[must_use]
    pub(crate) fn row_generation(&self, row: u32) -> Option<u64> {
        self.generations.get(row as usize).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_index_builds_postings_lengths_ids_and_generations() {
        let rows = vec![
            (b"a".to_vec(), 1, vec![(10, 2), (30, 1)]),
            (b"b".to_vec(), 7, vec![(10, 1), (20, 4)]),
        ];

        let index = Bm25IndexSidecar::from_text_rows(&rows);

        assert!(!index.is_empty());
        assert_eq!(index.doc_count(), 2);
        assert_eq!(index.total_doc_length(), 8);
        assert_eq!(index.df(10), 2);
        assert_eq!(index.df(20), 1);
        assert_eq!(index.df(99), 0);
        assert_eq!(index.postings(10), &[(0, 2), (1, 1)]);
        assert_eq!(index.postings(20), &[(1, 4)]);
        assert_eq!(index.doc_lengths(), &[3, 5]);
        assert_eq!(index.row_id(1), Some(b"b".as_slice()));
        assert_eq!(index.row_generation(0), Some(1));
        assert_eq!(index.row_generation(1), Some(7));
    }
}
