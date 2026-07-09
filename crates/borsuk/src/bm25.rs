use std::collections::BTreeMap;

use crate::{BorsukError, Result};

/// One text-bearing row: its record-id bytes and its `(term_id, tf)` pairs.
pub(crate) type TextRow = (Vec<u8>, Vec<(u32, u32)>);

/// Per-segment BM25 inverted-index sidecar over text-bearing rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Bm25IndexSidecar {
    postings: BTreeMap<u32, Vec<(u32, u32)>>,
    doc_lengths: Vec<u32>,
    row_ids: Vec<Vec<u8>>,
}

impl Bm25IndexSidecar {
    /// Build a sidecar from text-bearing rows in segment order.
    #[must_use]
    pub(crate) fn from_text_rows(rows: &[TextRow]) -> Self {
        let mut postings: BTreeMap<u32, Vec<(u32, u32)>> = BTreeMap::new();
        let mut doc_lengths = Vec::with_capacity(rows.len());
        let mut row_ids = Vec::with_capacity(rows.len());

        for (row, (id, term_tfs)) in rows.iter().enumerate() {
            let row = u32::try_from(row).expect("bm25 row index exceeds u32");
            row_ids.push(id.clone());
            let mut doc_length = 0_u32;
            for &(term, tf) in term_tfs {
                doc_length = doc_length
                    .checked_add(tf)
                    .expect("bm25 document length exceeds u32");
                postings.entry(term).or_default().push((row, tf));
            }
            doc_lengths.push(doc_length);
        }

        Self {
            postings,
            doc_lengths,
            row_ids,
        }
    }

    /// Return whether the sidecar contains no text-bearing rows.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    /// Return the number of indexed documents.
    #[must_use]
    pub(crate) fn doc_count(&self) -> u32 {
        u32::try_from(self.doc_lengths.len()).expect("bm25 doc count exceeds u32")
    }

    /// Return the total length of all indexed documents.
    #[must_use]
    pub(crate) fn total_doc_length(&self) -> u64 {
        self.doc_lengths.iter().map(|len| u64::from(*len)).sum()
    }

    /// Return the document frequency for a term in this sidecar.
    #[must_use]
    pub(crate) fn df(&self, term: u32) -> u32 {
        self.postings
            .get(&term)
            .map(|postings| u32::try_from(postings.len()).expect("bm25 df exceeds u32"))
            .unwrap_or(0)
    }

    /// Return postings for a term.
    #[must_use]
    pub(crate) fn postings(&self, term: u32) -> &[(u32, u32)] {
        self.postings.get(&term).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Return the length of an indexed document row.
    #[must_use]
    pub(crate) fn doc_length(&self, row: u32) -> Option<u32> {
        usize::try_from(row)
            .ok()
            .and_then(|index| self.doc_lengths.get(index))
            .copied()
    }

    /// Return the record id bytes for an indexed document row.
    #[must_use]
    pub(crate) fn row_id(&self, row: u32) -> Option<&[u8]> {
        usize::try_from(row)
            .ok()
            .and_then(|index| self.row_ids.get(index))
            .map(Vec::as_slice)
    }

    /// Encode the sidecar to little-endian bytes.
    ///
    /// Layout: `u32 doc_count`, `doc_count` `u32` document lengths, `u32
    /// row_id_count`, then per row `u32 id_len` and raw id bytes, followed by
    /// `u32 term_count`, then per term `u32 term_id`, `u32 posting_count`, and
    /// `posting_count` pairs of `u32 row` and `u32 tf`.
    #[must_use]
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_u32(self.doc_count(), &mut out);
        for &doc_length in &self.doc_lengths {
            write_u32(doc_length, &mut out);
        }
        write_u32(
            u32::try_from(self.row_ids.len()).expect("bm25 row id count exceeds u32"),
            &mut out,
        );
        for id in &self.row_ids {
            write_u32(
                u32::try_from(id.len()).expect("bm25 row id length exceeds u32"),
                &mut out,
            );
            out.extend_from_slice(id);
        }
        write_u32(
            u32::try_from(self.postings.len()).expect("bm25 term count exceeds u32"),
            &mut out,
        );
        for (&term, postings) in &self.postings {
            write_u32(term, &mut out);
            write_u32(
                u32::try_from(postings.len()).expect("bm25 posting count exceeds u32"),
                &mut out,
            );
            for &(row, tf) in postings {
                write_u32(row, &mut out);
                write_u32(tf, &mut out);
            }
        }
        out
    }

    /// Decode a sidecar produced by [`Bm25IndexSidecar::to_bytes`].
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor { bytes, pos: 0 };
        let doc_count = read_u32(&mut cursor)?;
        let capacity = usize::try_from(doc_count.min(4096))
            .map_err(|_| corrupt("doc count does not fit usize"))?;
        let mut doc_lengths = Vec::with_capacity(capacity);
        for _ in 0..doc_count {
            doc_lengths.push(read_u32(&mut cursor)?);
        }

        let row_id_count = read_u32(&mut cursor)?;
        if row_id_count != doc_count {
            return Err(corrupt("row id count does not match doc count"));
        }
        let mut row_ids = Vec::with_capacity(capacity);
        for _ in 0..row_id_count {
            let id_len = usize::try_from(read_u32(&mut cursor)?)
                .map_err(|_| corrupt("row id length does not fit usize"))?;
            if id_len == 0 {
                return Err(corrupt("record id must not be empty"));
            }
            row_ids.push(read_bytes(&mut cursor, id_len)?.to_vec());
        }

        let term_count = read_u32(&mut cursor)?;
        let mut postings = BTreeMap::new();
        let doc_count_usize =
            usize::try_from(doc_count).map_err(|_| corrupt("doc count does not fit usize"))?;
        let mut observed_lengths = vec![0_u32; doc_count_usize];
        for _ in 0..term_count {
            let term = read_u32(&mut cursor)?;
            let posting_count = read_u32(&mut cursor)?;
            if posting_count > doc_count {
                return Err(corrupt("posting count exceeds doc count"));
            }
            let posting_capacity = usize::try_from(posting_count.min(4096))
                .map_err(|_| corrupt("posting count does not fit usize"))?;
            let mut term_postings = Vec::with_capacity(posting_capacity);
            let mut previous_row = None;
            for _ in 0..posting_count {
                let row = read_u32(&mut cursor)?;
                if row >= doc_count {
                    return Err(corrupt("posting row exceeds doc count"));
                }
                if previous_row.is_some_and(|previous| row <= previous) {
                    return Err(corrupt("posting rows must be strictly ascending"));
                }
                previous_row = Some(row);

                let tf = read_u32(&mut cursor)?;
                if tf == 0 {
                    return Err(corrupt("posting term frequency must be greater than zero"));
                }
                let row_index =
                    usize::try_from(row).map_err(|_| corrupt("posting row does not fit usize"))?;
                observed_lengths[row_index] = observed_lengths[row_index]
                    .checked_add(tf)
                    .ok_or_else(|| corrupt("document length overflow"))?;
                term_postings.push((row, tf));
            }
            if postings.insert(term, term_postings).is_some() {
                return Err(corrupt("duplicate term id"));
            }
        }

        if observed_lengths != doc_lengths {
            return Err(corrupt("document lengths do not match postings"));
        }
        if cursor.pos != bytes.len() {
            return Err(corrupt("trailing bytes after bm25 index sidecar"));
        }

        Ok(Self {
            postings,
            doc_lengths,
            row_ids,
        })
    }
}

fn write_u32(value: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

fn read_u32(cursor: &mut Cursor<'_>) -> Result<u32> {
    Ok(u32::from_le_bytes(read_array(cursor)?))
}

fn read_array<const N: usize>(cursor: &mut Cursor<'_>) -> Result<[u8; N]> {
    let end = cursor
        .pos
        .checked_add(N)
        .filter(|end| *end <= cursor.bytes.len())
        .ok_or_else(|| corrupt("unexpected end of bm25 index"))?;
    let slice = &cursor.bytes[cursor.pos..end];
    cursor.pos = end;

    let mut array = [0; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn read_bytes<'a>(cursor: &mut Cursor<'a>, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .pos
        .checked_add(len)
        .filter(|end| *end <= cursor.bytes.len())
        .ok_or_else(|| corrupt("unexpected end of bm25 index"))?;
    let slice = &cursor.bytes[cursor.pos..end];
    cursor.pos = end;
    Ok(slice)
}

fn corrupt(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("bm25 index: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_sidecar_builds_postings_and_lengths() {
        let rows = vec![
            (b"a".to_vec(), vec![(10, 2), (30, 1)]),
            (b"b".to_vec(), vec![(10, 1), (20, 4)]),
        ];

        let sidecar = Bm25IndexSidecar::from_text_rows(&rows);

        assert!(!sidecar.is_empty());
        assert_eq!(sidecar.doc_count(), 2);
        assert_eq!(sidecar.total_doc_length(), 8);
        assert_eq!(sidecar.df(10), 2);
        assert_eq!(sidecar.df(20), 1);
        assert_eq!(sidecar.df(99), 0);
        assert_eq!(sidecar.postings(10), &[(0, 2), (1, 1)]);
        assert_eq!(sidecar.postings(20), &[(1, 4)]);
        assert_eq!(sidecar.doc_length(0), Some(3));
        assert_eq!(sidecar.doc_length(1), Some(5));
        assert_eq!(sidecar.row_id(1), Some(b"b".as_slice()));
    }

    #[test]
    fn bm25_sidecar_codec_roundtrips_and_rejects_truncation() {
        let rows = vec![
            (b"a".to_vec(), vec![(10, 2), (30, 1)]),
            (b"b".to_vec(), vec![(10, 1), (20, 4)]),
        ];
        let sidecar = Bm25IndexSidecar::from_text_rows(&rows);
        let bytes = sidecar.to_bytes();

        assert_eq!(Bm25IndexSidecar::from_bytes(&bytes).unwrap(), sidecar);
        assert!(Bm25IndexSidecar::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn bm25_sidecar_codec_rejects_inconsistent_lengths() {
        let rows = vec![(b"a".to_vec(), vec![(10, 2)])];
        let sidecar = Bm25IndexSidecar::from_text_rows(&rows);
        let mut bytes = sidecar.to_bytes();
        bytes[0] = 2;

        assert!(Bm25IndexSidecar::from_bytes(&bytes).is_err());
    }
}
