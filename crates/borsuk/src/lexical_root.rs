//! Format-independent global routing for Parquet lexical blocks.
//!
//! Physical roots, term pages, postings, and row metadata are all Arrow/Parquet
//! tables (see `format`). This module only owns validated routing data and exact
//! upper-bound planning; it intentionally defines no private binary codec.

use std::collections::{BTreeMap, BTreeSet};

use crate::{BorsukError, Result};

const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LexicalKind {
    Bm25,
    Sparse,
}

impl LexicalKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Bm25 => "bm25",
            Self::Sparse => "sparse",
        }
    }

    pub(crate) fn from_str(value: &str) -> Result<Self> {
        match value {
            "bm25" => Ok(Self::Bm25),
            "sparse" => Ok(Self::Sparse),
            _ => Err(corrupt(format!("unknown lexical kind `{value}`"))),
        }
    }
}

/// Top-level term-range page reference. A query fetches only pages whose
/// inclusive term range intersects its terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexicalTermPageRef {
    pub first_term: u32,
    pub last_term: u32,
    pub path: String,
    /// BLAKE3 of the complete Parquet object, used for content addressing.
    pub checksum: String,
    /// BLAKE3 of canonical decoded entries, verified after ranged decoding.
    pub content_checksum: String,
    pub encoded_bytes: u64,
    pub term_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexicalRoot {
    pub kind: LexicalKind,
    pub dimensions: u32,
    pub document_count: u64,
    pub total_document_length: u64,
    pub pages: Vec<LexicalTermPageRef>,
}

/// One immutable row-block run. All paths reference Parquet tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexicalRunRef {
    /// Stable immutable segment checksum, independent of manifest ordering.
    pub segment_key: String,
    pub row_start: u32,
    pub row_count: u32,
    pub decoded_bytes: u64,
    pub postings_path: String,
    pub postings_checksum: String,
    pub postings_bytes: u64,
    pub postings_row_group: u32,
    /// BLAKE3 of canonical decoded rows in this selected Parquet row group.
    pub postings_group_checksum: String,
    pub metadata_path: String,
    pub metadata_checksum: String,
    pub metadata_bytes: u64,
    pub metadata_row_group: u32,
    /// BLAKE3 of canonical decoded metadata rows in this row group.
    pub metadata_group_checksum: String,
}

/// One term's statistics inside one row block.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LexicalTermBlock {
    pub term: u32,
    pub document_frequency: u64,
    pub run: LexicalRunRef,
    pub posting_count: u32,
    /// Sparse minimum value, or minimum BM25 term frequency.
    pub min_value: f32,
    /// Sparse maximum value, or maximum BM25 term frequency.
    pub max_value: f32,
    /// Minimum length among BM25 documents carrying this term in the run.
    pub min_doc_length: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LexicalTermPage {
    pub kind: LexicalKind,
    pub entries: Vec<LexicalTermBlock>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlannedRun {
    pub run: LexicalRunRef,
    pub upper_bound: f64,
    pub terms: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Bm25Posting {
    pub term: u32,
    pub row: u32,
    pub term_frequency: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SparsePosting {
    pub term: u32,
    pub row: u32,
    pub value: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LexicalRowMetadata {
    pub row: u32,
    pub record_id: Vec<u8>,
    pub generation: u64,
    pub mutation_stamp: Option<crate::mutation::MutationStamp>,
    /// Zero for sparse rows; positive for BM25 rows.
    pub document_length: u32,
}

pub(crate) fn bm25_postings_checksum(postings: &[Bm25Posting]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"borsuk/bm25/parquet-row-group");
    for posting in postings {
        hasher.update(&posting.term.to_le_bytes());
        hasher.update(&posting.row.to_le_bytes());
        hasher.update(&posting.term_frequency.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

pub(crate) fn sparse_postings_checksum(postings: &[SparsePosting]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"borsuk/sparse/parquet-row-group");
    for posting in postings {
        hasher.update(&posting.term.to_le_bytes());
        hasher.update(&posting.row.to_le_bytes());
        hasher.update(&posting.value.to_bits().to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

pub(crate) fn row_metadata_checksum(rows: &[LexicalRowMetadata]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"borsuk/lexical-metadata/parquet-row-group");
    for row in rows {
        hasher.update(&row.row.to_le_bytes());
        hasher.update(&(row.record_id.len() as u64).to_le_bytes());
        hasher.update(&row.record_id);
        hasher.update(&row.generation.to_le_bytes());
        match row.mutation_stamp {
            Some(stamp) => {
                hasher.update(&[1]);
                hasher.update(&stamp.version().hlc().to_le_bytes());
                hasher.update(&stamp.version().writer());
                hasher.update(&stamp.digest());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        hasher.update(&row.document_length.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

pub(crate) fn term_page_content_checksum(page: &LexicalTermPage) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"borsuk/lexical-term-page");
    hasher.update(page.kind.as_str().as_bytes());
    for entry in &page.entries {
        hasher.update(&entry.term.to_le_bytes());
        hasher.update(&entry.document_frequency.to_le_bytes());
        hasher.update(&(entry.run.segment_key.len() as u64).to_le_bytes());
        hasher.update(entry.run.segment_key.as_bytes());
        hasher.update(&entry.run.row_start.to_le_bytes());
        hasher.update(&entry.run.row_count.to_le_bytes());
        hasher.update(&entry.run.decoded_bytes.to_le_bytes());
        for value in [
            &entry.run.postings_path,
            &entry.run.postings_checksum,
            &entry.run.postings_group_checksum,
            &entry.run.metadata_path,
            &entry.run.metadata_checksum,
            &entry.run.metadata_group_checksum,
        ] {
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
        hasher.update(&entry.run.postings_bytes.to_le_bytes());
        hasher.update(&entry.run.postings_row_group.to_le_bytes());
        hasher.update(&entry.run.metadata_bytes.to_le_bytes());
        hasher.update(&entry.run.metadata_row_group.to_le_bytes());
        hasher.update(&entry.posting_count.to_le_bytes());
        hasher.update(&entry.min_value.to_bits().to_le_bytes());
        hasher.update(&entry.max_value.to_bits().to_le_bytes());
        hasher.update(&entry.min_doc_length.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

impl LexicalRoot {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_kind_dimensions(self.kind, self.dimensions)?;
        if self.kind == LexicalKind::Bm25
            && self.document_count > 0
            && self.total_document_length == 0
        {
            return Err(corrupt("BM25 root has documents but zero total length"));
        }
        for page in &self.pages {
            if page.first_term > page.last_term
                || page.path.is_empty()
                || page.checksum.is_empty()
                || page.content_checksum.is_empty()
                || page.encoded_bytes == 0
                || page.term_count == 0
            {
                return Err(corrupt("invalid term-page reference"));
            }
        }
        if self
            .pages
            .windows(2)
            .any(|pages| pages[0].last_term > pages[1].first_term)
        {
            return Err(corrupt("term-page ranges overlap or are unsorted"));
        }
        Ok(())
    }

    pub(crate) fn pages_for_terms(&self, terms: &BTreeSet<u32>) -> Vec<&LexicalTermPageRef> {
        if terms.is_empty() {
            return Vec::new();
        }
        self.pages
            .iter()
            .filter(|page| {
                terms
                    .range(page.first_term..=page.last_term)
                    .next()
                    .is_some()
            })
            .collect()
    }
}

impl LexicalTermPage {
    pub(crate) fn validate(&self, root: &LexicalRoot) -> Result<()> {
        if self.kind != root.kind {
            return Err(corrupt("term-page kind differs from root"));
        }
        let mut prior_key = None;
        let mut document_frequencies = BTreeMap::new();
        for entry in &self.entries {
            let key = (
                entry.term,
                entry.run.segment_key.as_str(),
                entry.run.row_start,
            );
            if prior_key.is_some_and(|prior| prior >= key) {
                return Err(corrupt("term blocks are not strictly sorted"));
            }
            prior_key = Some(key);
            if entry.document_frequency == 0
                || entry.document_frequency > root.document_count
                || entry.posting_count == 0
                || !entry.min_value.is_finite()
                || !entry.max_value.is_finite()
                || entry.min_value > entry.max_value
            {
                return Err(corrupt("invalid term-block statistics"));
            }
            if let Some(previous) =
                document_frequencies.insert(entry.term, entry.document_frequency)
                && previous != entry.document_frequency
            {
                return Err(corrupt("global document frequency differs across blocks"));
            }
            validate_run(&entry.run)?;
            if self.kind == LexicalKind::Bm25
                && (entry.min_value < 1.0
                    || entry.max_value.fract() != 0.0
                    || entry.min_doc_length == 0)
            {
                return Err(corrupt("invalid BM25 term-block bounds"));
            }
        }
        Ok(())
    }

    pub(crate) fn plan_sparse(
        pages: &[Self],
        root: &LexicalRoot,
        query: &[(u32, f32)],
    ) -> Result<Vec<PlannedRun>> {
        if root.kind != LexicalKind::Sparse {
            return Err(corrupt("sparse planner used with BM25 root"));
        }
        if query
            .iter()
            .any(|(term, weight)| *term >= root.dimensions || !weight.is_finite())
        {
            return Err(BorsukError::InvalidMetricInput(
                "sparse query term or weight is invalid".to_string(),
            ));
        }
        for page in pages {
            page.validate(root)?;
        }
        let weights = query.iter().copied().collect::<BTreeMap<_, _>>();
        let mut plans = BTreeMap::<(String, u32, String), PlannedRun>::new();
        for entry in pages.iter().flat_map(|page| &page.entries) {
            let Some(&weight) = weights.get(&entry.term) else {
                continue;
            };
            if weight == 0.0 {
                continue;
            }
            let contribution = if weight.is_sign_negative() {
                f64::from(weight) * f64::from(entry.min_value)
            } else {
                f64::from(weight) * f64::from(entry.max_value)
            };
            add_plan(&mut plans, entry, contribution);
        }
        Ok(sorted_plans(plans))
    }

    pub(crate) fn plan_bm25(
        pages: &[Self],
        root: &LexicalRoot,
        query_terms: &BTreeSet<u32>,
    ) -> Result<Vec<PlannedRun>> {
        if root.kind != LexicalKind::Bm25 {
            return Err(corrupt("BM25 planner used with sparse root"));
        }
        if root.document_count == 0 || root.total_document_length == 0 {
            return Ok(Vec::new());
        }
        for page in pages {
            page.validate(root)?;
        }
        let avgdl = root.total_document_length as f64 / root.document_count as f64;
        let mut plans = BTreeMap::<(String, u32, String), PlannedRun>::new();
        for entry in pages.iter().flat_map(|page| &page.entries) {
            if !query_terms.contains(&entry.term) {
                continue;
            }
            let idf = (1.0
                + (root.document_count as f64 - entry.document_frequency as f64 + 0.5)
                    / (entry.document_frequency as f64 + 0.5))
                .ln();
            let tf = f64::from(entry.max_value);
            let dl = f64::from(entry.min_doc_length);
            let denominator = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl);
            let contribution = idf * (tf * (BM25_K1 + 1.0)) / denominator;
            add_plan(&mut plans, entry, contribution);
        }
        Ok(sorted_plans(plans))
    }
}

fn add_plan(
    plans: &mut BTreeMap<(String, u32, String), PlannedRun>,
    entry: &LexicalTermBlock,
    contribution: f64,
) {
    let key = (
        entry.run.segment_key.clone(),
        entry.run.row_start,
        entry.run.postings_path.clone(),
    );
    let plan = plans.entry(key).or_insert_with(|| PlannedRun {
        run: entry.run.clone(),
        upper_bound: 0.0,
        terms: Vec::new(),
    });
    plan.upper_bound += contribution;
    plan.terms.push(entry.term);
}

fn sorted_plans(plans: BTreeMap<(String, u32, String), PlannedRun>) -> Vec<PlannedRun> {
    let mut plans = plans.into_values().collect::<Vec<_>>();
    for plan in &mut plans {
        plan.terms.sort_unstable();
        plan.terms.dedup();
    }
    plans.sort_by(|left, right| {
        right
            .upper_bound
            .total_cmp(&left.upper_bound)
            .then_with(|| left.run.segment_key.cmp(&right.run.segment_key))
            .then_with(|| left.run.row_start.cmp(&right.run.row_start))
    });
    plans
}

fn validate_kind_dimensions(kind: LexicalKind, dimensions: u32) -> Result<()> {
    if (kind == LexicalKind::Sparse && dimensions == 0)
        || (kind == LexicalKind::Bm25 && dimensions != 0)
    {
        return Err(corrupt("kind/dimensions mismatch"));
    }
    Ok(())
}

fn validate_run(run: &LexicalRunRef) -> Result<()> {
    if run.row_count == 0
        || run.decoded_bytes == 0
        || run.segment_key.is_empty()
        || run.postings_path.is_empty()
        || run.postings_checksum.is_empty()
        || run.postings_group_checksum.is_empty()
        || run.postings_bytes == 0
        || run.metadata_path.is_empty()
        || run.metadata_checksum.is_empty()
        || run.metadata_group_checksum.is_empty()
        || run.metadata_bytes == 0
    {
        return Err(corrupt("invalid lexical run reference"));
    }
    run.row_start
        .checked_add(run.row_count)
        .ok_or_else(|| corrupt("run row range overflows"))?;
    Ok(())
}

fn corrupt(message: impl Into<String>) -> BorsukError {
    BorsukError::InvalidStorage(format!("lexical index: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(segment_ordinal: u32, row_start: u32) -> LexicalRunRef {
        LexicalRunRef {
            segment_key: format!("segment-{segment_ordinal}"),
            row_start,
            row_count: 8,
            decoded_bytes: 640,
            postings_path: format!("lexical/postings/{segment_ordinal}-{row_start}.parquet"),
            postings_checksum: "a".repeat(64),
            postings_bytes: 1024,
            postings_row_group: row_start / 8,
            postings_group_checksum: "c".repeat(64),
            metadata_path: format!("lexical/rows/{segment_ordinal}-{row_start}.parquet"),
            metadata_checksum: "b".repeat(64),
            metadata_bytes: 800,
            metadata_row_group: row_start / 8,
            metadata_group_checksum: "d".repeat(64),
        }
    }

    fn sparse_fixture() -> (LexicalRoot, LexicalTermPage) {
        let root = LexicalRoot {
            kind: LexicalKind::Sparse,
            dimensions: 32,
            document_count: 16,
            total_document_length: 0,
            pages: vec![LexicalTermPageRef {
                first_term: 2,
                last_term: 9,
                path: "lexical/root/2-9.parquet".to_string(),
                checksum: "c".repeat(64),
                content_checksum: "e".repeat(64),
                encoded_bytes: 1200,
                term_count: 2,
            }],
        };
        let page = LexicalTermPage {
            kind: LexicalKind::Sparse,
            entries: vec![
                LexicalTermBlock {
                    term: 2,
                    document_frequency: 5,
                    run: run(0, 0),
                    posting_count: 3,
                    min_value: -1.0,
                    max_value: 3.0,
                    min_doc_length: 0,
                },
                LexicalTermBlock {
                    term: 2,
                    document_frequency: 5,
                    run: run(0, 8),
                    posting_count: 2,
                    min_value: -2.0,
                    max_value: 1.0,
                    min_doc_length: 0,
                },
                LexicalTermBlock {
                    term: 9,
                    document_frequency: 3,
                    run: run(0, 0),
                    posting_count: 3,
                    min_value: -4.0,
                    max_value: 5.0,
                    min_doc_length: 0,
                },
            ],
        };
        (root, page)
    }

    #[test]
    fn root_routes_only_intersecting_term_pages() {
        let (root, _) = sparse_fixture();
        root.validate().unwrap();
        let terms = [1, 10].into_iter().collect();
        assert!(root.pages_for_terms(&terms).is_empty());
        let terms = [9].into_iter().collect();
        assert_eq!(root.pages_for_terms(&terms), vec![&root.pages[0]]);
    }

    #[test]
    fn term_page_checksum_covers_routing_and_bound_content() {
        let (_, page) = sparse_fixture();
        let checksum = term_page_content_checksum(&page);
        let mut changed = page.clone();
        changed.entries[0].max_value += 1.0;
        assert_ne!(term_page_content_checksum(&changed), checksum);

        let mut changed = page;
        changed.entries[0].run.metadata_row_group += 1;
        assert_ne!(term_page_content_checksum(&changed), checksum);
    }

    #[test]
    fn sparse_planner_groups_parquet_runs_and_uses_sign_safe_bounds() {
        let (root, page) = sparse_fixture();
        let plans = LexicalTermPage::plan_sparse(&[page], &root, &[(2, -2.0), (9, 0.5)]).unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].run.row_start, 0);
        assert_eq!(plans[0].upper_bound, 4.5);
        assert_eq!(plans[0].terms, vec![2, 9]);
        assert_eq!(plans[1].upper_bound, 4.0);
    }

    #[test]
    fn bm25_bound_is_never_below_any_row_score() {
        let (mut root, mut page) = sparse_fixture();
        root.kind = LexicalKind::Bm25;
        root.dimensions = 0;
        root.document_count = 16;
        root.total_document_length = 160;
        page.kind = LexicalKind::Bm25;
        page.entries.truncate(1);
        page.entries[0].min_value = 1.0;
        page.entries[0].max_value = 8.0;
        page.entries[0].min_doc_length = 4;
        let query = [2].into_iter().collect();
        let bound = LexicalTermPage::plan_bm25(&[page], &root, &query).unwrap()[0].upper_bound;
        let idf = (1.0_f64 + (16.0_f64 - 5.0_f64 + 0.5_f64) / (5.0_f64 + 0.5_f64)).ln();
        for tf in 1..=8 {
            for dl in 4..=32 {
                let score = idf * (f64::from(tf) * (BM25_K1 + 1.0))
                    / (f64::from(tf) + BM25_K1 * (1.0 - BM25_B + BM25_B * f64::from(dl) / 10.0));
                assert!(bound + f64::EPSILON >= score);
            }
        }
    }

    #[test]
    fn pages_reject_inconsistent_global_df() {
        let (root, mut page) = sparse_fixture();
        page.entries[1].document_frequency = 4;
        assert!(page.validate(&root).is_err());
    }
}
