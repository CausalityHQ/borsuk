#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use borsuk::{
    BorsukError, BorsukIndex, IndexConfig, RecordId, SearchHit, UnicodeWordLowercase, VectorMetric,
    VectorRecord, term_frequencies,
};

const K1: f64 = 1.2;
const B: f64 = 0.75;

fn index_config(uri: String, text: bool, segment_max_vectors: usize) -> IndexConfig {
    IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: 2,
        segment_max_vectors,
        ram_budget_bytes: None,
        text,
        named_vectors: Default::default(),
    }
}

fn hit_ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter()
        .map(|hit| hit.id.to_utf8_string().unwrap())
        .collect()
}

fn text_record(id: &str, ordinal: usize, text: &str) -> VectorRecord {
    VectorRecord::new(id, vec![ordinal as f32, 0.0]).with_text(text)
}

fn corpus() -> Vec<(&'static str, &'static str)> {
    vec![
        ("doc-00", "rust search engine sparse dense vector"),
        ("doc-01", "rust full text bm25 search search"),
        ("doc-02", "vector database segment storage"),
        ("doc-03", "coffee beans morning espresso"),
        ("doc-04", "bm25 ranking text retrieval retrieval"),
        ("doc-05", "unicode lowercase tokenizer text"),
        ("doc-06", "segment sidecar index storage"),
        ("doc-07", "rust tokenizer unicode word"),
        ("doc-08", "dense vector similarity search"),
        ("doc-09", "metadata filter routing segment"),
        ("doc-10", "bm25 document length average"),
        ("doc-11", "full text search sidecar postings"),
        ("doc-12", "deleted document should vanish"),
        ("doc-13", "routing table resident stats"),
        ("doc-14", "query terms document frequency"),
        ("doc-15", "term frequency inverse document"),
        ("doc-16", "storage parquet manifest routing"),
        ("doc-17", "rust crate cargo clippy"),
        ("doc-18", "text index corpus average length"),
        ("doc-19", "search query multi term ranking"),
        ("doc-20", "blue lake quiet hiking trail"),
        ("doc-21", "database retrieval sparse postings"),
        ("doc-22", "tokenizer lowercase rust unicode"),
        ("doc-23", "bm25 scoring formula exact"),
        ("doc-24", "segment payload should not read"),
        ("doc-25", "coffee grinder beans filter"),
        ("doc-26", "document frequency global corpus"),
        ("doc-27", "text terms sidecar row ids"),
        ("doc-28", "ranking search text corpus"),
        ("doc-29", "parquet storage sidecar checksum"),
    ]
}

fn brute_force_bm25(
    docs: &[(&'static str, &'static str)],
    query: &str,
    k: usize,
) -> Vec<(String, f64)> {
    let doc_terms = docs
        .iter()
        .map(|(id, text)| {
            (
                (*id).to_string(),
                term_frequencies(&UnicodeWordLowercase, text),
            )
        })
        .collect::<Vec<_>>();
    let query_terms = term_frequencies(&UnicodeWordLowercase, query)
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    let doc_lengths = doc_terms
        .iter()
        .map(|(_, terms)| terms.values().copied().sum::<u32>())
        .collect::<Vec<_>>();
    let n = doc_terms.len() as f64;
    let avgdl = doc_lengths.iter().map(|len| f64::from(*len)).sum::<f64>() / n;

    let mut dfs = BTreeMap::<u32, u32>::new();
    for term in &query_terms {
        let df = doc_terms
            .iter()
            .filter(|(_, terms)| terms.contains_key(term))
            .count();
        dfs.insert(*term, df as u32);
    }

    let mut scored = doc_terms
        .iter()
        .zip(&doc_lengths)
        .filter_map(|((id, terms), doc_len)| {
            let mut score = 0.0;
            for term in &query_terms {
                let Some(tf) = terms.get(term) else {
                    continue;
                };
                let df = f64::from(dfs[term]);
                let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                let tf = f64::from(*tf);
                let dl = f64::from(*doc_len);
                let denominator = tf + K1 * (1.0 - B + B * dl / avgdl);
                score += idf * (tf * (K1 + 1.0)) / denominator;
            }
            (score > 0.0).then(|| (id.clone(), score))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored.truncate(k);
    scored
}

#[test]
fn search_text_matches_bruteforce_bm25_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let docs = corpus();
    let mut index = BorsukIndex::create(index_config(uri, true, 7)).unwrap();
    index
        .add(
            docs.iter()
                .enumerate()
                .map(|(ordinal, (id, text))| text_record(id, ordinal, text))
                .collect(),
        )
        .unwrap();
    // With the (default-on) WAL, adds buffer into WAL objects; flush materializes
    // them into real, indexed segments so this exercises the multi-segment
    // on-disk BM25 path (bytes_read > 0, segments_searched == segments).
    index.flush().unwrap();
    assert!(
        index.stats().segments >= 2,
        "test setup must create multiple segments"
    );

    for query in [
        "rust search",
        "bm25 text retrieval",
        "segment sidecar storage",
        "unicode tokenizer lowercase rust",
    ] {
        let expected = brute_force_bm25(&docs, query, 5);
        let report = index.search_text(query, 5).unwrap();
        let expected_ids = expected
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();

        assert_eq!(hit_ids(&report.hits), expected_ids, "query `{query}`");
        assert_eq!(report.leaf_mode, "bm25");
        assert_eq!(report.segments_searched, index.stats().segments);
        assert!(report.bytes_read > 0);
        assert_eq!(report.records_considered, 0);
        assert_eq!(report.records_scored, 0);
        for (hit, (_, expected_score)) in report.hits.iter().zip(expected) {
            assert!(
                (hit.distance as f64 + expected_score).abs() <= 1e-5,
                "query `{query}` hit {:?} distance {} expected score {}",
                hit.id,
                hit.distance,
                expected_score
            );
            assert!(hit.metadata.is_none());
        }
    }
}

#[test]
fn deleted_document_never_appears_in_search_text_results() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 1)).unwrap();
    index
        .add(vec![
            text_record("deleted-best", 0, "rareterm rareterm rareterm"),
            text_record("live-next", 1, "rareterm useful"),
            text_record("live-last", 2, "rareterm"),
        ])
        .unwrap();
    assert_eq!(index.delete(["deleted-best"]).unwrap(), 1);

    let report = index.search_text("rareterm", 3).unwrap();

    let ids = hit_ids(&report.hits);
    assert_eq!(ids.len(), 2);
    assert!(!ids.contains(&"deleted-best".to_string()));
    assert!(ids.contains(&"live-next".to_string()));
    assert!(ids.contains(&"live-last".to_string()));
}

#[test]
fn upserted_text_is_searchable_immediately_and_old_text_is_hidden() {
    // Regression: the lexical leg keyed rows by id with no generation, so an
    // upserted id was dropped from search_text until compaction even though the
    // dense leg saw the new copy at once. With per-row generations the text leg
    // now applies the same MVCC visibility: the fresh document is searchable
    // immediately and its superseded copy is hidden — no compaction required.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 8)).unwrap();
    index
        .add(vec![
            text_record("doc", 0, "oldterm stable"),
            text_record("other", 1, "stable neighbour"),
        ])
        .unwrap();

    // Replace the text of `doc` in place. No compaction runs.
    index
        .upsert(vec![text_record("doc", 0, "newterm stable")])
        .unwrap();

    // The new text is immediately searchable.
    let new_hits = hit_ids(&index.search_text("newterm", 5).unwrap().hits);
    assert_eq!(
        new_hits,
        vec!["doc"],
        "upserted text must be searchable now"
    );

    // The superseded text is gone.
    let old_hits = hit_ids(&index.search_text("oldterm", 5).unwrap().hits);
    assert!(
        old_hits.is_empty(),
        "superseded text must not surface: {old_hits:?}"
    );

    // The id appears exactly once for a shared term, not once per stored copy.
    let shared_hits = hit_ids(&index.search_text("stable", 5).unwrap().hits);
    assert_eq!(
        shared_hits.iter().filter(|id| *id == "doc").count(),
        1,
        "a live id must contribute a single hit: {shared_hits:?}"
    );
}

#[test]
fn search_text_rejects_disabled_index_and_zero_k() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut dense_index = BorsukIndex::create(index_config(uri, false, 2)).unwrap();
    dense_index
        .add(vec![VectorRecord::new("dense", vec![0.0, 0.0])])
        .unwrap();

    let disabled = dense_index.search_text("anything", 1).unwrap_err();
    assert!(
        matches!(disabled, BorsukError::InvalidMetricInput(ref message) if message.contains("text=false")),
        "{disabled:?}"
    );

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();
    let mut text_index = BorsukIndex::create(index_config(uri, true, 2)).unwrap();
    text_index
        .add(vec![text_record("text", 0, "enabled text")])
        .unwrap();

    let zero_k = text_index.search_text("enabled", 0).unwrap_err();
    assert!(
        matches!(zero_k, BorsukError::InvalidSearchOptions(ref message) if message == "k must be greater than zero"),
        "{zero_k:?}"
    );
}

#[test]
fn search_text_returns_empty_hits_for_terms_absent_from_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 2)).unwrap();
    index
        .add(vec![
            text_record("alpha", 0, "alpha beta"),
            text_record("gamma", 1, "gamma delta"),
        ])
        .unwrap();

    let report = index.search_text("zanzibar", 10).unwrap();

    assert!(report.hits.is_empty());
    assert_eq!(report.leaf_mode, "bm25");
}

#[test]
fn records_without_text_are_absent_from_search_text_results() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    let mut index = BorsukIndex::create(index_config(uri, true, 2)).unwrap();
    index
        .add(vec![
            text_record("with-text", 0, "needle"),
            VectorRecord::new(RecordId::from("vector-only"), vec![1.0, 0.0]),
        ])
        .unwrap();

    let report = index.search_text("needle", 10).unwrap();

    assert_eq!(hit_ids(&report.hits), vec!["with-text"]);
}
