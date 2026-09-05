//! Local claim-ineligible V33 shape-frontier replay through V32 PQ/page reduction.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
};

use borsuk::{
    V27HierarchyArtifactIdentity, V27HierarchyArtifacts, V30LayoutArtifactIdentity,
    V30LayoutArtifacts, V30PqArtifactIdentity, V30PqArtifacts, V32Router, V32SearchArm,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const FRONTIER_SHA256: &str = "470f7c95a965572feec11cd1b0d24e73bf1d8c1456a75117b8bf6796e091db6b";
const FRONTIER_BYTES: usize = 5_937_815;

#[derive(Debug, PartialEq, Eq)]
struct Args {
    roots: PathBuf,
    leaves: PathBuf,
    routing_ranges: PathBuf,
    page_ranges: PathBuf,
    pq24_codebook: PathBuf,
    pq48_codebook: PathBuf,
    pq_base_codes: PathBuf,
    pq_fidelity: PathBuf,
    pq_high_codes: PathBuf,
    frontier: PathBuf,
    output: PathBuf,
}

fn parse_args(values: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut values = values.into_iter();
    let mut paths = BTreeMap::new();
    let mut execute = false;
    while let Some(flag) = values.next() {
        if flag == "--execute-shape-pq-replay" {
            if execute {
                return Err("duplicate execution flag".to_owned());
            }
            execute = true;
            continue;
        }
        if !matches!(
            flag.as_str(),
            "--roots"
                | "--leaves"
                | "--routing-ranges"
                | "--page-ranges"
                | "--pq24-codebook"
                | "--pq48-codebook"
                | "--pq-base-codes"
                | "--pq-fidelity"
                | "--pq-high-codes"
                | "--frontier"
                | "--output"
        ) {
            return Err(format!("unknown flag: {flag}"));
        }
        let value = values
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        if paths.insert(flag.clone(), PathBuf::from(value)).is_some() {
            return Err(format!("duplicate flag: {flag}"));
        }
    }
    if !execute {
        return Err("--execute-shape-pq-replay is required".to_owned());
    }
    let mut take = |flag: &str| {
        paths
            .remove(flag)
            .ok_or_else(|| format!("{flag} is required"))
    };
    let args = Args {
        roots: take("--roots")?,
        leaves: take("--leaves")?,
        routing_ranges: take("--routing-ranges")?,
        page_ranges: take("--page-ranges")?,
        pq24_codebook: take("--pq24-codebook")?,
        pq48_codebook: take("--pq48-codebook")?,
        pq_base_codes: take("--pq-base-codes")?,
        pq_fidelity: take("--pq-fidelity")?,
        pq_high_codes: take("--pq-high-codes")?,
        frontier: take("--frontier")?,
        output: take("--output")?,
    };
    if !paths.is_empty() {
        return Err("V33 replay CLI authority differs".to_owned());
    }
    Ok(args)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFrontierRecord {
    hits: usize,
    query: Vec<f32>,
    query_ordinal: usize,
    selected_groups: Vec<usize>,
    selected_routing_leaves: Vec<u32>,
    selected_rows: u64,
    truth_logicals: Vec<u64>,
    truth_owner_ranks: Vec<usize>,
}

#[derive(Debug, PartialEq)]
struct FrontierRecord {
    hits: usize,
    query: [f32; 96],
    query_ordinal: usize,
    selected_groups: Vec<usize>,
    selected_routing_leaves: Vec<u32>,
    selected_rows: u64,
    truth_logicals: Vec<u64>,
}

fn expected_frontier_inputs() -> BTreeMap<String, String> {
    [
        (
            "directory",
            "1cd77b268304bc4d36acf9f4beb402ccabc3ec0b1ebde316d2dd7f3a2cdcc995",
        ),
        (
            "expanded_terminal",
            "f78754e0453d939a2c44a7dfeb332bf08e274264f12a48c706994171c2d00950",
        ),
        (
            "leaves",
            "acd94415d04602a8149354189b934e90a0340a5381cf892066fdc0798e73819e",
        ),
        (
            "prospective_terminal",
            "c54255e18102a425d740acb7b204bc5215a0325fed5632dae3546571a5cff8cb",
        ),
        (
            "query",
            "296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4",
        ),
        (
            "routing_ranges",
            "29c4c432560e87c5b00b7043426a3aec4886c6838e0e15c1f572771944abf0a6",
        ),
        (
            "shape",
            "6954ddac2e8dfda76338a9b3c3da278faea80326b29c3427c6aa22753d4e6bea",
        ),
    ]
    .into_iter()
    .map(|(role, digest)| (role.to_owned(), digest.to_owned()))
    .collect()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFrontierArm {
    arm: String,
    included_owners: usize,
    maximum_selected_rows: u64,
    minimum_selected_rows: u64,
    passed: bool,
    perfect_queries: usize,
    query_count: usize,
    records: Vec<serde_json::Value>,
    total_owners: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFrontierEnvelope {
    arms: Vec<RawFrontierArm>,
    claim_eligible: bool,
    code_reads: usize,
    corpus_reads: usize,
    input_sha256: BTreeMap<String, String>,
    page_reads: usize,
    passed: bool,
    row_limit: u64,
    schema: String,
}

fn parse_frontier(value: &serde_json::Value) -> Result<Vec<FrontierRecord>, String> {
    let envelope: RawFrontierEnvelope = serde_json::from_value(value.clone())
        .map_err(|error| format!("V33 frontier result schema differs: {error}"))?;
    if envelope.schema != "borsuk-v33-group-proxy-result-v2"
        || envelope.claim_eligible
        || envelope.code_reads != 5
        || envelope.corpus_reads != 0
        || envelope.page_reads != 0
        || envelope.row_limit != 262_144
        || envelope.input_sha256 != expected_frontier_inputs()
    {
        return Err("V33 frontier result authority differs".to_owned());
    }
    let mut matching = envelope
        .arms
        .into_iter()
        .filter(|arm| arm.arm == "diagonal-ellipsoid");
    let arm = matching
        .next()
        .ok_or_else(|| "V33 diagonal frontier missing".to_owned())?;
    if matching.next().is_some() {
        return Err("V33 diagonal frontier duplicated".to_owned());
    }
    let records = arm
        .records
        .iter()
        .map(parse_frontier_record)
        .collect::<Result<Vec<_>, _>>()?;
    let included = records.iter().try_fold(0_usize, |total, record| {
        total
            .checked_add(record.hits)
            .ok_or_else(|| "V33 frontier hit count overflows".to_owned())
    })?;
    let perfect = records.iter().filter(|record| record.hits == 10).count();
    let minimum = records.iter().map(|record| record.selected_rows).min();
    let maximum = records.iter().map(|record| record.selected_rows).max();
    if arm.query_count != records.len()
        || arm.total_owners != records.len() * 10
        || arm.included_owners != included
        || arm.perfect_queries != perfect
        || arm.minimum_selected_rows != minimum.unwrap_or_default()
        || arm.maximum_selected_rows != maximum.unwrap_or_default()
        || arm.passed != (included == arm.total_owners && perfect == records.len())
        || envelope.passed && !arm.passed
    {
        return Err("V33 diagonal frontier aggregate differs".to_owned());
    }
    Ok(records)
}

fn parse_frontier_record(value: &serde_json::Value) -> Result<FrontierRecord, String> {
    let raw: RawFrontierRecord = serde_json::from_value(value.clone())
        .map_err(|error| format!("V33 frontier record schema differs: {error}"))?;
    let query: [f32; 96] = raw
        .query
        .try_into()
        .map_err(|_| "V33 frontier query dimension differs".to_owned())?;
    let leaves = raw
        .selected_routing_leaves
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let groups = raw.selected_groups.iter().copied().collect::<BTreeSet<_>>();
    let truths = raw.truth_logicals.iter().copied().collect::<BTreeSet<_>>();
    if query.iter().any(|value| !value.is_finite())
        || raw.truth_logicals.len() != 10
        || truths.len() != 10
        || raw.truth_owner_ranks.len() != 10
        || raw.hits > 10
        || raw.selected_groups.is_empty()
        || groups.len() != raw.selected_groups.len()
        || raw.selected_groups.len() > 64
        || raw.selected_routing_leaves.is_empty()
        || leaves.len() != raw.selected_routing_leaves.len()
        || raw.selected_rows == 0
        || raw.selected_rows > 262_144
    {
        return Err("V33 frontier record authority differs".to_owned());
    }
    Ok(FrontierRecord {
        hits: raw.hits,
        query,
        query_ordinal: raw.query_ordinal,
        selected_groups: raw.selected_groups,
        selected_routing_leaves: raw.selected_routing_leaves,
        selected_rows: raw.selected_rows,
        truth_logicals: raw.truth_logicals,
    })
}

fn truth_page_hits(truth_pages: &[u32], selected_pages: &[u32]) -> usize {
    let selected = selected_pages.iter().copied().collect::<BTreeSet<_>>();
    truth_pages
        .iter()
        .filter(|page| selected.contains(page))
        .count()
}

fn bounded_page_prefix<T>(pages: &[T], page_count: usize) -> Result<&[T], &'static str> {
    if !(1..=64).contains(&page_count) {
        return Err("V33 replay page prefix cap differs");
    }
    Ok(&pages[..page_count.min(pages.len())])
}

#[cfg(test)]
fn canonical_result(
    _truth_total: usize,
    truth_hits: usize,
    page_reads: usize,
) -> Result<Vec<u8>, serde_json::Error> {
    let value = serde_json::json!({
        "claim_eligible": false,
        "corpus_reads": 0,
        "page_reads": page_reads,
        "schema": "borsuk-v33-shape-pq-replay-v1",
        "truth_hits": truth_hits,
    });
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn read(path: &PathBuf) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(fs::read(path)?)
}

fn hierarchy_identity(
    role: &str,
    sha256: &str,
    encoded_bytes: u64,
) -> V27HierarchyArtifactIdentity {
    V27HierarchyArtifactIdentity {
        role: role.to_owned(),
        sha256: sha256.to_owned(),
        encoded_bytes,
    }
}

fn layout_identity(role: &str, sha256: &str, encoded_bytes: u64) -> V30LayoutArtifactIdentity {
    V30LayoutArtifactIdentity {
        role: role.to_owned(),
        sha256: sha256.to_owned(),
        encoded_bytes,
    }
}

fn pq_identity(
    role: &str,
    sha256: &str,
    encoded_bytes: u64,
    row_count: u64,
    width_bytes: u8,
    dependencies: &[&str],
) -> V30PqArtifactIdentity {
    V30PqArtifactIdentity {
        role: role.to_owned(),
        sha256: sha256.to_owned(),
        encoded_bytes,
        row_count,
        width_bytes,
        dependencies: dependencies
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
    }
}

fn router(args: &Args) -> Result<V32Router, Box<dyn std::error::Error>> {
    let base_sha = "aa3bef1eefb6ef4670a8c6f73d48941116460d28ae6a552755a38f3776ffe8a8";
    let high_sha = "e2b92db4f8cdb5ab2b352ed491a5b00a47ea94431552b8d7fbf7764f4a29110c";
    let hierarchy = V27HierarchyArtifacts {
        roots: hierarchy_identity(
            "v27-roots-arrow",
            "6d264c753fb4b0338e0c5694cd2c5b3557d0fb7365dbadf487583c5796f7adfb",
            26_730,
        ),
        leaves: hierarchy_identity(
            "v27-leaves-arrow",
            "acd94415d04602a8149354189b934e90a0340a5381cf892066fdc0798e73819e",
            845_594,
        ),
        roots_bytes: read(&args.roots)?,
        leaves_bytes: read(&args.leaves)?,
    };
    let layout = V30LayoutArtifacts {
        source_rows: 1_000_000,
        leaf_ranges: layout_identity(
            "v32-routing-ranges-arrow",
            "29c4c432560e87c5b00b7043426a3aec4886c6838e0e15c1f572771944abf0a6",
            982_562,
        ),
        page_ranges: layout_identity(
            "v32-page-ranges-parquet",
            "63db80c346670dcf7e708a8f86274163e8ee67032c053f66be36cbf3bdb67aac",
            178_550,
        ),
        leaf_ranges_arrow: read(&args.routing_ranges)?,
        page_ranges_parquet: read(&args.page_ranges)?,
    };
    let pq = V30PqArtifacts {
        identities: vec![
            pq_identity("pq24-codebook", base_sha, 102_202, 1, 24, &[]),
            pq_identity("pq48-codebook", high_sha, 102_202, 1, 48, &[]),
            pq_identity(
                "pq-base-codes",
                "fa66cf136d8004362eb8d616b54c3469f4cf9f877874a1d0ba1934cd223b7dbd",
                23_079_546,
                950_000,
                24,
                &[base_sha],
            ),
            pq_identity(
                "pq-fidelity",
                "5c4f7332c2ffb30d3dcad3a8906800c838eacdb269a704401093e471952f40bc",
                231_490,
                1_000_000,
                0,
                &[base_sha, high_sha],
            ),
            pq_identity(
                "pq-high-codes",
                "657bcc01c138df60e8ed4abbc7c5f8eec677642e3ab399691f7b698cae5ace01",
                2_416_274,
                50_000,
                48,
                &[high_sha],
            ),
        ],
        bytes: vec![
            read(&args.pq24_codebook)?,
            read(&args.pq48_codebook)?,
            read(&args.pq_base_codes)?,
            read(&args.pq_fidelity)?,
            read(&args.pq_high_codes)?,
        ],
    };
    Ok(V32Router::from_artifacts(&hierarchy, &pq, &layout)?)
}

fn expected_query_ordinals() -> Vec<usize> {
    [4096_usize, 5120, 6144, 7168]
        .into_iter()
        .flat_map(|start| start..start + 32)
        .collect()
}

fn read_frontier(path: &PathBuf) -> Result<Vec<FrontierRecord>, Box<dyn std::error::Error>> {
    let bytes = read(path)?;
    if bytes.len() != FRONTIER_BYTES || format!("{:x}", Sha256::digest(&bytes)) != FRONTIER_SHA256 {
        return Err("V33 replay frontier byte authority differs".into());
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    let records = parse_frontier(&value).map_err(std::io::Error::other)?;
    if records.len() != 128
        || records
            .iter()
            .map(|record| record.query_ordinal)
            .ne(expected_query_ordinals())
    {
        return Err("V33 replay query cohort differs".into());
    }
    Ok(records)
}

fn selected_page_value(
    pages: &[borsuk::V27PageIdentity],
    truth_pages: &[u32],
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let encoded_bytes = pages.iter().try_fold(0_u64, |total, page| {
        total
            .checked_add(page.encoded_bytes)
            .ok_or("V33 selected page byte count overflows")
    })?;
    let ordinals = pages.iter().map(|page| page.ordinal).collect::<Vec<_>>();
    let truth_hits = truth_page_hits(truth_pages, &ordinals);
    Ok(serde_json::json!({
        "encoded_bytes": encoded_bytes,
        "page_count": pages.len(),
        "page_ordinals": ordinals,
        "perfect": truth_hits == truth_pages.len(),
        "truth_hits": truth_hits,
    }))
}

fn execute(args: &Args) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let router = router(args)?;
    let records = read_frontier(&args.frontier)?;
    let arm = V32SearchArm {
        root_beam: 1,
        leaf_beam: 256,
        scan_budget: 262_144,
        candidate_depth: 12_288,
        page_count: 64,
    };
    let mut results = Vec::with_capacity(records.len());
    let mut aggregate = BTreeMap::<String, (usize, usize, u64)>::new();
    for record in records {
        let replay = router.capture_explicit_leaf_replay(
            &record.query,
            arm,
            &record.selected_routing_leaves,
        )?;
        let replay_sha256 = replay.sha256();
        let pq_work = replay.pq_work();
        let diagnostic = replay.diagnose(&record.truth_logicals)?;
        let truth_pages = diagnostic
            .targets
            .iter()
            .map(|target| target.page_ordinal)
            .collect::<Vec<_>>();
        let frontier_hits = diagnostic
            .targets
            .iter()
            .filter(|target| target.routing_leaf_rank.is_some())
            .count();
        let candidate_hits = diagnostic
            .targets
            .iter()
            .filter(|target| target.candidate_rank.is_some())
            .count();
        let mut projections = BTreeMap::new();
        for budget in [16_usize, 32, 48, 64] {
            let first = replay.physical_page_prefix(budget)?;
            let reciprocal = bounded_page_prefix(&diagnostic.reciprocal_rank_pages, budget)?;
            let first_value = selected_page_value(&first, &truth_pages)?;
            let reciprocal_value = selected_page_value(reciprocal, &truth_pages)?;
            for (name, value) in [
                ("first-distinct", &first_value),
                ("reciprocal-rank", &reciprocal_value),
            ] {
                let key = format!("{name}-{budget}");
                let entry = aggregate.entry(key).or_default();
                let hits = value["truth_hits"]
                    .as_u64()
                    .ok_or("V33 projection hit schema differs")?
                    as usize;
                entry.0 += hits;
                entry.1 += usize::from(hits == truth_pages.len());
                entry.2 = entry
                    .2
                    .checked_add(
                        value["encoded_bytes"]
                            .as_u64()
                            .ok_or("V33 projection byte schema differs")?,
                    )
                    .ok_or("V33 projection byte aggregate overflows")?;
            }
            projections.insert(
                budget.to_string(),
                serde_json::json!({
                    "first_distinct": first_value,
                    "reciprocal_rank": reciprocal_value,
                }),
            );
        }
        results.push(serde_json::json!({
            "candidate_truth_hits": candidate_hits,
            "codes_scanned": diagnostic.selection.work.codes_scanned,
            "frontier_truth_hits": frontier_hits,
            "projections": projections,
            "pq_work": {
                "base_cache_hits": pq_work.base.cache_hits,
                "base_eager_fallbacks": pq_work.base.eager_fallbacks,
                "base_entries_evaluated": pq_work.base.entries_evaluated,
                "high_cache_hits": pq_work.high.cache_hits,
                "high_eager_fallbacks": pq_work.high.eager_fallbacks,
                "high_entries_evaluated": pq_work.high.entries_evaluated,
            },
            "query_ordinal": record.query_ordinal,
            "replay_sha256": replay_sha256,
            "selected_group_count": record.selected_groups.len(),
            "selected_routing_leaf_count": record.selected_routing_leaves.len(),
            "selected_rows": record.selected_rows,
            "truth_count": record.truth_logicals.len(),
        }));
    }
    let aggregates = aggregate
        .into_iter()
        .map(|(key, (truth_hits, perfect_queries, encoded_bytes))| {
            (
                key,
                serde_json::json!({
                    "encoded_bytes": encoded_bytes,
                    "perfect_queries": perfect_queries,
                    "truth_hits": truth_hits,
                    "truth_total": 1280,
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let value = serde_json::json!({
        "aggregates": aggregates,
        "candidate_depth": 12_288,
        "claim_eligible": false,
        "corpus_reads": 0,
        "frontier_sha256": FRONTIER_SHA256,
        "page_reads": 0,
        "records": results,
        "row_limit": 262_144,
        "schema": "borsuk-v33-shape-pq-replay-v1",
    });
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args(std::env::args().skip(1)).map_err(std::io::Error::other)?;
    let bytes = execute(&args)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&args.output)?;
    output.write_all(&bytes)?;
    eprintln!(
        "status=complete encoded_bytes={} sha256={:x}",
        bytes.len(),
        Sha256::digest(&bytes)
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        bounded_page_prefix, canonical_result, expected_frontier_inputs, parse_args,
        parse_frontier, parse_frontier_record, truth_page_hits,
    };

    #[test]
    fn v33_shape_pq_replay_counts_truth_rows_not_distinct_truth_pages() {
        let selected = [7_u32, 9];
        assert_eq!(truth_page_hits(&[7, 7, 8, 9], &selected), 3);
    }

    #[test]
    fn v33_shape_pq_replay_preserves_a_short_candidate_page_pool() {
        assert_eq!(bounded_page_prefix(&[2_u32, 5], 64).unwrap(), &[2, 5]);
        assert!(bounded_page_prefix(&[2_u32, 5], 0).is_err());
    }

    #[test]
    fn v33_shape_pq_replay_result_is_canonical_and_claim_ineligible() {
        let bytes = canonical_result(12, 9, 0).unwrap();
        assert_eq!(
            bytes,
            b"{\"claim_eligible\":false,\"corpus_reads\":0,\"page_reads\":0,\"schema\":\"borsuk-v33-shape-pq-replay-v1\",\"truth_hits\":9}\n"
        );
    }

    #[test]
    fn v33_shape_pq_replay_frontier_is_strict_and_truth_free_until_join() {
        let mut record = serde_json::json!({
            "hits": 10,
            "query": vec![0.25_f32; 96],
            "query_ordinal": 4096,
            "selected_groups": [2, 7],
            "selected_routing_leaves": [3, 8],
            "selected_rows": 120,
            "truth_logicals": [0,1,2,3,4,5,6,7,8,9],
            "truth_owner_ranks": [1,1,1,1,1,1,1,1,1,1]
        });
        let parsed = parse_frontier_record(&record).unwrap();
        assert_eq!(parsed.query_ordinal, 4096);
        assert_eq!(parsed.selected_routing_leaves, vec![3, 8]);
        assert_eq!(parsed.truth_logicals.len(), 10);

        record["selected_routing_leaves"] = serde_json::json!([3, 3]);
        assert!(parse_frontier_record(&record).is_err());
        record["selected_routing_leaves"] = serde_json::json!([3, 8]);
        record["query"] = serde_json::json!(vec![0.25_f32; 95]);
        assert!(parse_frontier_record(&record).is_err());
    }

    #[test]
    fn v33_shape_pq_replay_cli_has_no_page_or_storage_surface() {
        assert!(parse_args(Vec::<String>::new()).is_err());
        assert!(
            parse_args(["--bucket".to_owned(), "forbidden".to_owned()])
                .unwrap_err()
                .contains("unknown flag")
        );
    }

    #[test]
    fn v33_shape_pq_replay_requires_the_versioned_diagonal_frontier() {
        let record = serde_json::json!({
            "hits": 10,
            "query": vec![0.25_f32; 96],
            "query_ordinal": 4096,
            "selected_groups": [2],
            "selected_routing_leaves": [3],
            "selected_rows": 120,
            "truth_logicals": [0,1,2,3,4,5,6,7,8,9],
            "truth_owner_ranks": [1,1,1,1,1,1,1,1,1,1]
        });
        let mut value = serde_json::json!({
            "arms": [{
                "arm": "diagonal-ellipsoid",
                "included_owners": 10,
                "maximum_selected_rows": 120,
                "minimum_selected_rows": 120,
                "passed": true,
                "perfect_queries": 1,
                "query_count": 1,
                "records": [record],
                "total_owners": 10
            }],
            "claim_eligible": false,
            "code_reads": 5,
            "corpus_reads": 0,
            "input_sha256": expected_frontier_inputs(),
            "page_reads": 0,
            "passed": false,
            "row_limit": 262144,
            "schema": "borsuk-v33-group-proxy-result-v2"
        });
        assert_eq!(parse_frontier(&value).unwrap().len(), 1);
        value["schema"] = serde_json::json!("borsuk-v33-group-proxy-result-v1");
        assert!(parse_frontier(&value).is_err());
    }
}
