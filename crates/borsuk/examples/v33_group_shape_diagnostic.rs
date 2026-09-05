//! Frozen local-only V33 shape diagnostic; no corpus or page-body interface.

use std::{collections::BTreeMap, env, fs, path::PathBuf};

use borsuk::{
    V27HierarchyArtifactIdentity, V27HierarchyArtifacts, V30LayoutArtifactIdentity,
    V30LayoutArtifacts, V30PqArtifactIdentity, V30PqArtifacts, V33FullCovarianceCeilingRequest,
    V33GroupShapeBuildRequest, V33ReconstructedOracleRequest, build_v33_full_covariance_ceiling,
    build_v33_group_shape_artifact, build_v33_reconstructed_group_oracle,
    canonical_v33_full_covariance_ceiling_result_bytes,
    canonical_v33_reconstructed_oracle_result_bytes,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

const GROUP_DIRECTORY_SHA256: &str =
    "1cd77b268304bc4d36acf9f4beb402ccabc3ec0b1ebde316d2dd7f3a2cdcc995";
const FRONTIER_SHA256: &str = "470f7c95a965572feec11cd1b0d24e73bf1d8c1456a75117b8bf6796e091db6b";
const FRONTIER_BYTES: u64 = 5_937_815;
const ORACLE_QUERY_ORDINAL: usize = 6_160;

#[derive(Debug, Clone, PartialEq, Eq)]
enum RunMode {
    GroupShape,
    ReconstructedOracle,
    FullCovarianceCeiling,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    group_directory: PathBuf,
    frontier: Option<PathBuf>,
    output: PathBuf,
    mode: RunMode,
}

#[derive(Debug, Clone, PartialEq)]
struct ReconstructedOracleFrontier {
    query_ordinal: usize,
    query: [f32; 96],
    truth_logicals: [u64; 10],
}

#[derive(Debug, Clone, PartialEq)]
struct FullCovarianceFrontier {
    query_ordinals: Vec<u64>,
    queries: Vec<[f32; 96]>,
    truth_logicals: Vec<Vec<u64>>,
}

fn exact_keys(value: &serde_json::Map<String, Value>, expected: &[&str]) -> bool {
    value
        .keys()
        .map(String::as_str)
        .eq(expected.iter().copied())
}

fn parse_reconstructed_oracle_frontier(
    raw: &[u8],
) -> Result<ReconstructedOracleFrontier, Box<dyn std::error::Error>> {
    if raw.last() != Some(&b'\n') {
        return Err("V33 oracle frontier newline differs".into());
    }
    let value: Value = serde_json::from_slice(raw)?;
    let root = value
        .as_object()
        .ok_or("V33 oracle frontier schema differs")?;
    if !exact_keys(
        root,
        &[
            "arms",
            "claim_eligible",
            "code_reads",
            "corpus_reads",
            "input_sha256",
            "page_reads",
            "passed",
            "row_limit",
            "schema",
        ],
    ) || root.get("schema").and_then(Value::as_str) != Some("borsuk-v33-group-proxy-result-v2")
        || root.get("claim_eligible").and_then(Value::as_bool) != Some(false)
        || root.get("code_reads").and_then(Value::as_u64) != Some(5)
        || root.get("corpus_reads").and_then(Value::as_u64) != Some(0)
        || root.get("page_reads").and_then(Value::as_u64) != Some(0)
        || root.get("row_limit").and_then(Value::as_u64) != Some(262_144)
    {
        return Err("V33 oracle frontier authority differs".into());
    }
    let arms = root
        .get("arms")
        .and_then(Value::as_array)
        .ok_or("V33 oracle frontier arms differ")?;
    let mut matching = arms.iter().filter_map(|arm| {
        let object = arm.as_object()?;
        (object.get("arm")?.as_str()? == "diagonal-ellipsoid").then_some(object)
    });
    let arm = matching.next().ok_or("V33 oracle diagonal arm differs")?;
    if matching.next().is_some()
        || !exact_keys(
            arm,
            &[
                "arm",
                "included_owners",
                "maximum_selected_rows",
                "minimum_selected_rows",
                "passed",
                "perfect_queries",
                "query_count",
                "records",
                "total_owners",
            ],
        )
    {
        return Err("V33 oracle diagonal arm schema differs".into());
    }
    let records = arm
        .get("records")
        .and_then(Value::as_array)
        .ok_or("V33 oracle records differ")?;
    let mut matching = records.iter().filter_map(|record| {
        let object = record.as_object()?;
        (object.get("query_ordinal")?.as_u64()? == ORACLE_QUERY_ORDINAL as u64).then_some(object)
    });
    let record = matching.next().ok_or("V33 oracle query differs")?;
    if matching.next().is_some()
        || !exact_keys(
            record,
            &[
                "hits",
                "query",
                "query_ordinal",
                "selected_groups",
                "selected_routing_leaves",
                "selected_rows",
                "truth_logicals",
                "truth_owner_ranks",
            ],
        )
    {
        return Err("V33 oracle query schema differs".into());
    }
    let query = record
        .get("query")
        .and_then(Value::as_array)
        .ok_or("V33 oracle query vector differs")?;
    if query.len() != 96 {
        return Err("V33 oracle query dimension differs".into());
    }
    let query: [f32; 96] = query
        .iter()
        .map(|value| {
            let value = value.as_f64().ok_or("V33 oracle query value differs")? as f32;
            value
                .is_finite()
                .then_some(value)
                .ok_or("V33 oracle query value differs")
        })
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| "V33 oracle query dimension differs")?;
    let logicals = record
        .get("truth_logicals")
        .and_then(Value::as_array)
        .ok_or("V33 oracle truth logicals differ")?;
    if logicals.len() != 10 {
        return Err("V33 oracle truth count differs".into());
    }
    let truth_logicals: [u64; 10] = logicals
        .iter()
        .map(|value| value.as_u64().ok_or("V33 oracle truth logical differs"))
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| "V33 oracle truth count differs")?;
    if truth_logicals
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != 10
    {
        return Err("V33 oracle truth logicals differ".into());
    }
    Ok(ReconstructedOracleFrontier {
        query_ordinal: ORACLE_QUERY_ORDINAL,
        query,
        truth_logicals,
    })
}

fn parse_full_covariance_frontier(
    raw: &[u8],
) -> Result<FullCovarianceFrontier, Box<dyn std::error::Error>> {
    if raw.last() != Some(&b'\n') {
        return Err("V33 full covariance frontier newline differs".into());
    }
    let value: Value = serde_json::from_slice(raw)?;
    let root = value
        .as_object()
        .ok_or("V33 full covariance frontier schema differs")?;
    if !exact_keys(
        root,
        &[
            "arms",
            "claim_eligible",
            "code_reads",
            "corpus_reads",
            "input_sha256",
            "page_reads",
            "passed",
            "row_limit",
            "schema",
        ],
    ) || root.get("schema").and_then(Value::as_str) != Some("borsuk-v33-group-proxy-result-v2")
        || root.get("claim_eligible").and_then(Value::as_bool) != Some(false)
        || root.get("code_reads").and_then(Value::as_u64) != Some(5)
        || root.get("corpus_reads").and_then(Value::as_u64) != Some(0)
        || root.get("page_reads").and_then(Value::as_u64) != Some(0)
        || root.get("row_limit").and_then(Value::as_u64) != Some(262_144)
    {
        return Err("V33 full covariance frontier authority differs".into());
    }
    let arms = root
        .get("arms")
        .and_then(Value::as_array)
        .ok_or("V33 full covariance frontier arms differ")?;
    let mut matching = arms.iter().filter_map(|arm| {
        let object = arm.as_object()?;
        (object.get("arm")?.as_str()? == "diagonal-ellipsoid").then_some(object)
    });
    let arm = matching
        .next()
        .ok_or("V33 full covariance diagonal arm differs")?;
    if matching.next().is_some()
        || !exact_keys(
            arm,
            &[
                "arm",
                "included_owners",
                "maximum_selected_rows",
                "minimum_selected_rows",
                "passed",
                "perfect_queries",
                "query_count",
                "records",
                "total_owners",
            ],
        )
    {
        return Err("V33 full covariance diagonal arm schema differs".into());
    }
    let records = arm
        .get("records")
        .and_then(Value::as_array)
        .filter(|records| !records.is_empty())
        .ok_or("V33 full covariance records differ")?;
    let query_count = arm
        .get("query_count")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or("V33 full covariance query count differs")?;
    if query_count != records.len()
        || arm.get("total_owners").and_then(Value::as_u64) != Some((query_count * 10) as u64)
        || arm.get("included_owners").and_then(Value::as_u64).is_none()
        || arm.get("perfect_queries").and_then(Value::as_u64).is_none()
        || arm
            .get("minimum_selected_rows")
            .and_then(Value::as_u64)
            .is_none()
        || arm
            .get("maximum_selected_rows")
            .and_then(Value::as_u64)
            .is_none()
        || arm.get("passed").and_then(Value::as_bool).is_none()
    {
        return Err("V33 full covariance aggregate schema differs".into());
    }

    let mut query_ordinals = Vec::with_capacity(query_count);
    let mut queries = Vec::with_capacity(query_count);
    let mut truth_logicals = Vec::with_capacity(query_count);
    for record in records {
        let record = record
            .as_object()
            .ok_or("V33 full covariance record schema differs")?;
        if !exact_keys(
            record,
            &[
                "hits",
                "query",
                "query_ordinal",
                "selected_groups",
                "selected_routing_leaves",
                "selected_rows",
                "truth_logicals",
                "truth_owner_ranks",
            ],
        ) || record
            .get("hits")
            .and_then(Value::as_u64)
            .filter(|hits| *hits <= 10)
            .is_none()
            || record
                .get("selected_rows")
                .and_then(Value::as_u64)
                .filter(|rows| *rows > 0)
                .is_none()
            || record
                .get("selected_groups")
                .and_then(Value::as_array)
                .filter(|v| !v.is_empty())
                .is_none()
            || record
                .get("selected_routing_leaves")
                .and_then(Value::as_array)
                .filter(|v| !v.is_empty())
                .is_none()
            || record
                .get("truth_owner_ranks")
                .and_then(Value::as_array)
                .filter(|v| v.len() == 10)
                .is_none()
        {
            return Err("V33 full covariance record authority differs".into());
        }
        let query_ordinal = record
            .get("query_ordinal")
            .and_then(Value::as_u64)
            .ok_or("V33 full covariance query ordinal differs")?;
        if query_ordinals
            .last()
            .is_some_and(|previous| *previous >= query_ordinal)
        {
            return Err("V33 full covariance query order differs".into());
        }
        let query = record
            .get("query")
            .and_then(Value::as_array)
            .filter(|values| values.len() == 96)
            .ok_or("V33 full covariance query vector differs")?
            .iter()
            .map(|value| {
                let value = value
                    .as_f64()
                    .ok_or("V33 full covariance query value differs")?
                    as f32;
                value
                    .is_finite()
                    .then_some(value)
                    .ok_or("V33 full covariance query value differs")
            })
            .collect::<Result<Vec<_>, _>>()?
            .try_into()
            .map_err(|_| "V33 full covariance query dimension differs")?;
        let truth = record
            .get("truth_logicals")
            .and_then(Value::as_array)
            .filter(|values| values.len() == 10)
            .ok_or("V33 full covariance truth count differs")?
            .iter()
            .map(|value| {
                value
                    .as_u64()
                    .ok_or("V33 full covariance truth logical differs")
            })
            .collect::<Result<Vec<_>, _>>()?;
        if truth
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != 10
        {
            return Err("V33 full covariance truth logicals differ".into());
        }
        query_ordinals.push(query_ordinal);
        queries.push(query);
        truth_logicals.push(truth);
    }
    Ok(FullCovarianceFrontier {
        query_ordinals,
        queries,
        truth_logicals,
    })
}

fn parse_args(values: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut values = values.into_iter();
    let mut paths = BTreeMap::new();
    let mut mode = None;
    while let Some(flag) = values.next() {
        if matches!(
            flag.as_str(),
            "--execute-group-shape"
                | "--execute-reconstructed-oracle"
                | "--execute-full-covariance-ceiling"
        ) {
            let next = match flag.as_str() {
                "--execute-group-shape" => RunMode::GroupShape,
                "--execute-reconstructed-oracle" => RunMode::ReconstructedOracle,
                "--execute-full-covariance-ceiling" => RunMode::FullCovarianceCeiling,
                _ => unreachable!(),
            };
            if mode.replace(next).is_some() {
                return Err("duplicate execution mode".to_owned());
            }
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
                | "--group-directory"
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
    let mode = mode.ok_or("an explicit execution mode is required")?;
    let frontier = paths.remove("--frontier");
    let mut take = |flag: &str| {
        paths
            .remove(flag)
            .ok_or_else(|| format!("{flag} is required"))
    };
    if (mode == RunMode::GroupShape && frontier.is_some())
        || (mode != RunMode::GroupShape && frontier.is_none())
    {
        return Err("frontier authority differs for execution mode".to_owned());
    }
    Ok(Args {
        roots: take("--roots")?,
        leaves: take("--leaves")?,
        routing_ranges: take("--routing-ranges")?,
        page_ranges: take("--page-ranges")?,
        pq24_codebook: take("--pq24-codebook")?,
        pq48_codebook: take("--pq48-codebook")?,
        pq_base_codes: take("--pq-base-codes")?,
        pq_fidelity: take("--pq-fidelity")?,
        pq_high_codes: take("--pq-high-codes")?,
        group_directory: take("--group-directory")?,
        frontier,
        output: take("--output")?,
        mode,
    })
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

fn group_authority(bytes: &[u8]) -> Result<(Vec<u32>, Vec<u64>), Box<dyn std::error::Error>> {
    if format!("{:x}", Sha256::digest(bytes)) != GROUP_DIRECTORY_SHA256 {
        return Err("V33 group directory byte authority differs".into());
    }
    let value: Value = serde_json::from_slice(bytes)?;
    let groups = value
        .get("groups")
        .and_then(Value::as_array)
        .ok_or("V33 group directory schema differs")?;
    if groups.len() != 178 {
        return Err("V33 group directory count differs".into());
    }
    let mut mapping = vec![u32::MAX; 4_096];
    let mut rows = Vec::with_capacity(groups.len());
    for (group, value) in groups.iter().enumerate() {
        let group_rows = value
            .get("rows")
            .and_then(Value::as_u64)
            .filter(|rows| *rows > 0)
            .ok_or("V33 group row population differs")?;
        rows.push(group_rows);
        for parent in value
            .get("parents")
            .and_then(Value::as_array)
            .ok_or("V33 group parent schema differs")?
        {
            let parent =
                usize::try_from(parent.as_u64().ok_or("V33 group parent ordinal differs")?)?;
            if parent >= mapping.len() || mapping[parent] != u32::MAX {
                return Err("V33 group parent coverage differs".into());
            }
            mapping[parent] = u32::try_from(group)?;
        }
    }
    Ok((mapping, rows))
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let base_sha = "aa3bef1eefb6ef4670a8c6f73d48941116460d28ae6a552755a38f3776ffe8a8";
    let high_sha = "e2b92db4f8cdb5ab2b352ed491a5b00a47ea94431552b8d7fbf7764f4a29110c";
    let group_bytes = read(&args.group_directory)?;
    let (group_of_code_parent, group_rows) = group_authority(&group_bytes)?;
    let request = V33GroupShapeBuildRequest {
        hierarchy: V27HierarchyArtifacts {
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
        },
        layout: V30LayoutArtifacts {
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
        },
        pq: V30PqArtifacts {
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
        },
        group_of_code_parent,
        scalar_split_count: 43,
    };
    match args.mode {
        RunMode::GroupShape => {
            let artifact = build_v33_group_shape_artifact(&request)?;
            fs::write(&args.output, &artifact.arrow)?;
            eprintln!(
                "role={} rows={} bytes={} sha256={}",
                artifact.role, artifact.row_count, artifact.encoded_bytes, artifact.sha256
            );
        }
        RunMode::ReconstructedOracle => {
            let frontier = read(args.frontier.as_ref().ok_or("V33 frontier is required")?)?;
            if frontier.len() as u64 != FRONTIER_BYTES
                || format!("{:x}", Sha256::digest(&frontier)) != FRONTIER_SHA256
            {
                return Err("V33 oracle frontier byte authority differs".into());
            }
            let oracle = build_v33_reconstructed_group_oracle(&request)?;
            let evidence = parse_reconstructed_oracle_frontier(&frontier)?;
            let bytes = canonical_v33_reconstructed_oracle_result_bytes(
                &oracle,
                &V33ReconstructedOracleRequest {
                    frontier_sha256: FRONTIER_SHA256.to_owned(),
                    frontier_bytes: FRONTIER_BYTES,
                    query_ordinal: evidence.query_ordinal as u64,
                    query: evidence.query,
                    truth_logicals: evidence.truth_logicals.to_vec(),
                    group_rows,
                    row_limit: 262_144,
                    group_limit: 64,
                },
            )?;
            fs::write(&args.output, bytes)?;
        }
        RunMode::FullCovarianceCeiling => {
            let frontier = read(args.frontier.as_ref().ok_or("V33 frontier is required")?)?;
            if frontier.len() as u64 != FRONTIER_BYTES
                || format!("{:x}", Sha256::digest(&frontier)) != FRONTIER_SHA256
            {
                return Err("V33 full covariance frontier byte authority differs".into());
            }
            let ceiling = build_v33_full_covariance_ceiling(&request)?;
            let evidence = parse_full_covariance_frontier(&frontier)?;
            let bytes = canonical_v33_full_covariance_ceiling_result_bytes(
                &ceiling,
                &V33FullCovarianceCeilingRequest {
                    frontier_sha256: FRONTIER_SHA256.to_owned(),
                    frontier_bytes: FRONTIER_BYTES,
                    query_ordinals: evidence.query_ordinals,
                    queries: evidence.queries,
                    truth_logicals: evidence.truth_logicals,
                    group_rows,
                    row_limit: 262_144,
                    group_limit: 64,
                },
            )?;
            fs::write(&args.output, bytes)?;
        }
    }
    Ok(())
}

fn main() {
    let result = parse_args(env::args().skip(1))
        .map_err(Into::into)
        .and_then(run);
    if let Err(error) = result {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RunMode, parse_args, parse_full_covariance_frontier, parse_reconstructed_oracle_frontier,
    };

    fn valid_args() -> Vec<String> {
        [
            "--roots",
            "roots.arrow",
            "--leaves",
            "leaves.arrow",
            "--routing-ranges",
            "routing-ranges.arrow",
            "--page-ranges",
            "page-ranges.parquet",
            "--pq24-codebook",
            "pq24-codebook.arrow",
            "--pq48-codebook",
            "pq48-codebook.arrow",
            "--pq-base-codes",
            "pq-base-codes.arrow",
            "--pq-fidelity",
            "pq-fidelity.arrow",
            "--pq-high-codes",
            "pq-high-codes.arrow",
            "--group-directory",
            "groups.json",
            "--output",
            "shapes.arrow",
            "--execute-group-shape",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn v33_group_shape_cli_requires_exact_local_roles_and_explicit_execution() {
        let parsed = parse_args(valid_args()).unwrap();
        assert_eq!(parsed.output.to_string_lossy(), "shapes.arrow");
        let mut missing = valid_args();
        missing.drain(0..2);
        assert!(parse_args(missing).is_err());
        let mut duplicate = valid_args();
        duplicate.extend(["--roots".to_owned(), "other.arrow".to_owned()]);
        assert!(parse_args(duplicate).is_err());
        let mut forbidden = valid_args();
        forbidden.extend(["--bucket".to_owned(), "forbidden".to_owned()]);
        assert!(parse_args(forbidden).is_err());
    }

    #[test]
    fn v33_group_shape_cli_isolates_reconstructed_oracle_mode() {
        // Break caught: the exact-row bracket either reconstructs twice or
        // opens query evidence before its immutable population exists.
        let mut values = valid_args();
        values.pop();
        values.extend([
            "--frontier".to_owned(),
            "frontier.json".to_owned(),
            "--execute-reconstructed-oracle".to_owned(),
        ]);
        let parsed = parse_args(values).unwrap();
        assert_eq!(parsed.mode, RunMode::ReconstructedOracle);
        assert_eq!(parsed.frontier.unwrap().to_string_lossy(), "frontier.json");

        let mut both = valid_args();
        both.push("--execute-reconstructed-oracle".to_owned());
        both.extend(["--frontier".to_owned(), "frontier.json".to_owned()]);
        assert!(parse_args(both).is_err());
    }

    #[test]
    fn v33_group_shape_reconstructed_oracle_frontier_is_strict_and_finite() {
        let mut query = vec![0.0; 96];
        query[0] = 1.0;
        let value = serde_json::json!({
            "arms": [{
                "arm": "diagonal-ellipsoid",
                "included_owners": 9,
                "maximum_selected_rows": 1,
                "minimum_selected_rows": 1,
                "passed": false,
                "perfect_queries": 0,
                "query_count": 1,
                "records": [{
                    "hits": 9,
                    "query": query,
                    "query_ordinal": 6160,
                    "selected_groups": [0],
                    "selected_routing_leaves": [0],
                    "selected_rows": 1,
                    "truth_logicals": [0,1,2,3,4,5,6,7,8,9],
                    "truth_owner_ranks": [1,1,1,1,1,1,1,1,1,2]
                }],
                "total_owners": 10
            }],
            "claim_eligible": false,
            "code_reads": 5,
            "corpus_reads": 0,
            "input_sha256": {},
            "page_reads": 0,
            "passed": false,
            "row_limit": 262144,
            "schema": "borsuk-v33-group-proxy-result-v2"
        });
        let mut raw = serde_json::to_vec(&value).unwrap();
        raw.push(b'\n');
        let parsed = parse_reconstructed_oracle_frontier(&raw).unwrap();
        assert_eq!(parsed.query_ordinal, 6160);
        assert_eq!(parsed.query[0], 1.0);
        assert_eq!(parsed.truth_logicals, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        let mut invalid = raw;
        let offset = invalid
            .windows(5)
            .position(|window| window == b"1.0,0")
            .unwrap();
        invalid.splice(offset..offset + 3, b"NaN".iter().copied());
        assert!(parse_reconstructed_oracle_frontier(&invalid).is_err());
    }

    #[test]
    fn v33_group_shape_full_covariance_mode_consumes_every_frontier_query() {
        // Break caught: the ceiling reuses only the singled-out oracle query or
        // is allowed to execute alongside shape construction.
        let mut values = valid_args();
        values.pop();
        values.extend([
            "--frontier".to_owned(),
            "frontier.json".to_owned(),
            "--execute-full-covariance-ceiling".to_owned(),
        ]);
        let parsed = parse_args(values).unwrap();
        assert_eq!(parsed.mode, RunMode::FullCovarianceCeiling);

        let records = [4_096_u64, 5_120_u64]
            .into_iter()
            .map(|query_ordinal| {
                serde_json::json!({
                    "hits": 10,
                    "query": vec![0.0; 96],
                    "query_ordinal": query_ordinal,
                    "selected_groups": [0],
                    "selected_routing_leaves": [0],
                    "selected_rows": 1,
                    "truth_logicals": [0,1,2,3,4,5,6,7,8,9],
                    "truth_owner_ranks": [1,1,1,1,1,1,1,1,1,1]
                })
            })
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "arms": [{
                "arm": "diagonal-ellipsoid",
                "included_owners": 20,
                "maximum_selected_rows": 1,
                "minimum_selected_rows": 1,
                "passed": true,
                "perfect_queries": 2,
                "query_count": 2,
                "records": records,
                "total_owners": 20
            }],
            "claim_eligible": false,
            "code_reads": 5,
            "corpus_reads": 0,
            "input_sha256": {},
            "page_reads": 0,
            "passed": false,
            "row_limit": 262144,
            "schema": "borsuk-v33-group-proxy-result-v2"
        });
        let mut raw = serde_json::to_vec(&value).unwrap();
        raw.push(b'\n');
        let frontier = parse_full_covariance_frontier(&raw).unwrap();
        assert_eq!(frontier.query_ordinals, [4_096, 5_120]);
        assert_eq!(frontier.queries.len(), 2);
        assert_eq!(
            frontier.truth_logicals,
            vec![vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]; 2]
        );
    }
}
