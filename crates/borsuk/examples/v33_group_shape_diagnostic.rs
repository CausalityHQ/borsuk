//! Frozen local-only V33 shape diagnostic; no corpus or page-body interface.

use std::{collections::BTreeMap, env, fs, path::PathBuf};

use borsuk::{
    V27HierarchyArtifactIdentity, V27HierarchyArtifacts, V30LayoutArtifactIdentity,
    V30LayoutArtifacts, V30PqArtifactIdentity, V30PqArtifacts, V33GroupShapeBuildRequest,
    build_v33_group_shape_artifact,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

const GROUP_DIRECTORY_SHA256: &str =
    "1cd77b268304bc4d36acf9f4beb402ccabc3ec0b1ebde316d2dd7f3a2cdcc995";

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
    output: PathBuf,
}

fn parse_args(values: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut values = values.into_iter();
    let mut paths = BTreeMap::new();
    let mut execute = false;
    while let Some(flag) = values.next() {
        if flag == "--execute-group-shape" {
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
                | "--group-directory"
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
        return Err("--execute-group-shape is required".to_owned());
    }
    let mut take = |flag: &str| {
        paths
            .remove(flag)
            .ok_or_else(|| format!("{flag} is required"))
    };
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
        output: take("--output")?,
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

fn group_map(bytes: &[u8]) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
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
    for (group, value) in groups.iter().enumerate() {
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
    Ok(mapping)
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let base_sha = "aa3bef1eefb6ef4670a8c6f73d48941116460d28ae6a552755a38f3776ffe8a8";
    let high_sha = "e2b92db4f8cdb5ab2b352ed491a5b00a47ea94431552b8d7fbf7764f4a29110c";
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
        group_of_code_parent: group_map(&read(&args.group_directory)?)?,
        scalar_split_count: 43,
    };
    let artifact = build_v33_group_shape_artifact(&request)?;
    fs::write(&args.output, &artifact.arrow)?;
    eprintln!(
        "role={} rows={} bytes={} sha256={}",
        artifact.role, artifact.row_count, artifact.encoded_bytes, artifact.sha256
    );
    Ok(())
}

#[cfg(not(test))]
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
    use super::parse_args;

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
}
