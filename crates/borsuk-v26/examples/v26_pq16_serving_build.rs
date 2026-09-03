//! Offline V26 PQ16 serving-artifact construction with explicit local authority.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk_v26::{
    V26LocalObjectPath, V26ObjectIdentity, V26Pq16ServingBuildRequest,
    canonical_v26_pq16_serving_build_output_bytes, run_v26_pq16_serving_build,
};

fn take(values: &mut BTreeMap<String, String>, key: &str) -> Result<String, String> {
    values.remove(key).ok_or_else(|| format!("missing {key}"))
}

fn registered(
    values: &mut BTreeMap<String, String>,
    prefix: &str,
    role: &str,
    generation: &str,
) -> Result<V26LocalObjectPath, String> {
    let path = PathBuf::from(take(values, &format!("--{prefix}-path"))?);
    let uri = take(values, &format!("--{prefix}-uri"))?;
    let digest = take(values, &format!("--{prefix}-sha256"))?;
    let encoded_bytes = take(values, &format!("--{prefix}-bytes"))?
        .parse::<u64>()
        .map_err(|_| format!("invalid --{prefix}-bytes"))?;
    if !uri.starts_with("s3://")
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || encoded_bytes == 0
    {
        return Err(format!("invalid --{prefix} authority"));
    }
    Ok(V26LocalObjectPath {
        identity: V26ObjectIdentity {
            role: role.to_owned(),
            uri,
            digest_algorithm: "sha256".to_owned(),
            digest,
            encoded_bytes,
            generation: generation.to_owned(),
        },
        path,
    })
}

fn parse_args(args: Vec<String>) -> Result<V26Pq16ServingBuildRequest, String> {
    let mut values = BTreeMap::new();
    let mut build = false;
    let mut args = args.into_iter();
    while let Some(key) = args.next() {
        if key == "--build-pq16-serving" {
            if build {
                return Err("duplicate --build-pq16-serving".to_owned());
            }
            build = true;
            continue;
        }
        if !key.starts_with("--") {
            return Err(format!("unexpected argument {key}"));
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {key}"))?;
        if values.insert(key.clone(), value).is_some() {
            return Err(format!("duplicate {key}"));
        }
    }
    if !build {
        return Err("missing --build-pq16-serving".to_owned());
    }
    let generation = take(&mut values, "--generation")?;
    if generation.is_empty() {
        return Err("invalid --generation".to_owned());
    }
    let expected_rows = take(&mut values, "--expected-rows")?
        .parse::<u64>()
        .map_err(|_| "invalid --expected-rows".to_owned())?;
    let output_dir = PathBuf::from(take(&mut values, "--output-dir")?);
    let output_uri_prefix = take(&mut values, "--output-uri-prefix")?;
    let construction_rows = registered(
        &mut values,
        "construction",
        "construction-parquet",
        &generation,
    )?;
    let page_assignments = registered(
        &mut values,
        "page-assignments",
        "page-assignments-parquet",
        &generation,
    )?;
    let layout_terminal = registered(
        &mut values,
        "layout-terminal",
        "layout-terminal",
        &generation,
    )?;
    let primary_tree = registered(
        &mut values,
        "primary-tree",
        "primary-tree-parquet",
        &generation,
    )?;
    let replica_tree = registered(
        &mut values,
        "replica-tree",
        "replica-tree-parquet",
        &generation,
    )?;
    if !values.is_empty()
        || expected_rows == 0
        || !output_uri_prefix.starts_with("s3://")
        || !output_uri_prefix.ends_with('/')
    {
        return Err("unknown or invalid argument".to_owned());
    }
    Ok(V26Pq16ServingBuildRequest {
        construction_rows,
        page_assignments,
        layout_terminal,
        primary_tree,
        replica_tree,
        expected_rows,
        output_dir,
        output_uri_prefix,
    })
}

fn run() -> Result<(), String> {
    let request = parse_args(std::env::args().skip(1).collect())?;
    let output = run_v26_pq16_serving_build(&request).map_err(|error| error.to_string())?;
    let bytes = canonical_v26_pq16_serving_build_output_bytes(&request, &output)
        .map_err(|error| error.to_string())?;
    std::io::stdout()
        .write_all(&bytes)
        .map_err(|error| format!("stdout write failed: {error}"))
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    fn valid_args() -> Vec<String> {
        let mut args = vec![
            "--build-pq16-serving".to_owned(),
            "--generation".to_owned(),
            "v26-test".to_owned(),
            "--expected-rows".to_owned(),
            "262144".to_owned(),
            "--output-dir".to_owned(),
            "/tmp/serving".to_owned(),
            "--output-uri-prefix".to_owned(),
            "s3://v26/serving/".to_owned(),
        ];
        for role in [
            "construction",
            "page-assignments",
            "layout-terminal",
            "primary-tree",
            "replica-tree",
        ] {
            args.extend([
                format!("--{role}-path"),
                format!("/tmp/{role}"),
                format!("--{role}-uri"),
                format!("s3://v26/{role}"),
                format!("--{role}-sha256"),
                "a".repeat(64),
                format!("--{role}-bytes"),
                "1024".to_owned(),
            ]);
        }
        args
    }

    #[test]
    fn v26_pq16_serving_build_cli_requires_only_construction_authority() {
        let request = super::parse_args(valid_args()).unwrap();
        assert_eq!(request.expected_rows, 262_144);
        assert_eq!(
            request.construction_rows.identity.role,
            "construction-parquet"
        );
        assert_eq!(
            request.page_assignments.identity.role,
            "page-assignments-parquet"
        );
        assert_eq!(request.output_uri_prefix, "s3://v26/serving/");
    }

    #[test]
    fn v26_pq16_serving_build_cli_rejects_query_storage_and_ambiguous_flags() {
        for forbidden in ["--external-queries-path", "--bucket", "--endpoint", "--d3"] {
            let mut args = valid_args();
            args.extend([forbidden.to_owned(), "forbidden".to_owned()]);
            assert!(super::parse_args(args).is_err());
        }
        let mut duplicate = valid_args();
        duplicate.extend(["--expected-rows".to_owned(), "1".to_owned()]);
        assert!(super::parse_args(duplicate).is_err());
    }
}
