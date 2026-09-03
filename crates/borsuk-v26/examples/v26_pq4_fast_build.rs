//! Offline, query-independent V26 PQ4 fast-scan artifact builder.

use std::{collections::BTreeMap, io::Write, path::PathBuf};

use borsuk_v26::{
    V26LocalObjectPath, V26ObjectIdentity, V26Pq4FastBuildRequest,
    canonical_v26_pq4_fast_manifest_bytes, run_v26_pq4_fast_build,
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

fn parse_args(args: Vec<String>) -> Result<V26Pq4FastBuildRequest, String> {
    let mut values = BTreeMap::new();
    let mut execute = false;
    let mut args = args.into_iter();
    while let Some(key) = args.next() {
        if key == "--execute-pq4-fast-build" {
            if execute {
                return Err("duplicate --execute-pq4-fast-build".to_owned());
            }
            execute = true;
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
    if !execute {
        return Err("missing --execute-pq4-fast-build".to_owned());
    }
    let generation = take(&mut values, "--generation")?;
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
    if generation.is_empty()
        || expected_rows == 0
        || !output_uri_prefix.starts_with("s3://")
        || !output_uri_prefix.ends_with('/')
        || !values.is_empty()
    {
        return Err("unknown or invalid argument".to_owned());
    }
    Ok(V26Pq4FastBuildRequest {
        construction_rows,
        page_assignments,
        layout_terminal,
        expected_rows,
        output_dir,
        output_uri_prefix,
    })
}

fn run() -> Result<(), String> {
    let request = parse_args(std::env::args().skip(1).collect())?;
    let manifest = run_v26_pq4_fast_build(&request).map_err(|error| error.to_string())?;
    let bytes =
        canonical_v26_pq4_fast_manifest_bytes(&manifest).map_err(|error| error.to_string())?;
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
            "--execute-pq4-fast-build".to_owned(),
            "--generation".to_owned(),
            "v26-pq4-a0001".to_owned(),
            "--expected-rows".to_owned(),
            "1024".to_owned(),
            "--output-dir".to_owned(),
            "/tmp/v26-pq4-fast".to_owned(),
            "--output-uri-prefix".to_owned(),
            "s3://v26/pq4-fast/".to_owned(),
        ];
        for role in ["construction", "page-assignments", "layout-terminal"] {
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
    fn v26_pq4_fast_build_cli_requires_explicit_local_authority() {
        // Break caught: the builder derives an object identity, accepts implicit execution, or
        // exposes a query/truth/page/network capability in its request surface.
        let request = super::parse_args(valid_args()).unwrap();
        assert_eq!(request.expected_rows, 1_024);
        assert_eq!(
            request.construction_rows.identity.role,
            "construction-parquet"
        );
        assert_eq!(
            request.page_assignments.identity.role,
            "page-assignments-parquet"
        );
        assert_eq!(request.layout_terminal.identity.role, "layout-terminal");
        assert_eq!(request.output_uri_prefix, "s3://v26/pq4-fast/");

        let forbidden = [
            "--query-path",
            "--truth-uri",
            "--page-prefix",
            "--bucket",
            "--endpoint",
            "--aws-region",
            "--execute-d3",
        ];
        for flag in forbidden {
            let mut args = valid_args();
            args.extend([flag.to_owned(), "forbidden".to_owned()]);
            assert!(super::parse_args(args).is_err(), "accepted {flag}");
        }
    }

    #[test]
    fn v26_pq4_fast_build_cli_rejects_missing_duplicate_unknown_and_invalid_values() {
        // Break caught: parser last-write-wins, silently ignores unknowns, or accepts malformed
        // SHA/URI/length/count authority before any artifact is opened.
        let mut missing_execute = valid_args();
        missing_execute.remove(0);
        assert!(super::parse_args(missing_execute).is_err());

        let mut duplicate = valid_args();
        duplicate.extend(["--expected-rows".to_owned(), "1024".to_owned()]);
        assert!(super::parse_args(duplicate).is_err());

        let mut invalid_hash = valid_args();
        let index = invalid_hash
            .iter()
            .position(|value| value == "--construction-sha256")
            .unwrap();
        invalid_hash[index + 1] = "A".repeat(64);
        assert!(super::parse_args(invalid_hash).is_err());

        let mut zero_rows = valid_args();
        let index = zero_rows
            .iter()
            .position(|value| value == "--expected-rows")
            .unwrap();
        zero_rows[index + 1] = "0".to_owned();
        assert!(super::parse_args(zero_rows).is_err());

        let mut unknown = valid_args();
        unknown.extend(["--mystery".to_owned(), "value".to_owned()]);
        assert!(super::parse_args(unknown).is_err());
    }
}
