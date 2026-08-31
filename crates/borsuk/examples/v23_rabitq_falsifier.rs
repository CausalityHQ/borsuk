//! Local-only V23 residual RaBitQ development falsifier.

use std::{collections::BTreeMap, env, io::Write, path::PathBuf, process::ExitCode};

use borsuk::{
    V23RaBitQLocalArtifactPaths, V23RaBitQLocalObjectIdentity, V23RaBitQLocalRunRequest,
    run_v23_rabitq_local_request,
};

const ROLES: [&str; 10] = [
    "manifest",
    "construction-receipt",
    "incidence-tree",
    "row-codes",
    "leaf-offsets",
    "centroids",
    "rotation",
    "f16-control",
    "d2-report",
    "query-parquet",
];

fn take(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
}

fn valid_lower_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn identity(
    values: &mut BTreeMap<String, String>,
    role: &str,
) -> Result<V23RaBitQLocalObjectIdentity, String> {
    let uri = take(values, &format!("--{role}-uri"))?;
    let sha256 = take(values, &format!("--{role}-sha256"))?;
    let encoded_bytes = take(values, &format!("--{role}-bytes"))?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("invalid --{role}-bytes"))?;
    if !uri.starts_with("s3://")
        || uri.trim_start_matches("s3://").is_empty()
        || !valid_lower_hex(&sha256)
    {
        return Err(format!("invalid {role} identity"));
    }
    Ok(V23RaBitQLocalObjectIdentity {
        role: role.to_string(),
        uri,
        sha256,
        encoded_bytes,
    })
}

fn parse_v23_rabitq_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V23RaBitQLocalRunRequest, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| "program name is absent".to_string())?;
    let mut values = BTreeMap::new();
    let mut execute = false;
    while let Some(flag) = arguments.next() {
        if flag == "--execute-development" {
            if execute {
                return Err("duplicate --execute-development".to_string());
            }
            execute = true;
            continue;
        }
        if !flag.starts_with("--") {
            return Err(format!("unexpected positional argument {flag}"));
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        if value.starts_with("--") || values.insert(flag.clone(), value).is_some() {
            return Err(format!("invalid or duplicate flag {flag}"));
        }
    }
    if !execute {
        return Err("missing required flag --execute-development".to_string());
    }

    let mut paths = BTreeMap::new();
    for role in ROLES {
        let path = PathBuf::from(take(&mut values, &format!("--{role}"))?);
        if !path.is_absolute() {
            return Err(format!("local {role} path is not absolute"));
        }
        paths.insert(role, path);
    }
    let mut identities = Vec::with_capacity(ROLES.len());
    for role in ROLES {
        identities.push(identity(&mut values, role)?);
    }
    if let Some(flag) = values.keys().next() {
        return Err(format!("unknown flag {flag}"));
    }
    let mut path = |role| paths.remove(role).unwrap();
    Ok(V23RaBitQLocalRunRequest {
        paths: V23RaBitQLocalArtifactPaths {
            manifest: path("manifest"),
            construction_receipt: path("construction-receipt"),
            incidence_tree: path("incidence-tree"),
            row_codes: path("row-codes"),
            leaf_offsets: path("leaf-offsets"),
            centroids: path("centroids"),
            rotation: path("rotation"),
            f16_control: path("f16-control"),
            d2_report: path("d2-report"),
            query_parquet: path("query-parquet"),
        },
        manifest_identity: identities.remove(0),
        registered_inputs: identities,
        execute_development: true,
    })
}

fn run() -> Result<(), String> {
    let request = parse_v23_rabitq_args(env::args())?;
    let bytes = run_v23_rabitq_local_request(request).map_err(|error| error.to_string())?;
    std::io::stdout()
        .write_all(&bytes)
        .map_err(|error| format!("failed to write canonical result: {error}"))
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::parse_v23_rabitq_args;
    use borsuk::{V23RaBitQLocalRunRequest, run_v23_rabitq_local_request};

    const ROLES: [(&str, char); 10] = [
        ("manifest", '0'),
        ("construction-receipt", '1'),
        ("incidence-tree", '2'),
        ("row-codes", '3'),
        ("leaf-offsets", '4'),
        ("centroids", '5'),
        ("rotation", '6'),
        ("f16-control", '7'),
        ("d2-report", '8'),
        ("query-parquet", '9'),
    ];

    fn arguments() -> Vec<String> {
        let mut values = vec!["v23-rabitq-falsifier".to_string()];
        for (role, marker) in ROLES {
            values.extend([
                format!("--{role}"),
                format!("/fixtures/{role}"),
                format!("--{role}-uri"),
                format!("s3://frozen-v23/{role}"),
                format!("--{role}-sha256"),
                marker.to_string().repeat(64),
                format!("--{role}-bytes"),
                "4096".to_string(),
            ]);
        }
        values.push("--execute-development".to_string());
        values
    }

    #[test]
    fn v23_rabitq_example_binds_exact_local_files_and_registered_identities() {
        let request = parse_v23_rabitq_args(arguments()).unwrap();
        assert_eq!(
            request.paths.row_codes,
            PathBuf::from("/fixtures/row-codes")
        );
        assert_eq!(request.registered_inputs.len(), 9);
        assert_eq!(request.manifest_identity.role, "manifest");
        assert!(request.execute_development);

        let typed: V23RaBitQLocalRunRequest = request;
        let _runner = run_v23_rabitq_local_request;
        drop(typed);
    }

    #[test]
    fn v23_rabitq_example_rejects_missing_duplicate_unknown_and_relative_values() {
        let mut missing = arguments();
        missing.retain(|value| value != "--execute-development");
        assert!(parse_v23_rabitq_args(missing).is_err());

        let mut duplicate = arguments();
        duplicate.extend(["--row-codes".to_string(), "/other".to_string()]);
        assert!(parse_v23_rabitq_args(duplicate).is_err());

        let mut unknown = arguments();
        unknown.extend(["--mystery".to_string(), "value".to_string()]);
        assert!(parse_v23_rabitq_args(unknown).is_err());

        let mut relative = arguments();
        let index = relative
            .iter()
            .position(|value| value == "/fixtures/query-parquet")
            .unwrap();
        relative[index] = "query.parquet".to_string();
        assert!(parse_v23_rabitq_args(relative).is_err());

        let mut invalid_bytes = arguments();
        let index = invalid_bytes
            .iter()
            .position(|value| value == "--row-codes-bytes")
            .unwrap();
        invalid_bytes[index + 1] = "0".to_string();
        assert!(parse_v23_rabitq_args(invalid_bytes).is_err());
    }

    #[test]
    fn v23_rabitq_example_has_no_network_storage_holdout_or_d3_surface() {
        for forbidden in [
            "--bucket",
            "--endpoint",
            "--page-prefix",
            "--storage-uri",
            "--execute-holdout",
            "--execute-d3",
        ] {
            let mut values = arguments();
            values.extend([forbidden.to_string(), "forbidden".to_string()]);
            assert!(
                parse_v23_rabitq_args(values).is_err(),
                "accepted forbidden flag {forbidden}"
            );
        }
    }
}
