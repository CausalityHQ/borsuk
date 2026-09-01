//! Local-only one-pass V23 residual RaBitQ constructor.

use std::{collections::BTreeMap, env, io::Write, path::PathBuf, process::ExitCode};

use borsuk::{
    V23RaBitQConstructionLocalPaths, V23RaBitQConstructionLocalRunRequest,
    V23RaBitQLocalObjectIdentity, run_v23_rabitq_construction_local_request,
};

const ROLES: [&str; 4] = ["manifest", "tree-receipt", "incidence-tree", "page-roster"];

fn take(values: &mut BTreeMap<String, String>, flag: &str) -> Result<String, String> {
    values
        .remove(flag)
        .ok_or_else(|| format!("missing required flag {flag}"))
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
    let digest_valid = sha256.len() == 64
        && sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !uri.starts_with("s3://") || !digest_valid {
        return Err(format!("invalid {role} identity"));
    }
    Ok(V23RaBitQLocalObjectIdentity {
        role: role.to_string(),
        uri,
        sha256,
        encoded_bytes,
    })
}

fn parse_v23_rabitq_construct_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V23RaBitQConstructionLocalRunRequest, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| "program name is absent".to_string())?;
    let mut values = BTreeMap::new();
    let mut execute = false;
    while let Some(flag) = arguments.next() {
        if flag == "--execute-construction" {
            if execute {
                return Err("duplicate --execute-construction".to_string());
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
        return Err("missing required flag --execute-construction".to_string());
    }
    let mut paths = BTreeMap::new();
    for role in ROLES {
        let path = PathBuf::from(take(&mut values, &format!("--{role}"))?);
        if !path.is_absolute() {
            return Err(format!("local {role} path is not absolute"));
        }
        paths.insert(role, path);
    }
    let scratch_directory = PathBuf::from(take(&mut values, "--scratch-directory")?);
    let output_directory = PathBuf::from(take(&mut values, "--output-directory")?);
    if !scratch_directory.is_absolute()
        || !output_directory.is_absolute()
        || scratch_directory == output_directory
    {
        return Err("construction directories differ".to_string());
    }
    let mut identities = Vec::with_capacity(ROLES.len());
    for role in ROLES {
        identities.push(identity(&mut values, role)?);
    }
    if let Some(flag) = values.keys().next() {
        return Err(format!("unknown flag {flag}"));
    }
    let mut path = |role| paths.remove(role).unwrap();
    Ok(V23RaBitQConstructionLocalRunRequest {
        paths: V23RaBitQConstructionLocalPaths {
            manifest: path("manifest"),
            tree_receipt: path("tree-receipt"),
            incidence_tree: path("incidence-tree"),
            page_roster: path("page-roster"),
        },
        manifest_identity: identities.remove(0),
        registered_inputs: identities,
        scratch_directory,
        output_directory,
        execute_construction: true,
    })
}

fn run() -> Result<(), String> {
    let request = parse_v23_rabitq_construct_args(env::args())?;
    let stdin = std::io::stdin();
    let bytes = run_v23_rabitq_construction_local_request(request, stdin.lock())
        .map_err(|error| error.to_string())?;
    std::io::stdout()
        .write_all(&bytes)
        .map_err(|error| format!("failed to write construction receipt: {error}"))
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

    use super::parse_v23_rabitq_construct_args;
    use borsuk::{V23RaBitQConstructionLocalRunRequest, run_v23_rabitq_construction_local_request};

    const ROLES: [(&str, char); 4] = [
        ("manifest", '0'),
        ("tree-receipt", '1'),
        ("incidence-tree", '2'),
        ("page-roster", '3'),
    ];

    fn arguments() -> Vec<String> {
        let mut values = vec!["v23-rabitq-construct".to_string()];
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
        values.extend([
            "--scratch-directory".to_string(),
            "/scratch/rabitq".to_string(),
            "--output-directory".to_string(),
            "/output/rabitq".to_string(),
            "--execute-construction".to_string(),
        ]);
        values
    }

    #[test]
    fn v23_rabitq_construct_example_binds_exact_inputs_and_directories() {
        let request = parse_v23_rabitq_construct_args(arguments()).unwrap();
        assert_eq!(
            request.paths.incidence_tree,
            PathBuf::from("/fixtures/incidence-tree")
        );
        assert_eq!(request.registered_inputs.len(), 3);
        assert_eq!(request.scratch_directory, PathBuf::from("/scratch/rabitq"));
        assert_eq!(request.output_directory, PathBuf::from("/output/rabitq"));
        assert!(request.execute_construction);
        let typed: V23RaBitQConstructionLocalRunRequest = request;
        let _runner = run_v23_rabitq_construction_local_request::<std::io::Empty>;
        drop(typed);
    }

    #[test]
    fn v23_rabitq_construct_example_rejects_ambiguous_or_remote_surface() {
        for forbidden in [
            "--query-parquet",
            "--endpoint",
            "--bucket",
            "--page-prefix",
            "--execute-development",
            "--execute-holdout",
            "--execute-d3",
        ] {
            let mut values = arguments();
            values.extend([forbidden.to_string(), "forbidden".to_string()]);
            assert!(parse_v23_rabitq_construct_args(values).is_err());
        }
        let mut duplicate = arguments();
        duplicate.extend(["--incidence-tree".to_string(), "/other".to_string()]);
        assert!(parse_v23_rabitq_construct_args(duplicate).is_err());
        let mut relative = arguments();
        let index = relative
            .iter()
            .position(|value| value == "/scratch/rabitq")
            .unwrap();
        relative[index] = "scratch".to_string();
        assert!(parse_v23_rabitq_construct_args(relative).is_err());
    }
}
