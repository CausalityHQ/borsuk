//! Local-only executable boundary for the claim-ineligible V23 incidence falsifier.

use borsuk::{
    V23IncidenceLocalDirectoryPhaseRequest, V23IncidenceLocalRolePath, V23IncidenceObjectIdentity,
    V23IncidencePhase, V23IncidenceRunMode, run_v23_incidence_local_directory_phase,
};
use std::path::PathBuf;

fn lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn take_value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires one value"))
}

fn take_named_value(
    arguments: &mut impl Iterator<Item = String>,
    expected: &str,
) -> Result<String, String> {
    let flag = arguments
        .next()
        .ok_or_else(|| format!("{expected} is absent"))?;
    if flag != expected {
        return Err(format!("expected {expected}, got {flag}"));
    }
    take_value(arguments, expected)
}

fn set_once<T>(slot: &mut Option<T>, value: T, flag: &str) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("{flag} is duplicated"));
    }
    Ok(())
}

fn gate(flag: &str) -> Option<V23IncidenceRunMode> {
    let (execute, phase) = if let Some(phase) = flag.strip_prefix("--preflight-") {
        (false, phase)
    } else {
        (true, flag.strip_prefix("--execute-")?)
    };
    let phase = match phase {
        "tree-training" => V23IncidencePhase::TreeTraining,
        "posting-construction" => V23IncidencePhase::PostingConstruction,
        "development-evaluation" => V23IncidencePhase::DevelopmentEvaluation,
        "holdout-binding" => V23IncidencePhase::HoldoutBinding,
        "holdout-evaluation" => V23IncidencePhase::HoldoutEvaluation,
        _ => return None,
    };
    Some(if execute {
        V23IncidenceRunMode::Execute(phase)
    } else {
        V23IncidenceRunMode::Preflight(phase)
    })
}

fn take_sha256_role_path(
    arguments: &mut impl Iterator<Item = String>,
    role: &str,
    path: PathBuf,
    prefix: &str,
) -> Result<V23IncidenceLocalRolePath, String> {
    let uri = take_named_value(arguments, &format!("--{prefix}-uri"))?;
    let digest = take_named_value(arguments, &format!("--{prefix}-sha256"))?;
    let encoded_bytes = take_named_value(arguments, &format!("--{prefix}-bytes"))?
        .parse::<u64>()
        .map_err(|_| format!("--{prefix}-bytes differs"))?;
    let generation = take_named_value(arguments, &format!("--{prefix}-generation"))?;
    if !path.is_absolute()
        || uri.is_empty()
        || !lower_hex_digest(&digest)
        || encoded_bytes == 0
        || generation.is_empty()
    {
        return Err(format!("--{prefix} authority differs"));
    }
    Ok(V23IncidenceLocalRolePath {
        identity: V23IncidenceObjectIdentity {
            role: role.to_string(),
            uri,
            digest_algorithm: "sha256".to_string(),
            digest,
            encoded_bytes,
            generation,
        },
        path,
    })
}

fn parse_directory_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<V23IncidenceLocalDirectoryPhaseRequest, String> {
    let mut arguments = arguments.into_iter();
    let gate_flag = arguments
        .next()
        .ok_or_else(|| "phase gate is absent".to_string())?;
    let mode = gate(&gate_flag).ok_or_else(|| "phase gate differs".to_string())?;
    let phase = match mode {
        V23IncidenceRunMode::Preflight(phase) | V23IncidenceRunMode::Execute(phase) => phase,
    };
    let manifest_role = if phase == V23IncidencePhase::TreeTraining {
        "construction-manifest"
    } else {
        "phase-manifest"
    };

    let manifest_flag = arguments
        .next()
        .ok_or_else(|| "--manifest is absent".to_string())?;
    if manifest_flag != "--manifest" {
        return Err(format!("expected --manifest, got {manifest_flag}"));
    }
    let manifest_path = PathBuf::from(take_value(&mut arguments, "--manifest")?);
    let manifest = take_sha256_role_path(&mut arguments, manifest_role, manifest_path, "manifest")?;

    let bulk_flag = arguments
        .next()
        .ok_or_else(|| "--bulk-manifest is absent".to_string())?;
    if bulk_flag != "--bulk-manifest" {
        return Err(format!("expected --bulk-manifest, got {bulk_flag}"));
    }
    let bulk_path = PathBuf::from(take_value(&mut arguments, "--bulk-manifest")?);
    let bulk_manifest =
        take_sha256_role_path(&mut arguments, "bulk-manifest", bulk_path, "bulk-manifest")?;

    let staging_directory_flag = arguments
        .next()
        .ok_or_else(|| "--staging-directory is absent".to_string())?;
    if staging_directory_flag != "--staging-directory" {
        return Err(format!(
            "expected --staging-directory, got {staging_directory_flag}"
        ));
    }
    let staging_directory_path = PathBuf::from(take_value(&mut arguments, "--staging-directory")?);

    let staging_receipt_flag = arguments
        .next()
        .ok_or_else(|| "--staging-receipt is absent".to_string())?;
    if staging_receipt_flag != "--staging-receipt" {
        return Err(format!(
            "expected --staging-receipt, got {staging_receipt_flag}"
        ));
    }
    let staging_receipt_path = PathBuf::from(take_value(&mut arguments, "--staging-receipt")?);
    let staging_receipt = take_sha256_role_path(
        &mut arguments,
        "staging-receipt",
        staging_receipt_path,
        "staging-receipt",
    )?;

    let mut preflight_receipt = None;
    let mut scratch_path = None;
    let mut output_path = None;
    let mut executable_sha256 = None;
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--preflight-receipt" => {
                if preflight_receipt.is_some() {
                    return Err("--preflight-receipt is duplicated".to_string());
                }
                let path = PathBuf::from(take_value(&mut arguments, &flag)?);
                preflight_receipt = Some(take_sha256_role_path(
                    &mut arguments,
                    "preflight-receipt",
                    path,
                    "preflight-receipt",
                )?);
            }
            "--scratch" => {
                let value = PathBuf::from(take_value(&mut arguments, &flag)?);
                set_once(&mut scratch_path, value, &flag)?;
            }
            "--output" => {
                let value = PathBuf::from(take_value(&mut arguments, &flag)?);
                set_once(&mut output_path, value, &flag)?;
            }
            "--executable-sha256" => {
                let value = take_value(&mut arguments, &flag)?;
                if !lower_hex_digest(&value) {
                    return Err("--executable-sha256 differs".to_string());
                }
                set_once(&mut executable_sha256, value, &flag)?;
            }
            _ => return Err(format!("unknown argument {flag}")),
        }
    }
    if matches!(mode, V23IncidenceRunMode::Execute(_)) != preflight_receipt.is_some()
        || !staging_directory_path.is_absolute()
    {
        return Err("directory request authority differs".to_string());
    }
    Ok(V23IncidenceLocalDirectoryPhaseRequest {
        mode,
        manifest,
        bulk_manifest,
        staging_directory_path,
        staging_receipt,
        preflight_receipt,
        scratch_path: scratch_path.ok_or_else(|| "--scratch is absent".to_string())?,
        output_path: output_path.ok_or_else(|| "--output is absent".to_string())?,
        executable_sha256: executable_sha256
            .ok_or_else(|| "--executable-sha256 is absent".to_string())?,
    })
}

#[cfg(not(test))]
fn main() {
    let request = parse_directory_args(std::env::args().skip(1)).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(2);
    });
    let output_path = request.output_path.clone();
    let bytes = run_v23_incidence_local_directory_phase(request).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    std::fs::write(&output_path, bytes).unwrap_or_else(|error| {
        eprintln!("failed to write {}: {error}", output_path.display());
        std::process::exit(1);
    });
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const PHASES: [V23IncidencePhase; 5] = [
        V23IncidencePhase::TreeTraining,
        V23IncidencePhase::PostingConstruction,
        V23IncidencePhase::DevelopmentEvaluation,
        V23IncidencePhase::HoldoutBinding,
        V23IncidencePhase::HoldoutEvaluation,
    ];

    fn phase_name(phase: V23IncidencePhase) -> &'static str {
        match phase {
            V23IncidencePhase::TreeTraining => "tree-training",
            V23IncidencePhase::PostingConstruction => "posting-construction",
            V23IncidencePhase::DevelopmentEvaluation => "development-evaluation",
            V23IncidencePhase::HoldoutBinding => "holdout-binding",
            V23IncidencePhase::HoldoutEvaluation => "holdout-evaluation",
        }
    }

    fn directory_arguments(phase: V23IncidencePhase, execute: bool) -> Vec<String> {
        let manifest_role = if phase == V23IncidencePhase::TreeTraining {
            "construction-manifest"
        } else {
            "phase-manifest"
        };
        let mut arguments = vec![
            format!(
                "--{}-{}",
                if execute { "execute" } else { "preflight" },
                phase_name(phase)
            ),
            "--manifest".to_string(),
            format!("/authority/{manifest_role}.json"),
            "--manifest-uri".to_string(),
            format!("s3://borsuk-evidence/{manifest_role}.json"),
            "--manifest-sha256".to_string(),
            "11".repeat(32),
            "--manifest-bytes".to_string(),
            "4096".to_string(),
            "--manifest-generation".to_string(),
            "generation-manifest".to_string(),
            "--bulk-manifest".to_string(),
            "/authority/bulk-manifest.json".to_string(),
            "--bulk-manifest-uri".to_string(),
            "s3://borsuk-evidence/bulk-manifest.json".to_string(),
            "--bulk-manifest-sha256".to_string(),
            "22".repeat(32),
            "--bulk-manifest-bytes".to_string(),
            "2048".to_string(),
            "--bulk-manifest-generation".to_string(),
            "generation-bulk-manifest".to_string(),
            "--staging-directory".to_string(),
            "/inputs/bulk".to_string(),
            "--staging-receipt".to_string(),
            "/authority/staging-receipt.json".to_string(),
            "--staging-receipt-uri".to_string(),
            "file:///authority/staging-receipt.json".to_string(),
            "--staging-receipt-sha256".to_string(),
            "33".repeat(32),
            "--staging-receipt-bytes".to_string(),
            "1024".to_string(),
            "--staging-receipt-generation".to_string(),
            "generation-staging-receipt".to_string(),
            "--scratch".to_string(),
            "/scratch".to_string(),
            "--output".to_string(),
            "/output/receipt.json".to_string(),
            "--executable-sha256".to_string(),
            "44".repeat(32),
        ];
        if execute {
            arguments.extend([
                "--preflight-receipt".to_string(),
                "/authority/preflight-receipt.json".to_string(),
                "--preflight-receipt-uri".to_string(),
                "file:///authority/preflight-receipt.json".to_string(),
                "--preflight-receipt-sha256".to_string(),
                "55".repeat(32),
                "--preflight-receipt-bytes".to_string(),
                "512".to_string(),
                "--preflight-receipt-generation".to_string(),
                "generation-preflight-receipt".to_string(),
            ]);
        }
        arguments
    }

    #[test]
    fn v23_incidence_directory_cli_is_bounded_independently_of_corpus_size() {
        for phase in PHASES {
            for execute in [false, true] {
                let arguments = directory_arguments(phase, execute);
                assert!(arguments.iter().map(String::len).sum::<usize>() < 16 * 1024);
                assert!(arguments.iter().all(|argument| {
                    !argument.starts_with("training-shard-") && !argument.starts_with("page-body-")
                }));
                let request: V23IncidenceLocalDirectoryPhaseRequest =
                    parse_directory_args(arguments).unwrap();
                assert_eq!(
                    matches!(request.mode, V23IncidenceRunMode::Execute(_)),
                    execute
                );
                assert_eq!(request.staging_directory_path, Path::new("/inputs/bulk"));
            }
        }
    }

    #[test]
    fn v23_incidence_directory_cli_rejects_unbounded_and_storage_surfaces() {
        let valid = directory_arguments(V23IncidencePhase::PostingConstruction, false);
        for flag in [
            "--input-role",
            "--page-body",
            "--training-shard",
            "--bucket",
            "--aws-profile",
            "--endpoint",
            "--storage-uri",
            "--d3",
        ] {
            let mut changed = valid.clone();
            changed.extend([flag.to_string(), "forbidden".to_string()]);
            assert!(parse_directory_args(changed).is_err(), "accepted {flag}");
        }
        assert!(parse_directory_args(valid[1..].to_vec()).is_err());
        assert!(parse_directory_args([valid.clone(), valid[..2].to_vec()].concat()).is_err());
    }

    #[test]
    fn v23_incidence_directory_cli_calls_only_the_high_level_local_runner() {
        let request =
            parse_directory_args(directory_arguments(V23IncidencePhase::TreeTraining, false))
                .unwrap();
        let error = run_v23_incidence_local_directory_phase(request).unwrap_err();
        assert!(
            error.to_string().contains("construction-manifest")
                || error.to_string().contains("offline probes are absent")
        );
    }
}
