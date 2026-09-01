use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const MANIFEST_SCHEMA: &str = "borsuk-v23-balanced-page-manifest-v1";
const RECEIPT_SCHEMA: &str = "borsuk-v23-balanced-page-receipt-v1";
const DIMENSIONS: u64 = 96;
const SUPERCELL_TARGET_ROWS: u64 = 12_288;
const PRIMARY_ROWS_PER_PAGE: u64 = 384;
const TOP_SUPERCELLS: u64 = 96;
const SELECTED_PAGES: u64 = 8;
const MAX_SUPERCELLS: u64 = 8_192;
const MAX_PAGES_PER_SUPERCELL: u64 = 64;
const RUNTIME_RESERVE_BYTES: u64 = 850 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedArmConfig {
    pub(crate) name: String,
    pub(crate) amplification_ppm: u64,
    pub(crate) replicas_per_page: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedIdentity {
    pub(crate) role: String,
    pub(crate) uri: String,
    pub(crate) digest_algorithm: String,
    pub(crate) digest: String,
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedManifest {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) dataset_id: String,
    pub(crate) rows: u64,
    pub(crate) dimensions: u32,
    pub(crate) supercell_target_rows: u64,
    pub(crate) primary_rows_per_page: u16,
    pub(crate) top_supercells: u16,
    pub(crate) selected_pages: u8,
    pub(crate) arms: Vec<V23BalancedArmConfig>,
    pub(crate) ordered_inputs: Vec<V23BalancedIdentity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23BalancedStop {
    Authority,
    Resource,
    Determinism,
    Progress,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedReceipt {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) manifest_sha256: String,
    pub(crate) ordered_inputs: Vec<V23BalancedIdentity>,
    pub(crate) outputs: Vec<V23BalancedIdentity>,
    pub(crate) stop: Option<V23BalancedStop>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23BalancedProjection {
    pub(crate) supercells: u64,
    pub(crate) maximum_pages: u64,
    pub(crate) maximum_scored_dimensions: u64,
    pub(crate) serving_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum V23BalancedLocalMode {
    Preflight,
    Execute,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23BalancedLocalRequest {
    pub manifest: PathBuf,
    pub input_directory: PathBuf,
    pub output_directory: PathBuf,
    pub mode: V23BalancedLocalMode,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("V23 balanced page {message}"))
}

fn valid_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn validate_v23_balanced_identity(identity: &V23BalancedIdentity) -> Result<()> {
    if identity.role.is_empty()
        || !identity.uri.starts_with("s3://")
        || identity.digest_algorithm != "sha256"
        || !valid_lower_hex(&identity.digest, 64)
        || identity.encoded_bytes == 0
    {
        return Err(invalid("object identity differs"));
    }
    Ok(())
}

fn validate_identity_list(identities: &[V23BalancedIdentity]) -> Result<()> {
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for identity in identities {
        validate_v23_balanced_identity(identity)?;
        if !roles.insert(identity.role.as_str()) || !uris.insert(identity.uri.as_str()) {
            return Err(invalid("object identity duplicates"));
        }
    }
    Ok(())
}

fn expected_arms() -> [V23BalancedArmConfig; 3] {
    [
        V23BalancedArmConfig {
            name: "amp-1125".to_owned(),
            amplification_ppm: 1_125_000,
            replicas_per_page: 48,
        },
        V23BalancedArmConfig {
            name: "amp-1250".to_owned(),
            amplification_ppm: 1_250_000,
            replicas_per_page: 96,
        },
        V23BalancedArmConfig {
            name: "amp-1500".to_owned(),
            amplification_ppm: 1_500_000,
            replicas_per_page: 192,
        },
    ]
}

pub(crate) fn validate_v23_balanced_manifest(manifest: &V23BalancedManifest) -> Result<()> {
    if manifest.schema != MANIFEST_SCHEMA
        || manifest.claim_eligible
        || !valid_lower_hex(&manifest.source_commit, 40)
        || !valid_lower_hex(&manifest.source_archive_sha256, 64)
        || manifest.dataset_id != "deep-image-96"
        || manifest.rows == 0
        || u64::from(manifest.dimensions) != DIMENSIONS
        || manifest.supercell_target_rows != SUPERCELL_TARGET_ROWS
        || u64::from(manifest.primary_rows_per_page) != PRIMARY_ROWS_PER_PAGE
        || u64::from(manifest.top_supercells) != TOP_SUPERCELLS
        || u64::from(manifest.selected_pages) != SELECTED_PAGES
        || manifest.arms.as_slice() != expected_arms()
    {
        return Err(invalid("manifest constants differ"));
    }
    if manifest
        .ordered_inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>()
        != ["source-shard-manifest", "f16-control"]
    {
        return Err(invalid("construction input roles differ"));
    }
    validate_identity_list(&manifest.ordered_inputs)?;
    project_v23_balanced_shape(manifest.rows)?;
    Ok(())
}

pub(crate) fn project_v23_balanced_shape(rows: u64) -> Result<V23BalancedProjection> {
    if rows == 0 {
        return Err(invalid("row count is zero"));
    }
    let targets = rows.div_ceil(SUPERCELL_TARGET_ROWS);
    let supercells = targets
        .checked_next_power_of_two()
        .ok_or_else(|| invalid("supercell projection overflows"))?
        .min(MAX_SUPERCELLS);
    let maximum_pages = rows
        .div_ceil(PRIMARY_ROWS_PER_PAGE)
        .checked_add(supercells - 1)
        .ok_or_else(|| invalid("page projection overflows"))?;
    let supercell_dimensions = supercells
        .checked_mul(DIMENSIONS)
        .ok_or_else(|| invalid("supercell work projection overflows"))?;
    let page_dimensions = TOP_SUPERCELLS
        .checked_mul(MAX_PAGES_PER_SUPERCELL)
        .and_then(|value| value.checked_mul(DIMENSIONS))
        .ok_or_else(|| invalid("page work projection overflows"))?;
    let maximum_scored_dimensions = supercell_dimensions
        .checked_add(page_dimensions)
        .ok_or_else(|| invalid("query work projection overflows"))?;
    let serving_bytes = supercells
        .checked_mul(DIMENSIONS * 4 + 16)
        .and_then(|value| value.checked_add(maximum_pages.checked_mul(DIMENSIONS * 4 + 64)?))
        .and_then(|value| value.checked_add(RUNTIME_RESERVE_BYTES))
        .ok_or_else(|| invalid("serving memory projection overflows"))?;
    Ok(V23BalancedProjection {
        supercells,
        maximum_pages,
        maximum_scored_dimensions,
        serving_bytes,
    })
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

fn regular_file(path: &Path) -> Result<()> {
    let metadata = path.symlink_metadata().map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_file() {
        return Err(invalid("local artifact is not a regular file"));
    }
    Ok(())
}

fn empty_directory(path: &Path, role: &str) -> Result<()> {
    let metadata = path.symlink_metadata().map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_dir()
        || path
            .read_dir()
            .map_err(|source| BorsukError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .next()
            .is_some()
    {
        return Err(invalid(&format!("local {role} directory differs")));
    }
    Ok(())
}

fn input_basename(role: &str) -> Result<&'static str> {
    match role {
        "source-shard-manifest" => Ok("source-shard-manifest.json"),
        "f16-control" => Ok("f16-control.arrow"),
        _ => Err(invalid("local input role differs")),
    }
}

fn authenticate_local_input(directory: &Path, identity: &V23BalancedIdentity) -> Result<()> {
    validate_v23_balanced_identity(identity)?;
    let path = directory.join(input_basename(&identity.role)?);
    regular_file(&path)?;
    let metadata = path.metadata().map_err(|source| BorsukError::Io {
        path: path.clone(),
        source,
    })?;
    if metadata.len() != identity.encoded_bytes {
        return Err(invalid("local input bytes differ"));
    }
    let mut file = File::open(&path).map_err(|source| BorsukError::Io {
        path: path.clone(),
        source,
    })?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.clone(),
            source,
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    if format!("{:x}", digest.finalize()) != identity.digest {
        return Err(invalid("local input bytes differ"));
    }
    Ok(())
}

#[doc(hidden)]
pub fn run_v23_balanced_local_request(request: V23BalancedLocalRequest) -> Result<Vec<u8>> {
    if !request.manifest.is_absolute()
        || !request.input_directory.is_absolute()
        || !request.output_directory.is_absolute()
        || request.manifest.parent() == Some(request.input_directory.as_path())
    {
        return Err(invalid("local request path differs"));
    }
    regular_file(&request.manifest)?;
    if request
        .manifest
        .metadata()
        .map_err(|source| BorsukError::Io {
            path: request.manifest.clone(),
            source,
        })?
        .len()
        > 1024 * 1024
    {
        return Err(invalid("local manifest exceeds one MiB"));
    }
    empty_directory(&request.output_directory, "output")?;
    let input_metadata = request
        .input_directory
        .symlink_metadata()
        .map_err(|source| BorsukError::Io {
            path: request.input_directory.clone(),
            source,
        })?;
    if !input_metadata.file_type().is_dir() {
        return Err(invalid("local input directory differs"));
    }
    let manifest_bytes = fs::read(&request.manifest).map_err(|source| BorsukError::Io {
        path: request.manifest.clone(),
        source,
    })?;
    let manifest: V23BalancedManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| invalid(&format!("local manifest JSON differs: {error}")))?;
    validate_v23_balanced_manifest(&manifest)?;
    let mut expected_manifest_bytes = serde_json::to_vec(&canonical_json_value(
        serde_json::to_value(&manifest)
            .map_err(|error| invalid(&format!("local manifest serialization failed: {error}")))?,
    ))
    .map_err(|error| invalid(&format!("local manifest canonical JSON failed: {error}")))?;
    expected_manifest_bytes.push(b'\n');
    if manifest_bytes != expected_manifest_bytes {
        return Err(invalid("local manifest bytes differ"));
    }
    let expected_names = manifest
        .ordered_inputs
        .iter()
        .map(|identity| input_basename(&identity.role))
        .collect::<Result<BTreeSet<_>>>()?;
    let observed_names = request
        .input_directory
        .read_dir()
        .map_err(|source| BorsukError::Io {
            path: request.input_directory.clone(),
            source,
        })?
        .map(|entry| {
            entry
                .map_err(|source| BorsukError::Io {
                    path: request.input_directory.clone(),
                    source,
                })?
                .file_name()
                .into_string()
                .map_err(|_| invalid("local input basename differs"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed_names
        != expected_names
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>()
    {
        return Err(invalid("local input inventory differs"));
    }
    for identity in &manifest.ordered_inputs {
        authenticate_local_input(&request.input_directory, identity)?;
    }
    if request.mode == V23BalancedLocalMode::Execute {
        return Err(invalid("local execution pipeline is not authorized"));
    }
    canonical_v23_balanced_receipt_bytes(&V23BalancedReceipt {
        schema: RECEIPT_SCHEMA.to_owned(),
        claim_eligible: false,
        manifest_sha256: format!("{:x}", Sha256::digest(&manifest_bytes)),
        ordered_inputs: manifest.ordered_inputs,
        outputs: Vec::new(),
        stop: None,
    })
}

pub(crate) fn canonical_v23_balanced_receipt_bytes(
    receipt: &V23BalancedReceipt,
) -> Result<Vec<u8>> {
    if receipt.schema != RECEIPT_SCHEMA
        || receipt.claim_eligible
        || !valid_lower_hex(&receipt.manifest_sha256, 64)
        || receipt.ordered_inputs.is_empty()
        || (receipt.stop.is_some() && !receipt.outputs.is_empty())
    {
        return Err(invalid("receipt authority differs"));
    }
    validate_identity_list(&receipt.ordered_inputs)?;
    validate_identity_list(&receipt.outputs)?;
    let value = serde_json::to_value(receipt)
        .map_err(|error| invalid(&format!("receipt serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("canonical JSON failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sha2::{Digest, Sha256};

    use super::{
        V23BalancedArmConfig, V23BalancedIdentity, V23BalancedLocalMode, V23BalancedLocalRequest,
        V23BalancedManifest, V23BalancedReceipt, V23BalancedStop,
        canonical_v23_balanced_receipt_bytes, project_v23_balanced_shape,
        run_v23_balanced_local_request, validate_v23_balanced_manifest,
    };

    fn sha256(byte: u8) -> String {
        std::iter::repeat_n(format!("{byte:02x}"), 32).collect()
    }

    fn identity(role: &str, byte: u8) -> V23BalancedIdentity {
        V23BalancedIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v23-eu-west-1/frozen/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: sha256(byte),
            encoded_bytes: 4096,
        }
    }

    fn identity_for_bytes(role: &str, bytes: &[u8]) -> V23BalancedIdentity {
        V23BalancedIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v23-eu-west-1/frozen/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: format!("{:x}", Sha256::digest(bytes)),
            encoded_bytes: bytes.len() as u64,
        }
    }

    fn canonical_manifest_bytes(manifest: &V23BalancedManifest) -> Vec<u8> {
        let value = serde_json::to_value(manifest).unwrap();
        let mut bytes = serde_json::to_vec(&super::canonical_json_value(value)).unwrap();
        bytes.push(b'\n');
        bytes
    }

    fn manifest_fixture(rows: u64) -> V23BalancedManifest {
        V23BalancedManifest {
            schema: "borsuk-v23-balanced-page-manifest-v1".to_owned(),
            claim_eligible: false,
            source_commit: sha256(0x11).chars().take(40).collect(),
            source_archive_sha256: sha256(0x12),
            dataset_id: "deep-image-96".to_owned(),
            rows,
            dimensions: 96,
            supercell_target_rows: 12_288,
            primary_rows_per_page: 384,
            top_supercells: 96,
            selected_pages: 8,
            arms: vec![
                V23BalancedArmConfig {
                    name: "amp-1125".to_owned(),
                    amplification_ppm: 1_125_000,
                    replicas_per_page: 48,
                },
                V23BalancedArmConfig {
                    name: "amp-1250".to_owned(),
                    amplification_ppm: 1_250_000,
                    replicas_per_page: 96,
                },
                V23BalancedArmConfig {
                    name: "amp-1500".to_owned(),
                    amplification_ppm: 1_500_000,
                    replicas_per_page: 192,
                },
            ],
            ordered_inputs: vec![
                identity("source-shard-manifest", 0x21),
                identity("f16-control", 0x22),
            ],
        }
    }

    #[test]
    fn v23_balanced_authority_rejects_identity_shape_and_role_drift() {
        let valid = manifest_fixture(100_000_000);
        validate_v23_balanced_manifest(&valid).unwrap();

        let mut mutations = Vec::new();
        let mut changed = valid.clone();
        changed.claim_eligible = true;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.dimensions = 95;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.arms[0].replicas_per_page = 49;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.ordered_inputs[0].digest_algorithm = "blake3".to_owned();
        mutations.push(changed);
        let mut changed = valid.clone();
        changed
            .ordered_inputs
            .push(identity("official-query-parquet", 0x23));
        mutations.push(changed);

        for mutation in mutations {
            assert!(validate_v23_balanced_manifest(&mutation).is_err());
        }
    }

    #[test]
    fn v23_balanced_authority_projection_is_exact_at_100m() {
        let projection = project_v23_balanced_shape(100_000_000).unwrap();
        assert_eq!(projection.supercells, 8_192);
        assert_eq!(projection.maximum_pages, 268_608);
        assert_eq!(projection.maximum_scored_dimensions, 1_376_256);
        assert_eq!(projection.serving_bytes, 1_014_902_784);
        assert!(projection.serving_bytes < 3 * 1024 * 1024 * 1024);
        assert!(project_v23_balanced_shape(0).is_err());
    }

    #[test]
    fn v23_balanced_authority_receipt_is_claim_ineligible_and_canonical() {
        let receipt = V23BalancedReceipt {
            schema: "borsuk-v23-balanced-page-receipt-v1".to_owned(),
            claim_eligible: false,
            manifest_sha256: sha256(0x31),
            ordered_inputs: manifest_fixture(100_000_000).ordered_inputs,
            outputs: vec![identity("supercells-parquet", 0x32)],
            stop: None,
        };
        let bytes = canonical_v23_balanced_receipt_bytes(&receipt).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut changed = receipt.clone();
        changed.claim_eligible = true;
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
        let mut changed = receipt;
        changed.stop = Some(V23BalancedStop::Resource);
        changed
            .outputs
            .push(identity("partial-scientific-output", 0x33));
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
    }

    #[test]
    fn v23_balanced_local_preflight_authenticates_exact_inventory_without_science() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input");
        let output = directory.path().join("output");
        fs::create_dir(&input).unwrap();
        fs::create_dir(&output).unwrap();
        let source_manifest = b"source-shards\n";
        let f16_control = b"f16-control\n";
        fs::write(input.join("source-shard-manifest.json"), source_manifest).unwrap();
        fs::write(input.join("f16-control.arrow"), f16_control).unwrap();
        let mut manifest = manifest_fixture(100_000_000);
        manifest.ordered_inputs = vec![
            identity_for_bytes("source-shard-manifest", source_manifest),
            identity_for_bytes("f16-control", f16_control),
        ];
        let manifest_path = directory.path().join("manifest.json");
        let manifest_bytes = canonical_manifest_bytes(&manifest);
        fs::write(&manifest_path, &manifest_bytes).unwrap();

        let terminal = run_v23_balanced_local_request(V23BalancedLocalRequest {
            manifest: manifest_path.clone(),
            input_directory: input.clone(),
            output_directory: output.clone(),
            mode: V23BalancedLocalMode::Preflight,
        })
        .unwrap();
        let receipt: V23BalancedReceipt = serde_json::from_slice(&terminal).unwrap();
        assert_eq!(
            receipt.manifest_sha256,
            format!("{:x}", Sha256::digest(&manifest_bytes))
        );
        assert_eq!(receipt.ordered_inputs, manifest.ordered_inputs);
        assert!(receipt.outputs.is_empty());
        assert_eq!(receipt.stop, None);
        assert!(fs::read_dir(output).unwrap().next().is_none());

        fs::write(input.join("f16-control.arrow"), b"f16-drifted\n").unwrap();
        assert!(
            run_v23_balanced_local_request(V23BalancedLocalRequest {
                manifest: directory.path().join("manifest.json"),
                input_directory: input.clone(),
                output_directory: directory.path().join("output"),
                mode: V23BalancedLocalMode::Preflight,
            })
            .is_err()
        );
        fs::write(input.join("f16-control.arrow"), f16_control).unwrap();
        let mut length_drift = manifest.clone();
        length_drift.ordered_inputs[1].encoded_bytes += 1;
        fs::write(&manifest_path, canonical_manifest_bytes(&length_drift)).unwrap();
        assert!(
            run_v23_balanced_local_request(V23BalancedLocalRequest {
                manifest: manifest_path.clone(),
                input_directory: input.clone(),
                output_directory: directory.path().join("output"),
                mode: V23BalancedLocalMode::Preflight,
            })
            .is_err()
        );
        fs::write(&manifest_path, &manifest_bytes).unwrap();
        fs::rename(
            input.join("f16-control.arrow"),
            input.join("f16-control.missing"),
        )
        .unwrap();
        assert!(
            run_v23_balanced_local_request(V23BalancedLocalRequest {
                manifest: manifest_path.clone(),
                input_directory: input.clone(),
                output_directory: directory.path().join("output"),
                mode: V23BalancedLocalMode::Preflight,
            })
            .is_err()
        );
        fs::rename(
            input.join("f16-control.missing"),
            input.join("f16-control.arrow"),
        )
        .unwrap();
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        assert!(
            run_v23_balanced_local_request(V23BalancedLocalRequest {
                manifest: manifest_path.clone(),
                input_directory: input.clone(),
                output_directory: directory.path().join("output"),
                mode: V23BalancedLocalMode::Preflight,
            })
            .is_err()
        );
        fs::write(&manifest_path, &manifest_bytes).unwrap();
        fs::write(input.join("unexpected.bin"), b"unexpected").unwrap();
        assert!(
            run_v23_balanced_local_request(V23BalancedLocalRequest {
                manifest: directory.path().join("manifest.json"),
                input_directory: input,
                output_directory: directory.path().join("output"),
                mode: V23BalancedLocalMode::Preflight,
            })
            .is_err()
        );
    }

    #[test]
    fn v23_balanced_local_execute_is_fail_closed_after_complete_authentication() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input");
        let output = directory.path().join("output");
        fs::create_dir(&input).unwrap();
        fs::create_dir(&output).unwrap();
        let source_manifest = b"source-shards\n";
        let f16_control = b"f16-control\n";
        fs::write(input.join("source-shard-manifest.json"), source_manifest).unwrap();
        fs::write(input.join("f16-control.arrow"), f16_control).unwrap();
        let mut manifest = manifest_fixture(100_000_000);
        manifest.ordered_inputs = vec![
            identity_for_bytes("source-shard-manifest", source_manifest),
            identity_for_bytes("f16-control", f16_control),
        ];
        let manifest_path = directory.path().join("manifest.json");
        fs::write(&manifest_path, canonical_manifest_bytes(&manifest)).unwrap();

        let error = run_v23_balanced_local_request(V23BalancedLocalRequest {
            manifest: manifest_path,
            input_directory: input,
            output_directory: output,
            mode: V23BalancedLocalMode::Execute,
        })
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("execution pipeline is not authorized")
        );
    }

    #[test]
    fn v23_balanced_local_rejects_nonempty_output_before_execution() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input");
        let output = directory.path().join("output");
        fs::create_dir(&input).unwrap();
        fs::create_dir(&output).unwrap();
        fs::write(output.join("stale.json"), b"stale").unwrap();
        fs::write(input.join("source-shard-manifest.json"), b"source-shards\n").unwrap();
        fs::write(input.join("f16-control.arrow"), b"f16-control\n").unwrap();
        let manifest_path = directory.path().join("manifest.json");
        fs::write(
            &manifest_path,
            canonical_manifest_bytes(&manifest_fixture(100_000_000)),
        )
        .unwrap();

        let error = run_v23_balanced_local_request(V23BalancedLocalRequest {
            manifest: manifest_path,
            input_directory: input,
            output_directory: output,
            mode: V23BalancedLocalMode::Execute,
        })
        .unwrap_err();
        assert!(error.to_string().contains("output directory differs"));
    }
}
