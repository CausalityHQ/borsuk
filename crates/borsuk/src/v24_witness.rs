use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{BorsukError, Result};

const V24_RECEIPT_SCHEMA: &str = "borsuk-v24-witness-receipt-v1";
const V24_MAX_CONSTRUCTION_RSS_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const V24_MAX_PSI_FULL_AVG10_PPM: u32 = 500_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V24Phase {
    WitnessTraining,
    PostingConstruction,
    DevelopmentEvaluation,
    HoldoutBinding,
    HoldoutEvaluation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V24ObjectIdentity {
    pub(crate) role: String,
    pub(crate) uri: String,
    pub(crate) digest_algorithm: String,
    pub(crate) digest: String,
    pub(crate) encoded_bytes: u64,
    pub(crate) generation: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V24SourceRow {
    pub(crate) source_ordinal: u64,
    pub(crate) vector: [f32; 96],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V24Receipt {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) phase: V24Phase,
    pub(crate) parent_receipt_sha256: Option<String>,
    pub(crate) executable_sha256: String,
    pub(crate) ordered_inputs: Vec<V24ObjectIdentity>,
    pub(crate) outputs: Vec<V24ObjectIdentity>,
    pub(crate) peak_rss_bytes: u64,
    pub(crate) peak_psi_full_avg10_ppm: u32,
    pub(crate) swap_delta_bytes: u64,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn exact_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn role_is_registered(role: &str) -> bool {
    matches!(
        role,
        "construction-manifest"
            | "construction-rows-parquet"
            | "dataset-meta"
            | "training-manifest"
            | "training-result"
            | "witnesses-arrow"
            | "witness-graph"
            | "posting-manifest"
            | "page-rows-parquet"
            | "page-roster"
            | "witness-postings"
            | "query-parquet"
            | "neighbors-parquet"
            | "parent-receipt"
            | "preflight-receipt"
            | "development-result"
            | "holdout-truth"
            | "holdout-result"
    ) || role
        .strip_prefix("training-shard-")
        .is_some_and(|suffix| suffix.len() == 5 && suffix.bytes().all(|byte| byte.is_ascii_digit()))
        || role.strip_prefix("page-body-").is_some_and(|suffix| {
            suffix.len() == 5 && suffix.bytes().all(|byte| byte.is_ascii_digit())
        })
}

pub(crate) fn parse_v24_decimal_source_ordinal(bytes: &[u8]) -> Result<u64> {
    let value = std::str::from_utf8(bytes)
        .ok()
        .filter(|value| {
            !value.is_empty()
                && value.bytes().all(|byte| byte.is_ascii_digit())
                && (*value == "0" || !value.starts_with('0'))
        })
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| value.to_string().as_bytes() == bytes)
        .ok_or_else(|| invalid("V24 source ordinal differs"))?;
    Ok(value)
}

pub(crate) fn validate_v24_identity(
    observed: &V24ObjectIdentity,
    registered: &V24ObjectIdentity,
) -> Result<()> {
    if observed != registered
        || !role_is_registered(&registered.role)
        || !registered.uri.starts_with("s3://")
        || registered.uri.ends_with('/')
        || registered.uri.contains("/../")
        || registered.digest_algorithm != "sha256"
        || !exact_lower_hex(&registered.digest, 64)
        || registered.encoded_bytes == 0
        || registered.generation.is_empty()
    {
        return Err(invalid("V24 object identity differs"));
    }
    Ok(())
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect(),
        ),
        scalar => scalar,
    }
}

pub(crate) fn canonical_v24_receipt_bytes(receipt: &V24Receipt) -> Result<Vec<u8>> {
    let parent_is_valid = match receipt.phase {
        V24Phase::WitnessTraining => receipt.parent_receipt_sha256.is_none(),
        V24Phase::PostingConstruction
        | V24Phase::DevelopmentEvaluation
        | V24Phase::HoldoutBinding
        | V24Phase::HoldoutEvaluation => receipt
            .parent_receipt_sha256
            .as_deref()
            .is_some_and(|digest| exact_lower_hex(digest, 64)),
    };
    if receipt.schema != V24_RECEIPT_SCHEMA
        || receipt.claim_eligible
        || !parent_is_valid
        || !exact_lower_hex(&receipt.executable_sha256, 64)
        || receipt.ordered_inputs.is_empty()
        || receipt.outputs.is_empty()
        || receipt.peak_rss_bytes == 0
        || receipt.peak_rss_bytes > V24_MAX_CONSTRUCTION_RSS_BYTES
        || receipt.peak_psi_full_avg10_ppm > V24_MAX_PSI_FULL_AVG10_PPM
        || receipt.swap_delta_bytes != 0
    {
        return Err(invalid("V24 receipt authority differs"));
    }

    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for identity in receipt.ordered_inputs.iter().chain(&receipt.outputs) {
        validate_v24_identity(identity, identity)?;
        if !roles.insert(&identity.role) || !uris.insert(&identity.uri) {
            return Err(invalid("V24 receipt object roles differ"));
        }
    }

    let value = serde_json::to_value(receipt)
        .map_err(|error| invalid(&format!("V24 receipt serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V24 receipt serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        V24ObjectIdentity, V24Phase, V24Receipt, canonical_v24_receipt_bytes,
        parse_v24_decimal_source_ordinal, validate_v24_identity,
    };

    fn object(role: &str, digest_algorithm: &str, digest_byte: &str) -> V24ObjectIdentity {
        V24ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v24/{role}"),
            digest_algorithm: digest_algorithm.to_owned(),
            digest: digest_byte.repeat(32),
            encoded_bytes: 17,
            generation: format!("generation-{role}"),
        }
    }

    fn receipt() -> V24Receipt {
        V24Receipt {
            schema: "borsuk-v24-witness-receipt-v1".to_owned(),
            claim_eligible: false,
            phase: V24Phase::WitnessTraining,
            parent_receipt_sha256: None,
            executable_sha256: "11".repeat(32),
            ordered_inputs: vec![object("construction-manifest", "sha256", "22")],
            outputs: vec![object("witnesses-arrow", "sha256", "33")],
            peak_rss_bytes: 1_073_741_824,
            peak_psi_full_avg10_ppm: 0,
            swap_delta_bytes: 0,
        }
    }

    #[test]
    fn v24_witness_authority_rejects_positional_identity_and_v23_schemas() {
        let registered = object("construction-manifest", "sha256", "44");
        assert!(validate_v24_identity(&registered, &registered).is_ok());
        assert_eq!(parse_v24_decimal_source_ordinal(b"0").unwrap(), 0);
        assert_eq!(
            parse_v24_decimal_source_ordinal(b"9990000").unwrap(),
            9_990_000
        );
        for invalid in [b"".as_slice(), b"00", b"01", b"-1", b"0x1", &[0_u8; 8]] {
            assert!(parse_v24_decimal_source_ordinal(invalid).is_err());
        }

        let mut changed = registered.clone();
        changed.digest_algorithm = "blake3".to_owned();
        assert!(validate_v24_identity(&changed, &registered).is_err());
        let mut changed = registered.clone();
        changed.uri.push_str("-different");
        assert!(validate_v24_identity(&changed, &registered).is_err());
        let mut changed = registered.clone();
        changed.encoded_bytes += 1;
        assert!(validate_v24_identity(&changed, &registered).is_err());

        let mut changed = receipt();
        changed.schema = "borsuk-v23-incidence-receipt-v3".to_owned();
        assert!(canonical_v24_receipt_bytes(&changed).is_err());
    }

    #[test]
    fn v24_witness_authority_receipt_binds_phase_inputs_outputs_and_resources() {
        let registered = receipt();
        let bytes = canonical_v24_receipt_bytes(&registered).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut changed = registered.clone();
        changed.claim_eligible = true;
        assert!(canonical_v24_receipt_bytes(&changed).is_err());
        let mut changed = registered.clone();
        changed.parent_receipt_sha256 = Some("55".repeat(32));
        assert!(canonical_v24_receipt_bytes(&changed).is_err());
        let mut changed = registered.clone();
        changed.outputs[0] = changed.ordered_inputs[0].clone();
        assert!(canonical_v24_receipt_bytes(&changed).is_err());
        let mut changed = registered.clone();
        changed
            .ordered_inputs
            .push(changed.ordered_inputs[0].clone());
        assert!(canonical_v24_receipt_bytes(&changed).is_err());
        let mut changed = registered;
        changed.swap_delta_bytes = 1;
        assert!(canonical_v24_receipt_bytes(&changed).is_err());
    }
}
