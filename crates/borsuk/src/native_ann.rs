use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    error::{BorsukError, Result},
    metric::VectorMetric,
};

const NATIVE_ANN_FORMAT_VERSION: u16 = 1;
const NATIVE_ANN_PAGE_ROWS: u32 = 256;
const NATIVE_ANN_PQ_WIDTH: u8 = 16;
const NATIVE_ANN_SUMMARY_CODES_PER_PAGE: u8 = 2;
const SHA256_HEX_LEN: usize = 64;
const MUTATION_KEY_HEX_LEN: usize = 48;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeArtifactRef {
    pub(crate) role: String,
    pub(crate) uri: String,
    pub(crate) sha256: String,
    pub(crate) encoded_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRouterRef {
    pub(crate) pq_width: u8,
    pub(crate) summary_codes_per_page: u8,
    pub(crate) physical_rows: u64,
    pub(crate) page_count: u32,
    pub(crate) codebooks: NativeArtifactRef,
    pub(crate) row_codes: NativeArtifactRef,
    pub(crate) summary_codes: NativeArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRunRef {
    pub(crate) ordinal: u32,
    pub(crate) rows: u64,
    pub(crate) version_start: String,
    pub(crate) version_end: String,
    pub(crate) artifact: NativeArtifactRef,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeSq8Authority {
    pub(crate) quantizer_sha256: String,
    pub(crate) dimensions: u32,
    pub(crate) parameters: NativeArtifactRef,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeAnnRef {
    pub(crate) format_version: u16,
    pub(crate) generation: u64,
    pub(crate) previous_generation_sha256: Option<String>,
    pub(crate) source_identity: String,
    pub(crate) dimensions: u32,
    pub(crate) metric: VectorMetric,
    pub(crate) page_rows: u32,
    pub(crate) router: NativeRouterRef,
    pub(crate) mutation_directory: NativeArtifactRef,
    pub(crate) base_runs: Vec<NativeRunRef>,
    pub(crate) delta_runs: Vec<NativeRunRef>,
    pub(crate) sq8: NativeSq8Authority,
}

fn invalid(message: impl Into<String>) -> BorsukError {
    BorsukError::InvalidStorage(message.into())
}

fn validate_hex(value: &str, len: usize, name: &str) -> Result<()> {
    if value.len() != len
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(format!(
            "native ANN {name} must contain exactly {len} lower-case hex characters"
        )));
    }
    Ok(())
}

impl NativeArtifactRef {
    fn validate(&self, expected_role: &str) -> Result<()> {
        if self.role != expected_role {
            return Err(invalid(format!(
                "native ANN artifact role differs: expected {expected_role}, got {}",
                self.role
            )));
        }
        let uri = url::Url::parse(&self.uri)
            .map_err(|error| invalid(format!("native ANN artifact URI is invalid: {error}")))?;
        if uri.cannot_be_a_base()
            || uri.scheme().is_empty()
            || uri.path().is_empty()
            || uri.query().is_some()
            || uri.fragment().is_some()
        {
            return Err(invalid(
                "native ANN artifact URI is not an absolute object URI",
            ));
        }
        validate_hex(&self.sha256, SHA256_HEX_LEN, "artifact SHA-256")?;
        if self.encoded_bytes == 0 {
            return Err(invalid("native ANN artifact must not be empty"));
        }
        Ok(())
    }
}

impl NativeRunRef {
    fn validate(&self, expected_role: &str, expected_ordinal: usize) -> Result<()> {
        if usize::try_from(self.ordinal).ok() != Some(expected_ordinal) {
            return Err(invalid("native ANN run ordinals are not contiguous"));
        }
        if self.rows == 0 {
            return Err(invalid("native ANN run must contain rows"));
        }
        validate_hex(
            &self.version_start,
            MUTATION_KEY_HEX_LEN,
            "run start mutation key",
        )?;
        validate_hex(
            &self.version_end,
            MUTATION_KEY_HEX_LEN,
            "run end mutation key",
        )?;
        if self.version_start > self.version_end {
            return Err(invalid("native ANN run mutation range is reversed"));
        }
        self.artifact.validate(expected_role)
    }
}

impl NativeAnnRef {
    fn validate(&self) -> Result<()> {
        if self.format_version != NATIVE_ANN_FORMAT_VERSION {
            return Err(invalid("native ANN format version differs"));
        }
        if self.generation == 0 {
            return Err(invalid("native ANN generation must be nonzero"));
        }
        match (self.generation, self.previous_generation_sha256.as_deref()) {
            (1, None) => {}
            (1, Some(_)) => {
                return Err(invalid(
                    "native ANN first generation must not name a predecessor",
                ));
            }
            (_, Some(digest)) => {
                validate_hex(digest, SHA256_HEX_LEN, "previous generation SHA-256")?;
            }
            (_, None) => {
                return Err(invalid(
                    "native ANN generation after one must name its predecessor",
                ));
            }
        }
        if self.source_identity.is_empty() {
            return Err(invalid("native ANN source identity must not be empty"));
        }
        if self.dimensions == 0 {
            return Err(invalid("native ANN dimensions must be nonzero"));
        }
        if let VectorMetric::Minkowski { p } = &self.metric
            && (!p.is_finite() || *p < 1.0)
        {
            return Err(invalid(
                "native ANN Minkowski power must be finite and >= 1",
            ));
        }
        if self.page_rows != NATIVE_ANN_PAGE_ROWS {
            return Err(invalid("native ANN page row count differs"));
        }
        if self.router.pq_width != NATIVE_ANN_PQ_WIDTH {
            return Err(invalid("native ANN PQ width differs"));
        }
        if self.router.summary_codes_per_page != NATIVE_ANN_SUMMARY_CODES_PER_PAGE {
            return Err(invalid("native ANN summary-code count differs"));
        }
        if self.router.physical_rows == 0 || self.router.page_count == 0 {
            return Err(invalid("native ANN router counts must be nonzero"));
        }
        let expected_pages = self
            .router
            .physical_rows
            .div_ceil(u64::from(self.page_rows));
        if expected_pages != u64::from(self.router.page_count) {
            return Err(invalid(
                "native ANN router page count differs from row count",
            ));
        }
        self.router.codebooks.validate("router-codebooks")?;
        self.router.row_codes.validate("router-row-codes")?;
        self.router.summary_codes.validate("router-summary-codes")?;
        self.mutation_directory.validate("mutation-directory")?;
        self.sq8.parameters.validate("sq8-authority")?;
        validate_hex(
            &self.sq8.quantizer_sha256,
            SHA256_HEX_LEN,
            "SQ8 quantizer SHA-256",
        )?;
        if self.sq8.dimensions != self.dimensions {
            return Err(invalid("native ANN SQ8 dimensions differ"));
        }
        if self.sq8.quantizer_sha256 != self.sq8.parameters.sha256 {
            return Err(invalid("native ANN SQ8 quantizer binding differs"));
        }

        validate_runs(&self.base_runs, "base-run")?;
        validate_runs(&self.delta_runs, "delta-run")?;
        let base_rows = self.base_runs.iter().try_fold(0_u64, |total, run| {
            total
                .checked_add(run.rows)
                .ok_or_else(|| invalid("native ANN base row count overflows"))
        })?;
        if base_rows != self.router.physical_rows {
            return Err(invalid("native ANN router rows differ from base runs"));
        }
        if let (Some(base), Some(delta)) = (self.base_runs.last(), self.delta_runs.first())
            && base.version_end >= delta.version_start
        {
            return Err(invalid("native ANN base and delta mutation ranges overlap"));
        }

        let artifacts = [
            &self.router.codebooks,
            &self.router.row_codes,
            &self.router.summary_codes,
            &self.mutation_directory,
            &self.sq8.parameters,
        ];
        let mut uris = BTreeSet::new();
        for artifact in artifacts.into_iter().chain(
            self.base_runs
                .iter()
                .chain(self.delta_runs.iter())
                .map(|run| &run.artifact),
        ) {
            if !uris.insert(artifact.uri.as_str()) {
                return Err(invalid("native ANN artifact URIs must be unique"));
            }
        }
        Ok(())
    }
}

fn validate_runs(runs: &[NativeRunRef], role: &str) -> Result<()> {
    let mut previous_end: Option<&str> = None;
    for (ordinal, run) in runs.iter().enumerate() {
        run.validate(role, ordinal)?;
        if previous_end.is_some_and(|end| end >= run.version_start.as_str()) {
            return Err(invalid(
                "native ANN run mutation ranges overlap or are unordered",
            ));
        }
        previous_end = Some(&run.version_end);
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

pub(crate) fn native_ann_root_bytes(reference: &NativeAnnRef) -> Result<Vec<u8>> {
    reference.validate()?;
    let value = serde_json::to_value(reference)
        .map_err(|error| invalid(format!("native ANN root serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(format!("native ANN root serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn native_ann_root_from_bytes(bytes: &[u8]) -> Result<NativeAnnRef> {
    let reference: NativeAnnRef = serde_json::from_slice(bytes)
        .map_err(|error| invalid(format!("native ANN root JSON is invalid: {error}")))?;
    let canonical = native_ann_root_bytes(&reference)?;
    if canonical != bytes {
        return Err(invalid("native ANN root JSON is not canonical"));
    }
    Ok(reference)
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::metric::VectorMetric;

    const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const DIGEST_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const DIGEST_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const VERSION_1: &str = "000000000000000101010101010101010101010101010101";
    const VERSION_2: &str = "000000000000000202020202020202020202020202020202";
    const VERSION_3: &str = "000000000000000303030303030303030303030303030303";

    fn artifact(role: &str, suffix: &str, sha256: &str, encoded_bytes: u64) -> NativeArtifactRef {
        NativeArtifactRef {
            role: role.to_owned(),
            uri: format!("s3://borsuk-fixture/native/{suffix}"),
            sha256: sha256.to_owned(),
            encoded_bytes,
        }
    }

    fn run(
        role: &str,
        ordinal: u32,
        version_start: &str,
        version_end: &str,
        digest: &str,
    ) -> NativeRunRef {
        NativeRunRef {
            ordinal,
            rows: 256,
            version_start: version_start.to_owned(),
            version_end: version_end.to_owned(),
            artifact: artifact(role, &format!("{role}-{ordinal}.arrow"), digest, 4_096),
        }
    }

    fn valid_authority() -> NativeAnnRef {
        NativeAnnRef {
            format_version: 1,
            generation: 7,
            previous_generation_sha256: Some(DIGEST_A.to_owned()),
            source_identity: "deep-image/validation/v1".to_owned(),
            dimensions: 96,
            metric: VectorMetric::SquaredEuclidean,
            page_rows: 256,
            router: NativeRouterRef {
                pq_width: 16,
                summary_codes_per_page: 2,
                physical_rows: 512,
                page_count: 2,
                codebooks: artifact(
                    "router-codebooks",
                    "router-codebooks.parquet",
                    DIGEST_B,
                    65_536,
                ),
                row_codes: artifact(
                    "router-row-codes",
                    "router-row-codes.parquet",
                    DIGEST_C,
                    8_192,
                ),
                summary_codes: artifact(
                    "router-summary-codes",
                    "router-summary-codes.parquet",
                    DIGEST_D,
                    64,
                ),
            },
            mutation_directory: artifact(
                "mutation-directory",
                "mutation-directory.arrow",
                DIGEST_A,
                2_048,
            ),
            base_runs: vec![NativeRunRef {
                rows: 512,
                ..run("base-run", 0, VERSION_1, VERSION_1, DIGEST_B)
            }],
            delta_runs: vec![
                run("delta-run", 0, VERSION_2, VERSION_2, DIGEST_C),
                run("delta-run", 1, VERSION_3, VERSION_3, DIGEST_D),
            ],
            sq8: NativeSq8Authority {
                quantizer_sha256: DIGEST_D.to_owned(),
                dimensions: 96,
                parameters: artifact("sq8-authority", "sq8-authority.parquet", DIGEST_D, 1_024),
            },
        }
    }

    fn canonical_test_json(value: Value) -> Value {
        match value {
            Value::Array(values) => {
                Value::Array(values.into_iter().map(canonical_test_json).collect())
            }
            Value::Object(values) => Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, canonical_test_json(value)))
                    .collect(),
            ),
            scalar => scalar,
        }
    }

    fn canonical_test_bytes(value: Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(&canonical_test_json(value)).unwrap();
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn native_ann_authority_round_trips_canonical_cross_language_root() {
        let authority = valid_authority();

        let bytes = native_ann_root_bytes(&authority).unwrap();

        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
        assert!(!bytes.windows(2).any(|window| window == b": "));
        assert_eq!(native_ann_root_from_bytes(&bytes).unwrap(), authority);
        assert_eq!(native_ann_root_bytes(&authority).unwrap(), bytes);
    }

    #[test]
    fn native_ann_authority_rejects_every_identity_schema_and_generation_drift() {
        let valid = valid_authority();
        let valid_bytes = native_ann_root_bytes(&valid).unwrap();
        let valid_json: Value = serde_json::from_slice(&valid_bytes).unwrap();

        let mut malformed = Vec::new();
        let mut missing = valid_json.clone();
        missing.as_object_mut().unwrap().remove("source_identity");
        malformed.push(canonical_test_bytes(missing));
        let mut extra = valid_json;
        extra
            .as_object_mut()
            .unwrap()
            .insert("legacy_alias".to_owned(), Value::Bool(true));
        malformed.push(canonical_test_bytes(extra));
        let mut noncanonical = valid_bytes.clone();
        noncanonical.pop();
        malformed.push(noncanonical);
        for bytes in malformed {
            assert!(native_ann_root_from_bytes(&bytes).is_err());
        }

        let mut invalid = Vec::new();
        let mut changed = valid.clone();
        changed.generation = 0;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.previous_generation_sha256 = Some("A".repeat(64));
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.source_identity.clear();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.dimensions = 0;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.metric = VectorMetric::Minkowski { p: f32::NAN };
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.page_rows = 255;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.router.pq_width = 8;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.router.summary_codes_per_page = 1;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.router.row_codes.uri = "router-row-codes.parquet".to_owned();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.router.row_codes.sha256 = "0".repeat(63);
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.router.row_codes.encoded_bytes = 0;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.router.row_codes.role = "router-summary-codes".to_owned();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.router.summary_codes.uri = changed.router.row_codes.uri.clone();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.delta_runs[1].ordinal = 0;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.delta_runs.swap(0, 1);
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.delta_runs[1].version_start = VERSION_2.to_owned();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.base_runs[0].artifact.role = "delta-run".to_owned();
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.sq8.dimensions = 95;
        invalid.push(changed);
        let mut changed = valid.clone();
        changed.sq8.quantizer_sha256 = DIGEST_A.to_owned();
        changed.sq8.parameters.sha256 = DIGEST_B.to_owned();
        invalid.push(changed);

        for authority in invalid {
            assert!(native_ann_root_bytes(&authority).is_err());
        }
    }
}
