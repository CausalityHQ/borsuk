use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, FixedSizeListArray, Float16Array};
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v23_balanced_pages_arrow::{
        read_v23_row_page_assignments, write_v23_pages, write_v23_supercells,
    },
    v23_balanced_pages_build::{
        V23PageBuildShape, V23PrimaryPageBuild, V23ReplicaArmBuild, V23ReplicaArmOutput,
        V23ReplicaBuildInputs, V23RoutedRow, build_v23_primary_pages, build_v23_replica_arms,
    },
    v23_balanced_pages_eval::{
        V23BalancedPseudoqueryAccumulator, V23BalancedPseudoqueryEvidence,
        V23BalancedPseudoqueryPair, V23BalancedSample, V23BalancedSelectedPairEvidence,
        V23BalancedServingGeometry, build_v23_balanced_sample, classify_v23_balanced_pair_ladder,
        evaluate_v23_balanced_pseudoquery_pair_for_expected_count,
        prepare_v23_balanced_serving_geometry,
    },
    v23_balanced_pages_train::{
        V23BalancedTrainingRow, V23SupercellModel, route_v23_supercell_beam2,
        sample_v23_balanced_reservoir, train_v23_balanced_tree,
    },
};

const MANIFEST_SCHEMA: &str = "borsuk-v23-balanced-page-manifest-v3";
const RECEIPT_SCHEMA: &str = "borsuk-v23-balanced-page-receipt-v2";
const DIMENSIONS: u64 = 96;
const SUPERCELL_TARGET_ROWS: u64 = 12_288;
const PRIMARY_ROWS_PER_PAGE: u64 = 384;
const TOP_SUPERCELLS: u64 = 96;
const PAGE_BUDGETS: [u8; 3] = [8, 12, 16];
const F16_MAX_BATCH_ROWS: usize = 262_144;
const F16_MAX_FOOTER_BYTES: usize = 1024 * 1024;
const F16_MAX_BATCH_METADATA_BYTES: usize = 1024 * 1024;
const ARROW_CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
const MAX_SUPERCELLS: u64 = 8_192;
const MAX_PAGES_PER_SUPERCELL: u64 = 64;
const RUNTIME_RESERVE_BYTES: u64 = 850 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedArmConfig {
    pub(crate) name: String,
    pub(crate) amplification_ppm: u64,
    pub(crate) replicas_per_page: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V23BalancedArm {
    Amp1125,
    Amp1250,
    Amp1500,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct V23BalancedPageBudget(pub(crate) u8);

impl V23BalancedPageBudget {
    pub(crate) fn new(value: u8) -> Result<Self> {
        PAGE_BUDGETS
            .contains(&value)
            .then_some(Self(value))
            .ok_or_else(|| invalid("page budget differs"))
    }

    pub(crate) fn get(self) -> u8 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct V23BalancedSelectedPair {
    pub(crate) page_budget: V23BalancedPageBudget,
    pub(crate) arm: V23BalancedArm,
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
    pub(crate) deterministic_seed: u64,
    pub(crate) worker_threads: u16,
    pub(crate) sort_run_rows: u32,
    pub(crate) scratch_bytes_limit: u64,
    pub(crate) output_uri_prefix: String,
    pub(crate) rows: u64,
    pub(crate) dimensions: u32,
    pub(crate) supercell_target_rows: u64,
    pub(crate) primary_rows_per_page: u16,
    pub(crate) top_supercells: u16,
    pub(crate) page_budgets: Vec<V23BalancedPageBudget>,
    pub(crate) arms: Vec<V23BalancedArmConfig>,
    pub(crate) ordered_inputs: Vec<V23BalancedIdentity>,
    pub(crate) output_roles: Vec<String>,
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
    pub(crate) selected_pair: Option<V23BalancedSelectedPair>,
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

fn expected_output_roles() -> [&'static str; 11] {
    [
        "balanced-tree",
        "supercells-parquet",
        "pages-primary-parquet",
        "row-pages-primary-parquet",
        "pages-amp-1125-parquet",
        "row-pages-amp-1125-parquet",
        "pages-amp-1250-parquet",
        "row-pages-amp-1250-parquet",
        "pages-amp-1500-parquet",
        "row-pages-amp-1500-parquet",
        "development-result",
    ]
}

pub(crate) fn validate_v23_balanced_manifest(manifest: &V23BalancedManifest) -> Result<()> {
    if manifest.schema != MANIFEST_SCHEMA
        || manifest.claim_eligible
        || !valid_lower_hex(&manifest.source_commit, 40)
        || !valid_lower_hex(&manifest.source_archive_sha256, 64)
        || manifest.dataset_id != "deep-image-96"
        || manifest.deterministic_seed != 0x6a09_e667_f3bc_c909
        || manifest.worker_threads != 4
        || manifest.sort_run_rows != 262_144
        || manifest.scratch_bytes_limit != 64 * 1024 * 1024 * 1024
        || !manifest.output_uri_prefix.starts_with("s3://")
        || !manifest.output_uri_prefix.ends_with('/')
        || manifest.rows == 0
        || u64::from(manifest.dimensions) != DIMENSIONS
        || manifest.supercell_target_rows != SUPERCELL_TARGET_ROWS
        || u64::from(manifest.primary_rows_per_page) != PRIMARY_ROWS_PER_PAGE
        || u64::from(manifest.top_supercells) != TOP_SUPERCELLS
        || manifest
            .page_budgets
            .iter()
            .map(|budget| budget.get())
            .ne(PAGE_BUDGETS)
        || manifest.arms.as_slice() != expected_arms()
        || manifest
            .output_roles
            .iter()
            .map(String::as_str)
            .ne(expected_output_roles())
    {
        return Err(invalid("manifest constants differ"));
    }
    if manifest
        .ordered_inputs
        .iter()
        .map(|identity| identity.role.as_str())
        .collect::<Vec<_>>()
        != [
            "source-shard-manifest",
            "f16-control",
            "query-parquet",
            "neighbors-parquet",
        ]
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
        "query-parquet" => Ok("query.parquet"),
        "neighbors-parquet" => Ok("neighbors.parquet"),
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

pub(crate) struct V23BalancedF16RowStream {
    reader: FileReader<File>,
    pending: std::vec::IntoIter<V23BalancedTrainingRow>,
    next_source_ordinal: u64,
    expected_rows: u64,
    failed: bool,
}

impl Iterator for V23BalancedF16RowStream {
    type Item = Result<V23BalancedTrainingRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Some(row) = self.pending.next() {
            return Some(Ok(row));
        }
        loop {
            let batch = match self.reader.next() {
                Some(Ok(batch)) => batch,
                Some(Err(error)) => {
                    self.failed = true;
                    return Some(Err(error.into()));
                }
                None if self.next_source_ordinal == self.expected_rows => return None,
                None => {
                    self.failed = true;
                    return Some(Err(invalid("f16 corpus row count differs")));
                }
            };
            if batch.num_rows() == 0
                || batch.num_columns() != 1
                || batch.column(0).null_count() != 0
            {
                self.failed = true;
                return Some(Err(invalid("f16 corpus batch differs")));
            }
            let Some(vectors) = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
            else {
                self.failed = true;
                return Some(Err(invalid("f16 corpus vectors differ")));
            };
            let mut rows = Vec::with_capacity(vectors.len());
            for index in 0..vectors.len() {
                if self.next_source_ordinal >= self.expected_rows || vectors.is_null(index) {
                    self.failed = true;
                    return Some(Err(invalid("f16 corpus row count differs")));
                }
                let values = vectors.value(index);
                let Some(values) = values.as_any().downcast_ref::<Float16Array>() else {
                    self.failed = true;
                    return Some(Err(invalid("f16 corpus child differs")));
                };
                if values.len() != 96 || values.null_count() != 0 {
                    self.failed = true;
                    return Some(Err(invalid("f16 corpus row width differs")));
                }
                let vector = std::array::from_fn(|dimension| values.values()[dimension].to_f32());
                let squared_norm = vector.iter().try_fold(0.0_f64, |sum, value| {
                    value
                        .is_finite()
                        .then_some(sum + f64::from(*value) * f64::from(*value))
                });
                if squared_norm.is_none_or(|norm| !norm.is_finite() || norm == 0.0) {
                    self.failed = true;
                    return Some(Err(invalid("f16 corpus vector differs")));
                }
                rows.push(V23BalancedTrainingRow {
                    source_ordinal: self.next_source_ordinal,
                    vector,
                });
                self.next_source_ordinal += 1;
            }
            self.pending = rows.into_iter();
            if let Some(row) = self.pending.next() {
                return Some(Ok(row));
            }
        }
    }
}

fn validate_v23_balanced_f16_ipc_layout(file: &mut File, expected_rows: u64) -> Result<()> {
    let file_bytes = file
        .metadata()
        .map_err(|source| BorsukError::Io {
            path: PathBuf::from("f16-control.arrow"),
            source,
        })?
        .len();
    if file_bytes < 10 {
        return Err(invalid("f16 corpus IPC footer differs"));
    }
    file.seek(SeekFrom::End(-10))
        .map_err(|_| invalid("f16 corpus IPC footer differs"))?;
    let mut trailer = [0_u8; 10];
    file.read_exact(&mut trailer)
        .map_err(|_| invalid("f16 corpus IPC footer differs"))?;
    let footer_bytes = arrow_ipc::reader::read_footer_length(trailer)?;
    let footer_bytes_u64 =
        u64::try_from(footer_bytes).map_err(|_| invalid("f16 corpus IPC footer differs"))?;
    if footer_bytes == 0
        || footer_bytes > F16_MAX_FOOTER_BYTES
        || footer_bytes_u64 > file_bytes - 10
    {
        return Err(invalid("f16 corpus IPC footer differs"));
    }
    file.seek(SeekFrom::End(
        -10 - i64::try_from(footer_bytes).map_err(|_| invalid("f16 corpus IPC footer differs"))?,
    ))
    .map_err(|_| invalid("f16 corpus IPC footer differs"))?;
    let mut encoded_footer = vec![0_u8; footer_bytes];
    file.read_exact(&mut encoded_footer)
        .map_err(|_| invalid("f16 corpus IPC footer differs"))?;
    let footer = arrow_ipc::root_as_footer(&encoded_footer)
        .map_err(|_| invalid("f16 corpus IPC footer differs"))?;
    if footer
        .dictionaries()
        .is_some_and(|blocks| !blocks.is_empty())
    {
        return Err(invalid("f16 corpus IPC dictionaries differ"));
    }
    let blocks = footer
        .recordBatches()
        .ok_or_else(|| invalid("f16 corpus IPC batches differ"))?;
    if blocks.is_empty() {
        return Err(invalid("f16 corpus IPC batches differ"));
    }
    let data_end = file_bytes - 10 - footer_bytes_u64;
    let mut total_rows = 0_u64;
    for block in blocks {
        let offset =
            u64::try_from(block.offset()).map_err(|_| invalid("f16 corpus IPC block differs"))?;
        let metadata_bytes = usize::try_from(block.metaDataLength())
            .map_err(|_| invalid("f16 corpus IPC block differs"))?;
        let body_bytes = u64::try_from(block.bodyLength())
            .map_err(|_| invalid("f16 corpus IPC block differs"))?;
        let block_end = offset
            .checked_add(
                u64::try_from(metadata_bytes)
                    .map_err(|_| invalid("f16 corpus IPC block differs"))?,
            )
            .and_then(|end| end.checked_add(body_bytes))
            .ok_or_else(|| invalid("f16 corpus IPC block differs"))?;
        if !(4..=F16_MAX_BATCH_METADATA_BYTES).contains(&metadata_bytes) || block_end > data_end {
            return Err(invalid("f16 corpus IPC block differs"));
        }
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| invalid("f16 corpus IPC block differs"))?;
        let mut metadata = vec![0_u8; metadata_bytes];
        file.read_exact(&mut metadata)
            .map_err(|_| invalid("f16 corpus IPC block differs"))?;
        let flatbuffer = if metadata.starts_with(&ARROW_CONTINUATION_MARKER) {
            metadata
                .get(8..)
                .ok_or_else(|| invalid("f16 corpus IPC message differs"))?
        } else {
            metadata
                .get(4..)
                .ok_or_else(|| invalid("f16 corpus IPC message differs"))?
        };
        let message = arrow_ipc::root_as_message(flatbuffer)
            .map_err(|_| invalid("f16 corpus IPC message differs"))?;
        let record = message
            .header_as_record_batch()
            .ok_or_else(|| invalid("f16 corpus IPC message differs"))?;
        let rows = usize::try_from(record.length())
            .map_err(|_| invalid("f16 corpus IPC batch rows differ"))?;
        if rows == 0
            || rows > F16_MAX_BATCH_ROWS
            || record.compression().is_some()
            || u64::try_from(message.bodyLength()).ok() != Some(body_bytes)
        {
            return Err(invalid("f16 corpus IPC batch rows differ"));
        }
        let child_rows = rows
            .checked_mul(96)
            .ok_or_else(|| invalid("f16 corpus IPC batch rows differ"))?;
        let nodes = record
            .nodes()
            .ok_or_else(|| invalid("f16 corpus IPC nodes differ"))?;
        if nodes.len() != 2
            || nodes.get(0).length() != i64::try_from(rows).unwrap()
            || nodes.get(0).null_count() != 0
            || nodes.get(1).length() != i64::try_from(child_rows).unwrap()
            || nodes.get(1).null_count() != 0
        {
            return Err(invalid("f16 corpus IPC nodes differ"));
        }
        let buffers = record
            .buffers()
            .ok_or_else(|| invalid("f16 corpus IPC buffers differ"))?;
        let expected_value_bytes = child_rows
            .checked_mul(std::mem::size_of::<half::f16>())
            .ok_or_else(|| invalid("f16 corpus IPC buffers differ"))?;
        let outer_validity_bytes = rows.div_ceil(8);
        let child_validity_bytes = child_rows.div_ceil(8);
        let maximum_body_bytes = expected_value_bytes
            .checked_add(outer_validity_bytes)
            .and_then(|bytes| bytes.checked_add(child_validity_bytes))
            .and_then(|bytes| bytes.checked_add(3 * 63))
            .ok_or_else(|| invalid("f16 corpus IPC buffers differ"))?;
        if buffers.len() != 3
            || body_bytes
                > u64::try_from(maximum_body_bytes)
                    .map_err(|_| invalid("f16 corpus IPC buffers differ"))?
            || ![0, outer_validity_bytes].contains(
                &usize::try_from(buffers.get(0).length())
                    .map_err(|_| invalid("f16 corpus IPC buffers differ"))?,
            )
            || ![0, child_validity_bytes].contains(
                &usize::try_from(buffers.get(1).length())
                    .map_err(|_| invalid("f16 corpus IPC buffers differ"))?,
            )
            || usize::try_from(buffers.get(2).length()).ok() != Some(expected_value_bytes)
        {
            return Err(invalid("f16 corpus IPC buffers differ"));
        }
        for buffer in buffers {
            let start = u64::try_from(buffer.offset())
                .map_err(|_| invalid("f16 corpus IPC buffer differs"))?;
            let length = u64::try_from(buffer.length())
                .map_err(|_| invalid("f16 corpus IPC buffer differs"))?;
            if start.checked_add(length).is_none_or(|end| end > body_bytes) {
                return Err(invalid("f16 corpus IPC buffer differs"));
            }
        }
        total_rows = total_rows
            .checked_add(u64::try_from(rows).unwrap())
            .ok_or_else(|| invalid("f16 corpus row count differs"))?;
        if total_rows > expected_rows {
            return Err(invalid("f16 corpus row count differs"));
        }
    }
    if total_rows != expected_rows {
        return Err(invalid("f16 corpus row count differs"));
    }
    Ok(())
}

pub(crate) fn read_v23_balanced_f16_rows(
    path: &Path,
    expected_rows: u64,
) -> Result<V23BalancedF16RowStream> {
    if expected_rows == 0 {
        return Err(invalid("f16 corpus expected rows is zero"));
    }
    regular_file(path)?;
    let mut file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    validate_v23_balanced_f16_ipc_layout(&mut file, expected_rows)?;
    let reader = FileReader::try_new(
        File::open(path).map_err(|source| BorsukError::Io {
            path: path.to_path_buf(),
            source,
        })?,
        None,
    )?;
    let schema = Schema::new(vec![Field::new(
        "row",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float16, false)),
            96,
        ),
        false,
    )]);
    if reader.schema().as_ref() != &schema {
        return Err(invalid("f16 corpus schema differs"));
    }
    Ok(V23BalancedF16RowStream {
        reader,
        pending: Vec::new().into_iter(),
        next_source_ordinal: 0,
        expected_rows,
        failed: false,
    })
}

pub(crate) struct V23BalancedCorpusPass<'a, I> {
    rows: I,
    model: &'a V23SupercellModel,
    pseudoqueries: &'a mut V23BalancedPseudoqueryAccumulator,
}

impl<I> Iterator for V23BalancedCorpusPass<'_, I>
where
    I: Iterator<Item = Result<V23BalancedTrainingRow>>,
{
    type Item = Result<V23RoutedRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = match self.rows.next()? {
            Ok(row) => row,
            Err(error) => return Some(Err(error)),
        };
        if let Err(error) = self.pseudoqueries.consider(row.source_ordinal, &row.vector) {
            return Some(Err(error));
        }
        Some(
            route_v23_supercell_beam2(self.model, &row.vector, row.source_ordinal).map(|routed| {
                V23RoutedRow {
                    supercell_ordinal: routed.primary_supercell,
                    runner_up_supercell_ordinal: routed.runner_up_supercell,
                    source_ordinal: row.source_ordinal,
                    vector: row.vector,
                }
            }),
        )
    }
}

pub(crate) fn route_v23_balanced_corpus<'a, R>(
    rows: R,
    model: &'a V23SupercellModel,
    pseudoqueries: &'a mut V23BalancedPseudoqueryAccumulator,
) -> V23BalancedCorpusPass<'a, R::IntoIter>
where
    R: IntoIterator<Item = Result<V23BalancedTrainingRow>>,
{
    V23BalancedCorpusPass {
        rows: rows.into_iter(),
        model,
        pseudoqueries,
    }
}

pub(crate) struct V23BalancedRoutePass<'a, I> {
    rows: I,
    model: &'a V23SupercellModel,
}

impl<I> Iterator for V23BalancedRoutePass<'_, I>
where
    I: Iterator<Item = Result<V23BalancedTrainingRow>>,
{
    type Item = Result<V23RoutedRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = match self.rows.next()? {
            Ok(row) => row,
            Err(error) => return Some(Err(error)),
        };
        Some(
            route_v23_supercell_beam2(self.model, &row.vector, row.source_ordinal).map(|routed| {
                V23RoutedRow {
                    supercell_ordinal: routed.primary_supercell,
                    runner_up_supercell_ordinal: routed.runner_up_supercell,
                    source_ordinal: row.source_ordinal,
                    vector: row.vector,
                }
            }),
        )
    }
}

pub(crate) fn route_v23_balanced_rows<'a, R>(
    rows: R,
    model: &'a V23SupercellModel,
) -> V23BalancedRoutePass<'a, R::IntoIter>
where
    R: IntoIterator<Item = Result<V23BalancedTrainingRow>>,
{
    V23BalancedRoutePass {
        rows: rows.into_iter(),
        model,
    }
}

pub(crate) struct V23BalancedPrimaryConstructionRequest<'a> {
    pub(crate) corpus: &'a Path,
    pub(crate) rows: u64,
    pub(crate) reservoir_rows: usize,
    pub(crate) pseudoquery_rows: usize,
    pub(crate) supercells: u32,
    pub(crate) primary_rows_per_page: u16,
    pub(crate) seed: u64,
    pub(crate) workers: usize,
    pub(crate) run_rows: usize,
    pub(crate) scratch: &'a Path,
    pub(crate) row_pages_output: &'a Path,
    pub(crate) row_pages_uri: &'a str,
}

pub(crate) struct V23BalancedPrimaryConstruction {
    pub(crate) model: V23SupercellModel,
    pub(crate) primary: V23PrimaryPageBuild,
    pub(crate) pseudoquery_evidence: Vec<V23BalancedPseudoqueryEvidence>,
}

pub(crate) struct V23BalancedReplicaConstructionRequest<'a> {
    pub(crate) corpus: &'a Path,
    pub(crate) rows: u64,
    pub(crate) model: &'a V23SupercellModel,
    pub(crate) primary_path: &'a Path,
    pub(crate) primary: &'a V23PrimaryPageBuild,
    pub(crate) outputs: &'a [V23ReplicaArmOutput],
    pub(crate) scratch: &'a Path,
    pub(crate) run_rows: usize,
}

pub(crate) fn build_v23_balanced_primary(
    request: V23BalancedPrimaryConstructionRequest<'_>,
) -> Result<V23BalancedPrimaryConstruction> {
    if request.rows == 0
        || request.reservoir_rows == 0
        || request.pseudoquery_rows == 0
        || request.supercells == 0
        || request.primary_rows_per_page == 0
        || request.run_rows == 0
        || request.workers == 0
    {
        return Err(invalid("primary construction shape differs"));
    }
    let reservoir = sample_v23_balanced_reservoir(
        read_v23_balanced_f16_rows(request.corpus, request.rows)?,
        request.reservoir_rows,
        request.seed,
    )?;
    let model = train_v23_balanced_tree(
        reservoir,
        request.pseudoquery_rows,
        usize::try_from(request.supercells)
            .map_err(|_| invalid("primary supercell count differs"))?,
        request.seed,
        request.workers,
        request.run_rows,
    )?;
    let mut pseudoqueries = V23BalancedPseudoqueryAccumulator::new(model.pseudoqueries().to_vec())?;
    let primary = build_v23_primary_pages(
        route_v23_balanced_corpus(
            read_v23_balanced_f16_rows(request.corpus, request.rows)?,
            &model,
            &mut pseudoqueries,
        ),
        V23PageBuildShape {
            supercells: request.supercells,
            primary_rows_per_page: request.primary_rows_per_page,
            run_rows: request.run_rows,
        },
        request.workers,
        request.scratch,
        request.row_pages_output,
        request.row_pages_uri,
    )?;
    let pseudoquery_evidence = pseudoqueries.finish()?;
    Ok(V23BalancedPrimaryConstruction {
        model,
        primary,
        pseudoquery_evidence,
    })
}

pub(crate) fn build_v23_balanced_replicas(
    request: V23BalancedReplicaConstructionRequest<'_>,
) -> Result<Vec<V23ReplicaArmBuild>> {
    build_v23_replica_arms(
        || {
            Ok(route_v23_balanced_rows(
                read_v23_balanced_f16_rows(request.corpus, request.rows)?,
                request.model,
            ))
        },
        V23ReplicaBuildInputs {
            primary_path: request.primary_path,
            primary_identity: &request.primary.row_pages,
            supercells: &request.primary.supercells,
            pages: &request.primary.pages,
        },
        request.outputs,
        request.scratch,
        request.run_rows,
    )
}

fn output_identity(path: &Path, uri: String, role: &str) -> Result<V23BalancedIdentity> {
    regular_file(path)?;
    let mut file = File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let encoded_bytes = file
        .metadata()
        .map_err(|source| BorsukError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let identity = V23BalancedIdentity {
        role: role.to_owned(),
        uri,
        digest_algorithm: "sha256".to_owned(),
        digest: format!("{:x}", digest.finalize()),
        encoded_bytes,
    };
    validate_v23_balanced_identity(&identity)?;
    Ok(identity)
}

pub(crate) fn write_v23_balanced_construction_outputs(
    output_directory: &Path,
    output_uri_prefix: &str,
    primary: &V23BalancedPrimaryConstruction,
    replicas: &[V23ReplicaArmBuild],
) -> Result<Vec<V23BalancedIdentity>> {
    if !output_uri_prefix.starts_with("s3://")
        || !output_uri_prefix.ends_with('/')
        || replicas.len() != 3
        || replicas
            .iter()
            .zip(expected_arms())
            .any(|(build, expected)| build.config != expected)
    {
        return Err(invalid("construction output authority differs"));
    }
    let expected_existing = [
        "row-pages-primary.parquet",
        "row-pages-amp-1125.parquet",
        "row-pages-amp-1250.parquet",
        "row-pages-amp-1500.parquet",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let observed_existing = output_directory
        .read_dir()
        .map_err(|source| BorsukError::Io {
            path: output_directory.to_path_buf(),
            source,
        })?
        .map(|entry| {
            entry
                .map_err(|source| BorsukError::Io {
                    path: output_directory.to_path_buf(),
                    source,
                })?
                .file_name()
                .into_string()
                .map_err(|_| invalid("construction output basename differs"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if observed_existing != expected_existing {
        return Err(invalid("construction output inventory differs"));
    }

    let tree_path = output_directory.join("balanced-tree.bin");
    let tree_bytes = primary.model.canonical_tree_bytes()?;
    fs::write(&tree_path, tree_bytes).map_err(|source| BorsukError::Io {
        path: tree_path.clone(),
        source,
    })?;
    let supercells_path = output_directory.join("supercells.parquet");
    write_v23_supercells(&supercells_path, &primary.primary.supercells)?;
    let primary_pages_path = output_directory.join("pages-primary.parquet");
    write_v23_pages(&primary_pages_path, &primary.primary.pages, 0)?;
    for replica in replicas {
        write_v23_pages(
            &output_directory.join(format!("pages-{}.parquet", replica.config.name)),
            &replica.pages,
            replica.config.replicas_per_page,
        )?;
    }

    let artifacts = [
        ("balanced-tree", "balanced-tree.bin"),
        ("supercells-parquet", "supercells.parquet"),
        ("pages-primary-parquet", "pages-primary.parquet"),
        ("row-pages-primary-parquet", "row-pages-primary.parquet"),
        ("pages-amp-1125-parquet", "pages-amp-1125.parquet"),
        ("row-pages-amp-1125-parquet", "row-pages-amp-1125.parquet"),
        ("pages-amp-1250-parquet", "pages-amp-1250.parquet"),
        ("row-pages-amp-1250-parquet", "row-pages-amp-1250.parquet"),
        ("pages-amp-1500-parquet", "pages-amp-1500.parquet"),
        ("row-pages-amp-1500-parquet", "row-pages-amp-1500.parquet"),
    ];
    let identities = artifacts
        .into_iter()
        .map(|(role, basename)| {
            output_identity(
                &output_directory.join(basename),
                format!("{output_uri_prefix}{basename}"),
                role,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    if identities[3] != primary.primary.row_pages
        || replicas
            .iter()
            .zip([5_usize, 7, 9])
            .any(|(replica, index)| replica.row_pages != identities[index])
    {
        return Err(invalid("construction assignment identity differs"));
    }
    Ok(identities)
}

pub(crate) fn build_v23_balanced_pseudoquery_samples(
    primary: &V23BalancedPrimaryConstruction,
    replica: &V23ReplicaArmBuild,
    assignment_path: &Path,
    page_budget: V23BalancedPageBudget,
) -> Result<Vec<V23BalancedSample>> {
    let selected_arm = balanced_replica_arm(replica)?;
    let truth = read_v23_balanced_pseudoquery_truth(primary, replica, assignment_path)?;
    let geometry = prepare_v23_balanced_serving_geometry(
        &primary.primary.supercells,
        &replica.pages,
        selected_arm,
    )?;
    build_v23_balanced_pseudoquery_samples_from_truth(primary, &truth, &geometry, page_budget)
}

fn balanced_replica_arm(replica: &V23ReplicaArmBuild) -> Result<V23BalancedArm> {
    Ok(match replica.config.name.as_str() {
        "amp-1125" if replica.config == expected_arms()[0] => V23BalancedArm::Amp1125,
        "amp-1250" if replica.config == expected_arms()[1] => V23BalancedArm::Amp1250,
        "amp-1500" if replica.config == expected_arms()[2] => V23BalancedArm::Amp1500,
        _ => return Err(invalid("pseudoquery arm authority differs")),
    })
}

fn read_v23_balanced_pseudoquery_truth(
    primary: &V23BalancedPrimaryConstruction,
    replica: &V23ReplicaArmBuild,
    assignment_path: &Path,
) -> Result<Vec<Vec<Vec<u32>>>> {
    balanced_replica_arm(replica)?;
    let queries = primary.model.pseudoqueries();
    let evidence = &primary.pseudoquery_evidence;
    if queries.is_empty()
        || queries.len() != evidence.len()
        || queries.iter().zip(evidence).any(|(query, sample)| {
            query.0 != sample.query_source_ordinal
                || sample.scored_dimensions == 0
                || sample.scalar_control_dimensions != 10 * DIMENSIONS
                || !sample.scalar_simd_equal
                || sample
                    .neighbor_source_ordinals
                    .contains(&sample.query_source_ordinal)
                || sample
                    .neighbor_source_ordinals
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != 10
        })
    {
        return Err(invalid("pseudoquery evidence authority differs"));
    }
    let requested = evidence
        .iter()
        .flat_map(|sample| sample.neighbor_source_ordinals)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let page_count = u32::try_from(replica.pages.len())
        .map_err(|_| invalid("pseudoquery page count overflows"))?;
    let requested_assignments = read_v23_row_page_assignments(
        assignment_path,
        &replica.row_pages,
        &replica.row_pages.role,
        page_count,
        &requested,
    )?;
    evidence
        .iter()
        .map(|sample| {
            sample
                .neighbor_source_ordinals
                .iter()
                .map(|source_ordinal| {
                    let index = requested
                        .binary_search(source_ordinal)
                        .map_err(|_| invalid("pseudoquery neighbor assignment is missing"))?;
                    Ok(requested_assignments[index].clone())
                })
                .collect()
        })
        .collect()
}

fn build_v23_balanced_pseudoquery_samples_from_truth(
    primary: &V23BalancedPrimaryConstruction,
    truth: &[Vec<Vec<u32>>],
    geometry: &V23BalancedServingGeometry,
    page_budget: V23BalancedPageBudget,
) -> Result<Vec<V23BalancedSample>> {
    if truth.len() != primary.model.pseudoqueries().len() {
        return Err(invalid("pseudoquery truth cohort differs"));
    }
    primary
        .model
        .pseudoqueries()
        .iter()
        .zip(truth)
        .enumerate()
        .map(|(query_index, (query, assignments))| {
            build_v23_balanced_sample(
                u32::try_from(query_index).map_err(|_| invalid("pseudoquery index overflows"))?,
                &query.1,
                assignments.clone(),
                geometry,
                page_budget,
            )
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23BalancedPseudoqueryLadder {
    pub(crate) pairs: Vec<V23BalancedPseudoqueryPair>,
    pub(crate) selected: Option<V23BalancedSelectedPairEvidence>,
}

pub(crate) fn evaluate_v23_balanced_pseudoquery_ladder(
    primary: &V23BalancedPrimaryConstruction,
    replicas: &[V23ReplicaArmBuild],
    assignment_paths: &[PathBuf; 3],
) -> Result<V23BalancedPseudoqueryLadder> {
    evaluate_v23_balanced_pseudoquery_ladder_for_expected_count(
        primary,
        replicas,
        assignment_paths,
        1_024,
    )
}

pub(crate) fn evaluate_v23_balanced_pseudoquery_ladder_for_expected_count(
    primary: &V23BalancedPrimaryConstruction,
    replicas: &[V23ReplicaArmBuild],
    assignment_paths: &[PathBuf; 3],
    expected_count: usize,
) -> Result<V23BalancedPseudoqueryLadder> {
    if replicas.len() != 3
        || replicas.iter().enumerate().any(|(index, replica)| {
            balanced_replica_arm(replica).ok()
                != Some(
                    [
                        V23BalancedArm::Amp1125,
                        V23BalancedArm::Amp1250,
                        V23BalancedArm::Amp1500,
                    ][index],
                )
        })
    {
        return Err(invalid("pseudoquery ladder arm order differs"));
    }
    let mut arms = Vec::with_capacity(3);
    for (replica, assignment_path) in replicas.iter().zip(assignment_paths) {
        let selected_arm = balanced_replica_arm(replica)?;
        let truth = read_v23_balanced_pseudoquery_truth(primary, replica, assignment_path)?;
        let geometry = prepare_v23_balanced_serving_geometry(
            &primary.primary.supercells,
            &replica.pages,
            selected_arm,
        )?;
        arms.push((selected_arm, truth, geometry));
    }
    let mut pairs = Vec::with_capacity(9);
    for page_budget in PAGE_BUDGETS.map(V23BalancedPageBudget::new) {
        let page_budget = page_budget?;
        for (selected_arm, truth, geometry) in &arms {
            let samples = build_v23_balanced_pseudoquery_samples_from_truth(
                primary,
                truth,
                geometry,
                page_budget,
            )?;
            pairs.push(evaluate_v23_balanced_pseudoquery_pair_for_expected_count(
                V23BalancedSelectedPair {
                    page_budget,
                    arm: *selected_arm,
                },
                &samples,
                geometry,
                expected_count,
            )?);
        }
    }
    let selected = classify_v23_balanced_pair_ladder(&pairs)?;
    Ok(V23BalancedPseudoqueryLadder { pairs, selected })
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
        selected_pair: None,
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
        || (receipt.stop.is_some()
            && (!receipt.outputs.is_empty() || receipt.selected_pair.is_some()))
        || (receipt.outputs.is_empty() && receipt.selected_pair.is_some())
        || (!receipt.outputs.is_empty() && receipt.selected_pair.is_none())
        || receipt
            .selected_pair
            .is_some_and(|selected| V23BalancedPageBudget::new(selected.page_budget.get()).is_err())
    {
        return Err(invalid("receipt authority differs"));
    }
    validate_identity_list(&receipt.ordered_inputs)?;
    validate_identity_list(&receipt.outputs)?;
    if !receipt.outputs.is_empty()
        && receipt
            .outputs
            .iter()
            .map(|identity| identity.role.as_str())
            .ne(expected_output_roles())
    {
        return Err(invalid("receipt output roles differ"));
    }
    let value = serde_json::to_value(receipt)
        .map_err(|error| invalid(&format!("receipt serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("canonical JSON failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::File, path::Path, sync::Arc};

    use arrow_array::{FixedSizeListArray, Float16Array, RecordBatch};
    use arrow_ipc::writer::FileWriter;
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use sha2::{Digest, Sha256};

    use super::{
        V23BalancedArm, V23BalancedArmConfig, V23BalancedIdentity, V23BalancedLocalMode,
        V23BalancedLocalRequest, V23BalancedManifest, V23BalancedPageBudget,
        V23BalancedPrimaryConstructionRequest, V23BalancedReceipt,
        V23BalancedReplicaConstructionRequest, V23BalancedSelectedPair, V23BalancedStop,
        build_v23_balanced_primary, build_v23_balanced_pseudoquery_samples,
        build_v23_balanced_replicas, canonical_v23_balanced_receipt_bytes,
        evaluate_v23_balanced_pseudoquery_ladder_for_expected_count, expected_output_roles,
        project_v23_balanced_shape, read_v23_balanced_f16_rows, route_v23_balanced_corpus,
        run_v23_balanced_local_request, validate_v23_balanced_manifest,
        write_v23_balanced_construction_outputs,
    };
    use crate::{
        v23_balanced_pages_build::V23ReplicaArmOutput,
        v23_balanced_pages_eval::V23BalancedPseudoqueryAccumulator,
        v23_balanced_pages_train::{V23BalancedTrainingRow, train_v23_balanced_tree},
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

    fn write_f16_rows(path: &Path, child_name: &str, rows: &[[f16; 96]]) {
        let child = Arc::new(Field::new(child_name, DataType::Float16, false));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "row",
            DataType::FixedSizeList(child.clone(), 96),
            false,
        )]));
        let values = Float16Array::from_iter_values(rows.iter().flatten().copied());
        let vectors = FixedSizeListArray::try_new(child, 96, Arc::new(values), None).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();
        let mut writer = FileWriter::try_new(File::create(path).unwrap(), &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    fn manifest_fixture(rows: u64) -> V23BalancedManifest {
        V23BalancedManifest {
            schema: "borsuk-v23-balanced-page-manifest-v3".to_owned(),
            claim_eligible: false,
            source_commit: sha256(0x11).chars().take(40).collect(),
            source_archive_sha256: sha256(0x12),
            dataset_id: "deep-image-96".to_owned(),
            deterministic_seed: 0x6a09_e667_f3bc_c909,
            worker_threads: 4,
            sort_run_rows: 262_144,
            scratch_bytes_limit: 64 * 1024 * 1024 * 1024,
            output_uri_prefix: "s3://borsuk-v23-eu-west-1/balanced/attempt-0001/".to_owned(),
            rows,
            dimensions: 96,
            supercell_target_rows: 12_288,
            primary_rows_per_page: 384,
            top_supercells: 96,
            page_budgets: vec![
                V23BalancedPageBudget::new(8).unwrap(),
                V23BalancedPageBudget::new(12).unwrap(),
                V23BalancedPageBudget::new(16).unwrap(),
            ],
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
                identity("query-parquet", 0x23),
                identity("neighbors-parquet", 0x24),
            ],
            output_roles: [
                "balanced-tree",
                "supercells-parquet",
                "pages-primary-parquet",
                "row-pages-primary-parquet",
                "pages-amp-1125-parquet",
                "row-pages-amp-1125-parquet",
                "pages-amp-1250-parquet",
                "row-pages-amp-1250-parquet",
                "pages-amp-1500-parquet",
                "row-pages-amp-1500-parquet",
                "development-result",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
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
        changed.page_budgets.swap(0, 1);
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.page_budgets[2] = V23BalancedPageBudget(15);
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.ordered_inputs[0].digest_algorithm = "blake3".to_owned();
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.deterministic_seed ^= 1;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.worker_threads += 1;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.sort_run_rows -= 1;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.scratch_bytes_limit -= 1;
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.output_uri_prefix = "file:///tmp/balanced/".to_owned();
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.output_uri_prefix.pop();
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.output_roles.swap(0, 1);
        mutations.push(changed);
        let mut changed = valid.clone();
        changed.output_roles.pop();
        mutations.push(changed);
        let mut changed = valid.clone();
        changed
            .ordered_inputs
            .push(identity("unexpected-construction-input", 0x25));
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
            schema: "borsuk-v23-balanced-page-receipt-v2".to_owned(),
            claim_eligible: false,
            manifest_sha256: sha256(0x31),
            ordered_inputs: manifest_fixture(100_000_000).ordered_inputs,
            outputs: expected_output_roles()
                .into_iter()
                .enumerate()
                .map(|(index, role)| identity(role, 0x32 + u8::try_from(index).unwrap()))
                .collect(),
            selected_pair: Some(V23BalancedSelectedPair {
                page_budget: V23BalancedPageBudget::new(12).unwrap(),
                arm: V23BalancedArm::Amp1250,
            }),
            stop: None,
        };
        let bytes = canonical_v23_balanced_receipt_bytes(&receipt).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut changed = receipt.clone();
        changed.claim_eligible = true;
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
        let mut changed = receipt.clone();
        changed.outputs.pop();
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
        let mut changed = receipt.clone();
        changed.outputs.swap(0, 1);
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
        let mut changed = receipt.clone();
        changed.selected_pair.as_mut().unwrap().page_budget = V23BalancedPageBudget(9);
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
        let mut changed = receipt.clone();
        changed.selected_pair = None;
        assert!(canonical_v23_balanced_receipt_bytes(&changed).is_err());
        let mut changed = receipt;
        changed.stop = Some(V23BalancedStop::Resource);
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
        let query = b"query\n";
        let neighbors = b"neighbors\n";
        fs::write(input.join("source-shard-manifest.json"), source_manifest).unwrap();
        fs::write(input.join("f16-control.arrow"), f16_control).unwrap();
        fs::write(input.join("query.parquet"), query).unwrap();
        fs::write(input.join("neighbors.parquet"), neighbors).unwrap();
        let mut manifest = manifest_fixture(100_000_000);
        manifest.ordered_inputs = vec![
            identity_for_bytes("source-shard-manifest", source_manifest),
            identity_for_bytes("f16-control", f16_control),
            identity_for_bytes("query-parquet", query),
            identity_for_bytes("neighbors-parquet", neighbors),
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
        let query = b"query\n";
        let neighbors = b"neighbors\n";
        fs::write(input.join("source-shard-manifest.json"), source_manifest).unwrap();
        fs::write(input.join("f16-control.arrow"), f16_control).unwrap();
        fs::write(input.join("query.parquet"), query).unwrap();
        fs::write(input.join("neighbors.parquet"), neighbors).unwrap();
        let mut manifest = manifest_fixture(100_000_000);
        manifest.ordered_inputs = vec![
            identity_for_bytes("source-shard-manifest", source_manifest),
            identity_for_bytes("f16-control", f16_control),
            identity_for_bytes("query-parquet", query),
            identity_for_bytes("neighbors-parquet", neighbors),
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
        fs::write(input.join("query.parquet"), b"query\n").unwrap();
        fs::write(input.join("neighbors.parquet"), b"neighbors\n").unwrap();
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

    #[test]
    fn v23_balanced_local_f16_stream_rejects_schema_values_and_row_count_drift() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("f16-control.arrow");
        let rows = [
            std::array::from_fn(|dimension| f16::from_f32((dimension + 1) as f32)),
            std::array::from_fn(|dimension| f16::from_f32((dimension + 2) as f32)),
        ];
        write_f16_rows(&path, "element", &rows);
        let decoded = read_v23_balanced_f16_rows(&path, 2)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            decoded
                .iter()
                .map(|row| row.source_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(decoded[0].vector[0], 1.0);
        assert_eq!(decoded[1].vector[95], 97.0);

        write_f16_rows(&path, "item", &rows);
        assert!(read_v23_balanced_f16_rows(&path, 2).is_err());
        let mut invalid = rows;
        invalid[1][17] = f16::NAN;
        write_f16_rows(&path, "element", &invalid);
        assert!(
            read_v23_balanced_f16_rows(&path, 2)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .is_err()
        );
        write_f16_rows(&path, "element", &rows);
        assert!(read_v23_balanced_f16_rows(&path, 0).is_err());
        assert!(read_v23_balanced_f16_rows(&path, 3).is_err());
        assert!(read_v23_balanced_f16_rows(&path, 1).is_err());
    }

    #[test]
    fn v23_balanced_local_f16_stream_rejects_oversized_batch_before_iteration() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("f16-control.arrow");
        let row = std::array::from_fn(|dimension| f16::from_f32((dimension + 1) as f32));
        let rows = vec![row; 262_145];
        write_f16_rows(&path, "element", &rows);

        assert!(read_v23_balanced_f16_rows(&path, 262_145).is_err());
    }

    #[test]
    fn v23_balanced_local_corpus_pass_routes_and_accumulates_without_buffering() {
        let rows = (0_u64..64)
            .map(|source_ordinal| {
                let cluster = usize::try_from(source_ordinal % 8).unwrap();
                let mut vector = [0.0_f32; 96];
                vector[cluster] = 1.0;
                vector[8 + cluster] = 0.25 + source_ordinal as f32 * 0.0001;
                V23BalancedTrainingRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let model = train_v23_balanced_tree(rows.clone(), 8, 8, 0x1234_5678, 2, 7).unwrap();
        let mut accumulator =
            V23BalancedPseudoqueryAccumulator::new(model.pseudoqueries().to_vec()).unwrap();
        let routed = route_v23_balanced_corpus(rows.into_iter().map(Ok), &model, &mut accumulator)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(routed.len(), 64);
        assert_eq!(routed[0].source_ordinal, 0);
        assert_eq!(routed[63].source_ordinal, 63);
        assert!(routed.iter().all(|row| {
            row.supercell_ordinal < 8
                && row.runner_up_supercell_ordinal < 8
                && row.supercell_ordinal != row.runner_up_supercell_ordinal
        }));
        let evidence = accumulator.finish().unwrap();
        assert_eq!(evidence.len(), 8);
        assert!(evidence.iter().all(|sample| {
            sample.scored_dimensions == 64 * 96
                && !sample
                    .neighbor_source_ordinals
                    .contains(&sample.query_source_ordinal)
        }));
        assert!(accumulator.maximum_retained_candidates() <= 8 * 10);
    }

    #[test]
    fn v23_balanced_local_primary_pipeline_streams_training_routing_and_parquet() {
        let directory = tempfile::tempdir().unwrap();
        let corpus = directory.path().join("f16-control.arrow");
        let scratch = directory.path().join("scratch");
        let output = directory.path().join("row-pages-primary.parquet");
        fs::create_dir(&scratch).unwrap();
        let rows = (0_u64..64)
            .map(|source_ordinal| {
                let cluster = usize::try_from(source_ordinal % 8).unwrap();
                let mut vector = [f16::ZERO; 96];
                vector[cluster] = f16::ONE;
                vector[8 + cluster] = f16::from_f32(0.25 + source_ordinal as f32 * 0.0001);
                vector
            })
            .collect::<Vec<_>>();
        write_f16_rows(&corpus, "element", &rows);

        let built = build_v23_balanced_primary(V23BalancedPrimaryConstructionRequest {
            corpus: &corpus,
            rows: 64,
            reservoir_rows: 64,
            pseudoquery_rows: 8,
            supercells: 2,
            primary_rows_per_page: 4,
            seed: 0x1234_5678,
            workers: 2,
            run_rows: 7,
            scratch: &scratch,
            row_pages_output: &output,
            row_pages_uri: "s3://borsuk-v23-eu-west-1/reduced/row-pages-primary.parquet",
        })
        .unwrap();

        assert_eq!(built.primary.source_rows, 64);
        assert_eq!(built.primary.supercells.len(), 2);
        assert!(built.primary.pages.len() >= 16);
        assert_eq!(built.pseudoquery_evidence.len(), 8);
        assert_eq!(built.model.pseudoqueries().len(), 8);
        assert!(output.is_file());
        assert!(scratch.read_dir().unwrap().next().is_none());
    }

    #[test]
    fn v23_balanced_local_replica_pipeline_replays_corpus_for_all_exact_arms() {
        let directory = tempfile::tempdir().unwrap();
        let corpus = directory.path().join("f16-control.arrow");
        let primary_scratch = directory.path().join("primary-scratch");
        let replica_scratch = directory.path().join("replica-scratch");
        let output_directory = directory.path().join("output");
        let primary_output = output_directory.join("row-pages-primary.parquet");
        fs::create_dir(&primary_scratch).unwrap();
        fs::create_dir(&replica_scratch).unwrap();
        fs::create_dir(&output_directory).unwrap();
        let rows = (0_u64..64)
            .map(|source_ordinal| {
                let cluster = usize::try_from(source_ordinal % 8).unwrap();
                let mut vector = [f16::ZERO; 96];
                vector[cluster] = f16::ONE;
                vector[8 + cluster] = f16::from_f32(0.25 + source_ordinal as f32 * 0.0001);
                vector
            })
            .collect::<Vec<_>>();
        write_f16_rows(&corpus, "element", &rows);
        let primary = build_v23_balanced_primary(V23BalancedPrimaryConstructionRequest {
            corpus: &corpus,
            rows: 64,
            reservoir_rows: 64,
            pseudoquery_rows: 8,
            supercells: 2,
            primary_rows_per_page: 4,
            seed: 0x1234_5678,
            workers: 2,
            run_rows: 7,
            scratch: &primary_scratch,
            row_pages_output: &primary_output,
            row_pages_uri: "s3://borsuk-v23-eu-west-1/reduced/row-pages-primary.parquet",
        })
        .unwrap();
        let outputs = [
            ("amp-1125", 1_125_000, 48_u16),
            ("amp-1250", 1_250_000, 96_u16),
            ("amp-1500", 1_500_000, 192_u16),
        ]
        .map(
            |(name, amplification_ppm, replicas_per_page)| V23ReplicaArmOutput {
                config: V23BalancedArmConfig {
                    name: name.to_owned(),
                    amplification_ppm,
                    replicas_per_page,
                },
                row_pages_path: output_directory.join(format!("row-pages-{name}.parquet")),
                row_pages_uri: format!(
                    "s3://borsuk-v23-eu-west-1/reduced/row-pages-{name}.parquet"
                ),
            },
        );

        let replicas = build_v23_balanced_replicas(V23BalancedReplicaConstructionRequest {
            corpus: &corpus,
            rows: 64,
            model: &primary.model,
            primary_path: &primary_output,
            primary: &primary.primary,
            outputs: &outputs,
            scratch: &replica_scratch,
            run_rows: 7,
        })
        .unwrap();

        assert_eq!(
            replicas
                .iter()
                .map(|arm| arm.replica_rows)
                .collect::<Vec<_>>(),
            [8, 16, 32]
        );
        assert!(outputs.iter().all(|output| output.row_pages_path.is_file()));
        assert!(replica_scratch.read_dir().unwrap().next().is_none());

        let identities = write_v23_balanced_construction_outputs(
            &output_directory,
            "s3://borsuk-v23-eu-west-1/reduced/",
            &primary,
            &replicas,
        )
        .unwrap();
        assert_eq!(
            identities
                .iter()
                .map(|identity| identity.role.as_str())
                .collect::<Vec<_>>(),
            expected_output_roles()[..10]
        );
        assert_eq!(
            fs::read(output_directory.join("balanced-tree.bin")).unwrap(),
            primary.model.canonical_tree_bytes().unwrap()
        );
        assert_eq!(
            output_directory
                .read_dir()
                .unwrap()
                .collect::<std::io::Result<Vec<_>>>()
                .unwrap()
                .len(),
            10
        );

        let samples = build_v23_balanced_pseudoquery_samples(
            &primary,
            &replicas[0],
            &output_directory.join("row-pages-amp-1125.parquet"),
            V23BalancedPageBudget::new(8).unwrap(),
        )
        .unwrap();
        assert_eq!(samples.len(), 8);
        assert!(samples.iter().all(|sample| {
            sample.ground_truth_page_assignments.len() == 10
                && sample.selected_pages.len() == 8
                && sample.containment_page_universe.len() >= 8
        }));

        let assignment_paths = [
            output_directory.join("row-pages-amp-1125.parquet"),
            output_directory.join("row-pages-amp-1250.parquet"),
            output_directory.join("row-pages-amp-1500.parquet"),
        ];
        let ladder = evaluate_v23_balanced_pseudoquery_ladder_for_expected_count(
            &primary,
            &replicas,
            &assignment_paths,
            8,
        )
        .unwrap();
        assert_eq!(ladder.pairs.len(), 9);
        assert_eq!(
            ladder
                .pairs
                .iter()
                .map(|pair| (pair.selected_pair.page_budget.get(), pair.selected_pair.arm,))
                .collect::<Vec<_>>(),
            [8_u8, 12, 16]
                .into_iter()
                .flat_map(|budget| {
                    [
                        V23BalancedArm::Amp1125,
                        V23BalancedArm::Amp1250,
                        V23BalancedArm::Amp1500,
                    ]
                    .map(move |arm| (budget, arm))
                })
                .collect::<Vec<_>>()
        );
    }
}
