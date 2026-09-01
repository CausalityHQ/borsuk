use std::{cell::RefCell, collections::BTreeSet, fs, io::Read, path::PathBuf, rc::Rc, sync::Arc};

use arrow_array::{
    Array, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v23_incidence_eval::{
        V23IncidenceD2Authority, read_v23_incidence_d2_authority,
        read_v23_incidence_development_queries,
    },
    v23_incidence_tree::{V23IncidenceTrainingShape, decode_incidence_tree},
    v23_rabitq::{
        V23RaBitQManifest, V23RaBitQObjectIdentity, V23RaBitQPhase, V23RaBitQReceipt,
        V23RaBitQRunMode, canonical_v23_rabitq_manifest_bytes, canonical_v23_rabitq_receipt_bytes,
        read_v23_rabitq_receipt, validate_v23_rabitq_manifest,
    },
    v23_rabitq_arrow::{
        V23RaBitQGeometryBytes, V23RaBitQGeometryIdentities, V23RaBitQRowPlanes,
        read_v23_rabitq_f16_control, read_v23_rabitq_geometry, read_v23_rabitq_row_planes,
    },
    v23_rabitq_build::{V23RaBitQBuildRequest, V23RaBitQSourceRow, build_v23_rabitq_artifacts},
    v23_rabitq_eval::{
        V23RaBitQDevelopmentRequest, canonical_v23_rabitq_screen_result_bytes,
        detected_v23_rabitq_backend, evaluate_v23_rabitq_development,
    },
};

const INPUT_ROLES: [&str; 9] = [
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

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn lower_hex(value: &str, width: usize) -> bool {
    value.len() == width
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct V23RaBitQLocalObjectIdentity {
    pub role: String,
    pub uri: String,
    pub sha256: String,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23RaBitQLocalArtifactPaths {
    pub manifest: PathBuf,
    pub construction_receipt: PathBuf,
    pub incidence_tree: PathBuf,
    pub row_codes: PathBuf,
    pub leaf_offsets: PathBuf,
    pub centroids: PathBuf,
    pub rotation: PathBuf,
    pub f16_control: PathBuf,
    pub d2_report: PathBuf,
    pub query_parquet: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23RaBitQLocalRunRequest {
    pub paths: V23RaBitQLocalArtifactPaths,
    pub manifest_identity: V23RaBitQLocalObjectIdentity,
    pub registered_inputs: Vec<V23RaBitQLocalObjectIdentity>,
    pub execute_development: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23RaBitQConstructionLocalPaths {
    pub manifest: PathBuf,
    pub tree_receipt: PathBuf,
    pub incidence_tree: PathBuf,
    pub page_roster: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V23RaBitQConstructionLocalRunRequest {
    pub paths: V23RaBitQConstructionLocalPaths,
    pub manifest_identity: V23RaBitQLocalObjectIdentity,
    pub registered_inputs: Vec<V23RaBitQLocalObjectIdentity>,
    pub scratch_directory: PathBuf,
    pub output_directory: PathBuf,
    pub execute_construction: bool,
}

impl V23RaBitQLocalObjectIdentity {
    fn validate(&self, role: &str) -> Result<()> {
        if self.role != role
            || !self.uri.starts_with("s3://")
            || self.uri.trim_start_matches("s3://").is_empty()
            || !lower_hex(&self.sha256, 64)
            || self.encoded_bytes == 0
        {
            return Err(invalid("V23 RaBitQ local object identity differs"));
        }
        Ok(())
    }

    fn internal(&self) -> V23RaBitQObjectIdentity {
        V23RaBitQObjectIdentity {
            role: self.role.clone(),
            uri: self.uri.clone(),
            sha256: self.sha256.clone(),
            blake3: None,
            encoded_bytes: self.encoded_bytes,
        }
    }
}

impl V23RaBitQLocalArtifactPaths {
    fn ordered(&self) -> [&PathBuf; 10] {
        [
            &self.manifest,
            &self.construction_receipt,
            &self.incidence_tree,
            &self.row_codes,
            &self.leaf_offsets,
            &self.centroids,
            &self.rotation,
            &self.f16_control,
            &self.d2_report,
            &self.query_parquet,
        ]
    }

    fn input_path(&self, role: &str) -> Result<&PathBuf> {
        match role {
            "construction-receipt" => Ok(&self.construction_receipt),
            "incidence-tree" => Ok(&self.incidence_tree),
            "row-codes" => Ok(&self.row_codes),
            "leaf-offsets" => Ok(&self.leaf_offsets),
            "centroids" => Ok(&self.centroids),
            "rotation" => Ok(&self.rotation),
            "f16-control" => Ok(&self.f16_control),
            "d2-report" => Ok(&self.d2_report),
            "query-parquet" => Ok(&self.query_parquet),
            _ => Err(invalid("V23 RaBitQ local role differs")),
        }
    }
}

fn read_authenticated(
    path: &PathBuf,
    identity: &V23RaBitQLocalObjectIdentity,
    role: &str,
) -> Result<Vec<u8>> {
    identity.validate(role)?;
    if !path.is_absolute() || !path.is_file() {
        return Err(invalid("V23 RaBitQ local artifact path differs"));
    }
    let bytes = fs::read(path).map_err(|error| {
        invalid(&format!(
            "V23 RaBitQ local artifact read failed for {}: {error}",
            path.display()
        ))
    })?;
    if identity.encoded_bytes != bytes.len() as u64
        || identity.sha256 != format!("{:x}", Sha256::digest(&bytes))
    {
        return Err(invalid("V23 RaBitQ local artifact byte authority differs"));
    }
    Ok(bytes)
}

fn read_manifest(
    path: &PathBuf,
    identity: &V23RaBitQLocalObjectIdentity,
) -> Result<(V23RaBitQManifest, Vec<u8>)> {
    let bytes = read_authenticated(path, identity, "manifest")?;
    let manifest: V23RaBitQManifest = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(&format!("V23 RaBitQ manifest JSON differs: {error}")))?;
    validate_v23_rabitq_manifest(&manifest)?;
    if canonical_v23_rabitq_manifest_bytes(&manifest)? != bytes {
        return Err(invalid("V23 RaBitQ manifest canonical bytes differ"));
    }
    Ok((manifest, bytes))
}

fn validate_request(request: &V23RaBitQLocalRunRequest) -> Result<Vec<V23RaBitQObjectIdentity>> {
    if !request.execute_development || request.registered_inputs.len() != INPUT_ROLES.len() {
        return Err(invalid("V23 RaBitQ local execution was not authorized"));
    }
    request.manifest_identity.validate("manifest")?;
    let mut paths = BTreeSet::new();
    if request
        .paths
        .ordered()
        .iter()
        .any(|path| !path.is_absolute() || !paths.insert(path.as_path()))
    {
        return Err(invalid("V23 RaBitQ local artifact paths overlap"));
    }
    let mut uris = BTreeSet::new();
    if !uris.insert(request.manifest_identity.uri.as_str()) {
        return Err(invalid("V23 RaBitQ local object URIs overlap"));
    }
    let mut internal = Vec::with_capacity(INPUT_ROLES.len());
    for (identity, role) in request.registered_inputs.iter().zip(INPUT_ROLES) {
        identity.validate(role)?;
        if !uris.insert(identity.uri.as_str()) {
            return Err(invalid("V23 RaBitQ local object URIs overlap"));
        }
        internal.push(identity.internal());
    }
    Ok(internal)
}

fn validate_v23_rabitq_d2_page_authority(
    development_index_id: &str,
    construction_index_id: &str,
    construction_page_count: u32,
    construction_page_namespace_uri_prefix: Option<&String>,
    authority: &V23IncidenceD2Authority,
    rows: &V23RaBitQRowPlanes,
) -> Result<()> {
    if development_index_id != authority.index_id
        || construction_index_id != authority.index_id
        || construction_page_count != authority.page_count
        || construction_page_namespace_uri_prefix.and_then(|prefix| prefix.strip_suffix('/'))
            != Some(authority.page_uri.as_str())
        || rows
            .primary_pages
            .iter()
            .any(|page| *page >= authority.page_count)
        || rows
            .replica_pages
            .iter()
            .any(|page| *page != u32::MAX && *page >= authority.page_count)
    {
        return Err(invalid("V23 RaBitQ D2 page authority differs"));
    }
    Ok(())
}

#[doc(hidden)]
pub fn run_v23_rabitq_local_request(request: V23RaBitQLocalRunRequest) -> Result<Vec<u8>> {
    let inputs = validate_request(&request)?;
    let (manifest, _) = read_manifest(&request.paths.manifest, &request.manifest_identity)?;
    if manifest.run_mode != V23RaBitQRunMode::Execute(V23RaBitQPhase::Development)
        || manifest.registered_inputs != inputs
    {
        return Err(invalid("V23 RaBitQ local manifest binding differs"));
    }

    let mut role_bytes = Vec::with_capacity(INPUT_ROLES.len());
    for (identity, role) in request.registered_inputs.iter().zip(INPUT_ROLES) {
        role_bytes.push(read_authenticated(
            request.paths.input_path(role)?,
            identity,
            role,
        )?);
    }
    let receipt = read_v23_rabitq_receipt(&role_bytes[0])?;
    if receipt.run_mode != V23RaBitQRunMode::Execute(V23RaBitQPhase::Construction)
        || receipt.outputs != inputs[2..7]
    {
        return Err(invalid("V23 RaBitQ construction receipt binding differs"));
    }
    let tree = decode_incidence_tree(&role_bytes[1])?;
    if tree.shape != V23IncidenceTrainingShape::PRODUCTION {
        return Err(invalid("V23 RaBitQ incidence tree shape differs"));
    }
    let row_code_bytes = std::mem::take(&mut role_bytes[2]);
    let rows = read_v23_rabitq_row_planes(&row_code_bytes, &inputs[2])?;
    drop(row_code_bytes);
    let geometry = read_v23_rabitq_geometry(
        &V23RaBitQGeometryBytes {
            leaf_offsets: std::mem::take(&mut role_bytes[3]),
            centroids: std::mem::take(&mut role_bytes[4]),
            rotation: std::mem::take(&mut role_bytes[5]),
        },
        &V23RaBitQGeometryIdentities {
            leaf_offsets: inputs[3].clone(),
            centroids: inputs[4].clone(),
            rotation: inputs[5].clone(),
        },
        receipt.manifest.expected_unique_rows,
    )?;
    let f16_control_bytes = std::mem::take(&mut role_bytes[6]);
    let exact_rows =
        read_v23_rabitq_f16_control(&f16_control_bytes, &inputs[6], rows.sign_codes.len())?;
    drop(f16_control_bytes);
    let d2_report_bytes = std::mem::take(&mut role_bytes[7]);
    let d2_authority = read_v23_incidence_d2_authority(&d2_report_bytes)?;
    drop(d2_report_bytes);
    validate_v23_rabitq_d2_page_authority(
        &manifest.index_id,
        &receipt.manifest.index_id,
        receipt.manifest.expected_pages,
        receipt.manifest.page_namespace_uri_prefix.as_ref(),
        &d2_authority,
        &rows,
    )?;
    let query_bytes = std::mem::take(&mut role_bytes[8]);
    let queries = read_v23_incidence_development_queries(&query_bytes)?;
    drop(query_bytes);
    drop(role_bytes);
    let result = evaluate_v23_rabitq_development(V23RaBitQDevelopmentRequest {
        source_commit: manifest.source_commit,
        source_archive_sha256: manifest.source_archive_sha256,
        index_id: manifest.index_id,
        inputs: &inputs,
        tree: &tree,
        geometry: &geometry,
        rows: &rows,
        exact_rows: &exact_rows,
        queries: &queries,
        truth: &d2_authority.truth,
        backend: detected_v23_rabitq_backend()?,
    })?;
    canonical_v23_rabitq_screen_result_bytes(&result, &inputs)
}

fn occurrence_schema() -> Schema {
    Schema::new(vec![
        Field::new("canonical_record_id", DataType::Binary, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            ),
            false,
        ),
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("is_primary", DataType::Boolean, false),
    ])
}

struct OccurrenceState {
    batch: Option<RecordBatch>,
    row: usize,
    page_seen: Vec<bool>,
    pages_seen: usize,
}

struct OccurrenceReader<R: Read> {
    reader: StreamReader<R>,
    state: Rc<RefCell<OccurrenceState>>,
}

impl<R: Read> OccurrenceReader<R> {
    fn new(reader: R, expected_pages: u32) -> Result<(Self, Rc<RefCell<OccurrenceState>>)> {
        let reader = StreamReader::try_new(reader, None)?;
        if reader.schema().as_ref() != &occurrence_schema() {
            return Err(invalid("V23 RaBitQ occurrence Arrow schema differs"));
        }
        let state = Rc::new(RefCell::new(OccurrenceState {
            batch: None,
            row: 0,
            page_seen: vec![false; expected_pages as usize],
            pages_seen: 0,
        }));
        Ok((
            Self {
                reader,
                state: Rc::clone(&state),
            },
            state,
        ))
    }

    fn next_batch(&mut self) -> Result<bool> {
        let Some(batch) = self.reader.next().transpose()? else {
            return Ok(false);
        };
        if batch.num_rows() == 0
            || batch.num_columns() != 4
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V23 RaBitQ occurrence Arrow batch differs"));
        }
        let mut state = self.state.borrow_mut();
        state.batch = Some(batch);
        state.row = 0;
        Ok(true)
    }
}

impl<R: Read> Iterator for OccurrenceReader<R> {
    type Item = Result<V23RaBitQSourceRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let needs_batch = {
            let state = self.state.borrow();
            state
                .batch
                .as_ref()
                .is_none_or(|batch| state.row == batch.num_rows())
        };
        if needs_batch {
            match self.next_batch() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(error) => return Some(Err(error)),
            }
        }
        let mut state = self.state.borrow_mut();
        let row = state.row;
        let extracted = {
            let batch = state.batch.as_ref().unwrap();
            let ids = batch.column(0).as_any().downcast_ref::<BinaryArray>();
            let vectors = batch
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeListArray>();
            let pages = batch.column(2).as_any().downcast_ref::<UInt32Array>();
            let primary = batch.column(3).as_any().downcast_ref::<BooleanArray>();
            let (Some(ids), Some(vectors), Some(pages), Some(primary)) =
                (ids, vectors, pages, primary)
            else {
                return Some(Err(invalid("V23 RaBitQ occurrence Arrow columns differ")));
            };
            let values = vectors.values();
            let Some(values) = values.as_any().downcast_ref::<Float32Array>() else {
                return Some(Err(invalid("V23 RaBitQ occurrence vector child differs")));
            };
            let start = row * 96;
            (
                ids.value(row).to_vec(),
                values.values()[start..start + 96].try_into().unwrap(),
                pages.value(row),
                primary.value(row),
            )
        };
        let (canonical_record_id, vector, page, is_primary) = extracted;
        let Some(seen) = state.page_seen.get_mut(page as usize) else {
            return Some(Err(invalid("V23 RaBitQ occurrence page ordinal differs")));
        };
        if !*seen {
            *seen = true;
            state.pages_seen += 1;
        }
        state.row += 1;
        Some(Ok(V23RaBitQSourceRow {
            canonical_record_id,
            vector,
            page_ordinal: page,
            is_primary,
        }))
    }
}

fn decode_seed(value: &str) -> Result<[u8; 32]> {
    let mut seed = [0; 32];
    if value.len() != 64 {
        return Err(invalid("V23 RaBitQ rotation seed differs"));
    }
    for (output, pair) in seed.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *output = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16)
            .map_err(|_| invalid("V23 RaBitQ rotation seed differs"))?;
    }
    Ok(seed)
}

#[doc(hidden)]
pub fn run_v23_rabitq_construction_local_request<R: Read>(
    request: V23RaBitQConstructionLocalRunRequest,
    source: R,
) -> Result<Vec<u8>> {
    if !request.execute_construction
        || request.registered_inputs.len() != 3
        || !request.scratch_directory.is_absolute()
        || !request.output_directory.is_absolute()
    {
        return Err(invalid(
            "V23 RaBitQ construction execution was not authorized",
        ));
    }
    let (manifest, _) = read_manifest(&request.paths.manifest, &request.manifest_identity)?;
    if manifest.run_mode != V23RaBitQRunMode::Execute(V23RaBitQPhase::Construction) {
        return Err(invalid("V23 RaBitQ construction manifest phase differs"));
    }
    let roles = ["tree-receipt", "incidence-tree", "page-roster"];
    let mut identities = Vec::with_capacity(roles.len());
    let paths = [
        &request.paths.tree_receipt,
        &request.paths.incidence_tree,
        &request.paths.page_roster,
    ];
    let mut bytes = Vec::with_capacity(roles.len());
    for ((identity, role), path) in request.registered_inputs.iter().zip(roles).zip(paths) {
        bytes.push(read_authenticated(path, identity, role)?);
        identities.push(identity.internal());
    }
    if manifest.registered_inputs != identities {
        return Err(invalid("V23 RaBitQ construction input binding differs"));
    }
    let tree = decode_incidence_tree(&bytes[1])?;
    if tree.shape != V23IncidenceTrainingShape::PRODUCTION {
        return Err(invalid("V23 RaBitQ construction tree shape differs"));
    }
    let (occurrences, state) = OccurrenceReader::new(source, manifest.expected_pages)?;
    let built = build_v23_rabitq_artifacts(V23RaBitQBuildRequest {
        tree: &tree,
        source_rows: occurrences,
        expected_source_occurrences: manifest.expected_source_occurrences,
        expected_unique_rows: manifest.expected_unique_rows,
        rotation_seed: decode_seed(&manifest.rotation_seed_hex)?,
        scratch_directory: &request.scratch_directory,
        output_directory: &request.output_directory,
        output_uri_prefix: &manifest.output_uri_prefix,
        maximum_sort_run_bytes: 256 * 1024 * 1024,
    })?;
    if state.borrow().pages_seen != manifest.expected_pages as usize
        || built.outputs[..5]
            .iter()
            .map(|identity| identity.role.as_str())
            .ne(manifest.output_roles.iter().map(String::as_str))
    {
        return Err(invalid("V23 RaBitQ construction output authority differs"));
    }
    let receipt = V23RaBitQReceipt::complete(
        &manifest,
        &canonical_v23_rabitq_manifest_bytes(&manifest)?,
        built.outputs[..5].to_vec(),
    );
    let receipt_bytes = canonical_v23_rabitq_receipt_bytes(&receipt)?;
    fs::write(
        request.output_directory.join("construction-receipt.json"),
        &receipt_bytes,
    )
    .map_err(|error| {
        invalid(&format!(
            "V23 RaBitQ construction receipt write failed: {error}"
        ))
    })?;
    Ok(receipt_bytes)
}

#[cfg(test)]
mod tests {
    use super::validate_v23_rabitq_d2_page_authority;
    use crate::{
        v23_incidence_eval::V23IncidenceD2Authority, v23_rabitq_arrow::V23RaBitQRowPlanes,
    };

    fn rows() -> V23RaBitQRowPlanes {
        V23RaBitQRowPlanes {
            sign_codes: vec![[0; 12]; 2],
            residual_norms: vec![1.0; 2],
            alignments: vec![0.5; 2],
            primary_pages: vec![0, 28_281],
            replica_pages: vec![1, u32::MAX],
        }
    }

    fn authority() -> V23IncidenceD2Authority {
        V23IncidenceD2Authority {
            index_id: "index-bcda7bb66812e162d45077e6".to_string(),
            page_uri: "s3://borsuk-v23/pages".to_string(),
            page_count: 28_282,
            truth: Vec::new(),
        }
    }

    #[test]
    fn v23_rabitq_local_binds_d2_index_page_namespace_count_and_all_row_ordinals() {
        let expected = authority();
        let page_namespace_uri_prefix = format!("{}/", expected.page_uri);
        assert!(
            validate_v23_rabitq_d2_page_authority(
                &expected.index_id,
                &expected.index_id,
                28_282,
                Some(&page_namespace_uri_prefix),
                &expected,
                &rows(),
            )
            .is_ok()
        );

        let mut changed = authority();
        changed.index_id.push_str("-other");
        assert!(
            validate_v23_rabitq_d2_page_authority(
                &expected.index_id,
                &expected.index_id,
                28_282,
                Some(&page_namespace_uri_prefix),
                &changed,
                &rows(),
            )
            .is_err()
        );
        assert!(
            validate_v23_rabitq_d2_page_authority(
                &expected.index_id,
                "index-other",
                28_282,
                Some(&page_namespace_uri_prefix),
                &expected,
                &rows(),
            )
            .is_err()
        );
        assert!(
            validate_v23_rabitq_d2_page_authority(
                &expected.index_id,
                &expected.index_id,
                28_281,
                Some(&page_namespace_uri_prefix),
                &expected,
                &rows(),
            )
            .is_err()
        );
        let other_prefix = "s3://borsuk-v23/other-pages/".to_string();
        assert!(
            validate_v23_rabitq_d2_page_authority(
                &expected.index_id,
                &expected.index_id,
                28_282,
                Some(&other_prefix),
                &expected,
                &rows(),
            )
            .is_err()
        );
        let mut changed_rows = rows();
        changed_rows.primary_pages[0] = 28_282;
        assert!(
            validate_v23_rabitq_d2_page_authority(
                &expected.index_id,
                &expected.index_id,
                28_282,
                Some(&page_namespace_uri_prefix),
                &expected,
                &changed_rows,
            )
            .is_err()
        );
        changed_rows = rows();
        changed_rows.replica_pages[0] = 28_282;
        assert!(
            validate_v23_rabitq_d2_page_authority(
                &expected.index_id,
                &expected.index_id,
                28_282,
                Some(&page_namespace_uri_prefix),
                &expected,
                &changed_rows,
            )
            .is_err()
        );
    }
}
