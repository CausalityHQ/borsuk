//! Query-blind hyperplane-boundary spill qualification for V38.

use std::{collections::BTreeSet, sync::Arc};

use arrow_array::{Array, Float32Array, RecordBatch, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use rayon::{ThreadPoolBuilder, prelude::*};
use serde::{Deserialize, Serialize};

use crate::{BorsukError, Result};

const V38_CORPUS_ROWS: u64 = 1_000_000;
const V38_POSTING_COUNT: u32 = 123;
const V38_MAXIMUM_ALTERNATE_ASSIGNMENTS: u64 = 250_000;
const V38_MAXIMUM_OWNERS_PER_ROW: u8 = 2;
const V38_MAXIMUM_ROWS_PER_POSTING: u32 = 10_240;
const V38_PROJECTED_RECORD_BYTES: u32 = 48;
const V38_PROJECTED_FRAMING_ALLOWANCE_BYTES: u32 = 32_768;
const V38_SELECTED_POSTINGS: u32 = 14;
const V38_AGGREGATE_GATE_PPM: u32 = 998_000;
const V38_MINIMUM_GATE_PPM: u32 = 800_000;
const V38_QUERY_COUNT: u32 = 1_000;
const V38_GT_NEIGHBORS: u32 = 100;
const V38_MAXIMUM_QUERY_SOLVER_NODES: u64 = 250_000;
const V38_MAXIMUM_SOLVER_NODES: u64 = 25_000_000;
const V38_MAXIMUM_CERTIFICATE_BYTES: u32 = 256;
const V38_MAXIMUM_WORKERS: u32 = 32;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38SpillSpec {
    corpus_rows: u64,
    posting_count: u32,
    maximum_alternate_assignments: u64,
    maximum_owners_per_row: u8,
    maximum_rows_per_posting: u32,
    projected_record_bytes: u32,
    projected_framing_allowance_bytes: u32,
    selected_postings: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V38SpillCapacity {
    total_posting_capacity: u64,
    maximum_total_assignments: u64,
    maximum_posting_payload_bytes: u64,
    maximum_complete_object_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38ArtifactIdentity {
    blake3: String,
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38OutputTarget {
    role: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38ConstructionAuthority {
    algorithm: String,
    inputs: Vec<V38ArtifactIdentity>,
    metric: String,
    numeric_backend: String,
    outputs: Vec<V38OutputTarget>,
    schema: String,
    source_archive: V38ArtifactIdentity,
    source_commit: String,
    spec: V38SpillSpec,
    workers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38CeilingAuthority {
    aggregate_gate_ppm: u32,
    construction_result: V38ArtifactIdentity,
    development_ground_truth: V38ArtifactIdentity,
    gt_neighbors: u32,
    maximum_certificate_bytes: u32,
    maximum_query_solver_nodes: u64,
    maximum_solver_nodes: u64,
    minimum_gate_ppm: u32,
    posting_summary: V38ArtifactIdentity,
    query_count: u32,
    relation: V38ArtifactIdentity,
    schema: String,
    selected_postings: u32,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn valid_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_s3_object_uri(value: &str) -> bool {
    value
        .strip_prefix("s3://")
        .and_then(|object| object.split_once('/'))
        .is_some_and(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
}

fn validate_artifact(identity: &V38ArtifactIdentity, role: &str) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes == 0
        || !valid_lower_hex_digest(&identity.sha256)
        || !valid_lower_hex_digest(&identity.blake3)
        || !valid_s3_object_uri(&identity.uri)
    {
        return Err(invalid("V38 artifact authority differs"));
    }
    Ok(())
}

pub(crate) fn project_v38_spill_capacity(spec: &V38SpillSpec) -> Result<V38SpillCapacity> {
    let total_posting_capacity = u64::from(spec.posting_count)
        .checked_mul(u64::from(spec.maximum_rows_per_posting))
        .ok_or_else(|| invalid("V38 posting capacity overflows"))?;
    let maximum_total_assignments = spec
        .corpus_rows
        .checked_add(spec.maximum_alternate_assignments)
        .ok_or_else(|| invalid("V38 assignment capacity overflows"))?;
    let maximum_posting_payload_bytes = u64::from(spec.maximum_rows_per_posting)
        .checked_mul(u64::from(spec.projected_record_bytes))
        .ok_or_else(|| invalid("V38 posting payload overflows"))?;
    let maximum_complete_object_bytes = maximum_posting_payload_bytes
        .checked_add(u64::from(spec.projected_framing_allowance_bytes))
        .ok_or_else(|| invalid("V38 complete object projection overflows"))?;
    Ok(V38SpillCapacity {
        total_posting_capacity,
        maximum_total_assignments,
        maximum_posting_payload_bytes,
        maximum_complete_object_bytes,
    })
}

pub(crate) fn validate_v38_spill_spec(spec: &V38SpillSpec) -> Result<()> {
    if spec.corpus_rows != V38_CORPUS_ROWS
        || spec.posting_count != V38_POSTING_COUNT
        || spec.maximum_alternate_assignments != V38_MAXIMUM_ALTERNATE_ASSIGNMENTS
        || spec.maximum_owners_per_row != V38_MAXIMUM_OWNERS_PER_ROW
        || spec.maximum_rows_per_posting != V38_MAXIMUM_ROWS_PER_POSTING
        || spec.projected_record_bytes != V38_PROJECTED_RECORD_BYTES
        || spec.projected_framing_allowance_bytes != V38_PROJECTED_FRAMING_ALLOWANCE_BYTES
        || spec.selected_postings != V38_SELECTED_POSTINGS
    {
        return Err(invalid("V38 spill specification differs"));
    }
    let projection = project_v38_spill_capacity(spec)?;
    if projection.total_posting_capacity < projection.maximum_total_assignments
        || projection.maximum_posting_payload_bytes != 491_520
        || projection.maximum_complete_object_bytes != 524_288
    {
        return Err(invalid("V38 spill capacity differs"));
    }
    Ok(())
}

fn validate_construction_authority(authority: &V38ConstructionAuthority) -> Result<()> {
    if authority.schema != "borsuk-v38-boundary-spill-authority-v1"
        || authority.algorithm != "query-blind-hyperplane-boundary-spill-v1"
        || authority.metric != "squared-l2"
        || !matches!(
            authority.numeric_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || authority.workers == 0
        || authority.workers > V38_MAXIMUM_WORKERS
        || !valid_lower_hex_digest(&authority.source_commit)
    {
        return Err(invalid("V38 construction authority differs"));
    }
    validate_v38_spill_spec(&authority.spec)?;
    validate_artifact(&authority.source_archive, "source-archive")?;
    let roles = [
        "v37-authority",
        "v37-construction-result",
        "v37-tree",
        "v37-ownership",
        "source",
    ];
    if authority.inputs.len() != roles.len() {
        return Err(invalid("V38 construction input authority differs"));
    }
    for (identity, role) in authority.inputs.iter().zip(roles) {
        validate_artifact(identity, role)?;
    }
    let output_roles = ["spill-relation", "spill-postings"];
    if authority.outputs.len() != output_roles.len() {
        return Err(invalid("V38 construction output authority differs"));
    }
    for (output, role) in authority.outputs.iter().zip(output_roles) {
        if output.role != role || !valid_s3_object_uri(&output.uri) {
            return Err(invalid("V38 construction output authority differs"));
        }
    }
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for (role, uri) in authority
        .inputs
        .iter()
        .map(|item| (item.role.as_str(), item.uri.as_str()))
        .chain(std::iter::once((
            authority.source_archive.role.as_str(),
            authority.source_archive.uri.as_str(),
        )))
        .chain(
            authority
                .outputs
                .iter()
                .map(|item| (item.role.as_str(), item.uri.as_str())),
        )
    {
        if !roles.insert(role) || !uris.insert(uri) {
            return Err(invalid("V38 construction roles overlap"));
        }
    }
    Ok(())
}

fn validate_ceiling_authority(authority: &V38CeilingAuthority) -> Result<()> {
    if authority.schema != "borsuk-v38-boundary-spill-ceiling-authority-v1"
        || authority.aggregate_gate_ppm != V38_AGGREGATE_GATE_PPM
        || authority.minimum_gate_ppm != V38_MINIMUM_GATE_PPM
        || authority.query_count != V38_QUERY_COUNT
        || authority.gt_neighbors != V38_GT_NEIGHBORS
        || authority.selected_postings != V38_SELECTED_POSTINGS
        || authority.maximum_query_solver_nodes != V38_MAXIMUM_QUERY_SOLVER_NODES
        || authority.maximum_solver_nodes != V38_MAXIMUM_SOLVER_NODES
        || authority.maximum_certificate_bytes != V38_MAXIMUM_CERTIFICATE_BYTES
    {
        return Err(invalid("V38 ceiling authority differs"));
    }
    for (identity, role) in [
        (&authority.construction_result, "v38-construction-result"),
        (&authority.development_ground_truth, "gt100"),
        (&authority.posting_summary, "spill-postings"),
        (&authority.relation, "spill-relation"),
    ] {
        validate_artifact(identity, role)?;
    }
    let uris = [
        authority.construction_result.uri.as_str(),
        authority.development_ground_truth.uri.as_str(),
        authority.posting_summary.uri.as_str(),
        authority.relation.uri.as_str(),
    ];
    if uris.into_iter().collect::<BTreeSet<_>>().len() != uris.len() {
        return Err(invalid("V38 ceiling artifact roles overlap"));
    }
    Ok(())
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries: Vec<_> = object.into_iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        value => value,
    }
}

fn canonical_bytes<T: Serialize>(value: &T, context: &str) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|error| invalid(&format!("V38 {context} serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V38 {context} serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_v38_construction_authority_bytes(
    authority: &V38ConstructionAuthority,
) -> Result<Vec<u8>> {
    validate_construction_authority(authority)?;
    canonical_bytes(authority, "construction authority")
}

pub(crate) fn canonical_v38_ceiling_authority_bytes(
    authority: &V38CeilingAuthority,
) -> Result<Vec<u8>> {
    validate_ceiling_authority(authority)?;
    canonical_bytes(authority, "ceiling authority")
}

/// Validate and return the canonical bytes of a V38 construction authority.
#[doc(hidden)]
pub fn validate_v38_construction_authority_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let authority: V38ConstructionAuthority = serde_json::from_slice(bytes).map_err(|error| {
        invalid(&format!(
            "V38 construction authority parsing failed: {error}"
        ))
    })?;
    let canonical = canonical_v38_construction_authority_bytes(&authority)?;
    if canonical != bytes {
        return Err(invalid(
            "V38 construction authority bytes are not canonical",
        ));
    }
    Ok(canonical)
}

/// Validate and return the canonical bytes of a V38 ceiling authority.
#[doc(hidden)]
pub fn validate_v38_ceiling_authority_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let authority: V38CeilingAuthority = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V38 ceiling authority parsing failed: {error}")))?;
    let canonical = canonical_v38_ceiling_authority_bytes(&authority)?;
    if canonical != bytes {
        return Err(invalid("V38 ceiling authority bytes are not canonical"));
    }
    Ok(canonical)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V38SpillProposal {
    alternate_posting: u32,
    alternate_violation_bits: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V38SpillRecord {
    source_ordinal: u64,
    feature_row_id: u64,
    posting_ordinal: u32,
    owner_role: u8,
    posting_local_ordinal: u32,
    alternate_violation_bits: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V38PostingSummary {
    posting_ordinal: u32,
    primary_population: u64,
    alternate_population: u64,
    total_population: u64,
    projected_payload_bytes: u64,
    projected_framing_allowance_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V38SpillRelation {
    records: Vec<V38SpillRecord>,
    postings: Vec<V38PostingSummary>,
}

fn v38_spill_relation_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("owner_role", DataType::UInt8, false),
        Field::new("posting_local_ordinal", DataType::UInt32, false),
        Field::new("alternate_violation", DataType::Float32, true),
    ])
}

fn v38_posting_summary_schema() -> Schema {
    Schema::new(vec![
        Field::new("posting_ordinal", DataType::UInt32, false),
        Field::new("primary_population", DataType::UInt64, false),
        Field::new("alternate_population", DataType::UInt64, false),
        Field::new("total_population", DataType::UInt64, false),
        Field::new("projected_payload_bytes", DataType::UInt64, false),
        Field::new("projected_framing_allowance_bytes", DataType::UInt32, false),
    ])
}

fn parquet_properties(maximum_row_group_rows: usize) -> Result<WriterProperties> {
    if maximum_row_group_rows == 0 {
        return Err(invalid("V38 Parquet row group size differs"));
    }
    Ok(WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(maximum_row_group_rows))
        .build())
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, index: usize, message: &str) -> Result<&'a T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid(message))
}

fn validate_v38_spill_relation_shape(records: &[V38SpillRecord]) -> Result<u32> {
    if records.is_empty() {
        return Err(invalid("V38 spill relation is empty"));
    }
    let mut previous = None;
    let mut current_source = None;
    let mut primary_seen = false;
    let mut primary_feature_id = None;
    let mut primary_posting = None;
    let mut unique_feature_ids = BTreeSet::new();
    let mut maximum_posting = None;
    for record in records {
        let key = (record.source_ordinal, record.owner_role);
        if previous.is_some_and(|value| key <= value)
            || record.owner_role > 1
            || record.owner_role == 0 && record.alternate_violation_bits.is_some()
            || record.owner_role == 1 && record.alternate_violation_bits.is_none()
        {
            return Err(invalid("V38 spill relation ordering or role differs"));
        }
        if current_source != Some(record.source_ordinal) {
            if current_source.is_some() && !primary_seen {
                return Err(invalid("V38 spill primary owner is absent"));
            }
            current_source = Some(record.source_ordinal);
            primary_seen = false;
            primary_feature_id = None;
            primary_posting = None;
        }
        if record.owner_role == 0 {
            if primary_seen || !unique_feature_ids.insert(record.feature_row_id) {
                return Err(invalid("V38 spill primary owner is duplicated"));
            }
            primary_seen = true;
            primary_feature_id = Some(record.feature_row_id);
            primary_posting = Some(record.posting_ordinal);
        } else if !primary_seen {
            return Err(invalid("V38 spill alternate precedes primary"));
        } else if primary_feature_id != Some(record.feature_row_id)
            || primary_posting == Some(record.posting_ordinal)
        {
            return Err(invalid("V38 spill alternate binding differs"));
        }
        if let Some(bits) = record.alternate_violation_bits {
            let value = f32::from_bits(bits);
            if !value.is_finite() || value.total_cmp(&0.0).is_lt() {
                return Err(invalid("V38 spill alternate violation differs"));
            }
        }
        maximum_posting = Some(
            maximum_posting.map_or(record.posting_ordinal, |maximum: u32| {
                maximum.max(record.posting_ordinal)
            }),
        );
        previous = Some(key);
    }
    if !primary_seen {
        return Err(invalid("V38 spill primary owner is absent"));
    }
    let posting_count = maximum_posting
        .and_then(|maximum| maximum.checked_add(1))
        .ok_or_else(|| invalid("V38 spill posting count overflows"))?;
    if usize::try_from(posting_count)
        .ok()
        .is_none_or(|count| count > records.len())
    {
        return Err(invalid("V38 spill posting count exceeds relation rows"));
    }
    let mut next_primary = vec![0_u32; posting_count as usize];
    for record in records.iter().filter(|record| record.owner_role == 0) {
        let posting = record.posting_ordinal as usize;
        if record.posting_local_ordinal != next_primary[posting] {
            return Err(invalid("V38 spill primary local order differs"));
        }
        next_primary[posting] = next_primary[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill primary population overflows"))?;
    }
    let mut next_alternate = next_primary;
    for record in records.iter().filter(|record| record.owner_role == 1) {
        let posting = record.posting_ordinal as usize;
        if record.posting_local_ordinal != next_alternate[posting] {
            return Err(invalid("V38 spill alternate local order differs"));
        }
        next_alternate[posting] = next_alternate[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill total population overflows"))?;
    }
    Ok(posting_count)
}

fn validate_v38_spill_relation_against_primary(
    records: &[V38SpillRecord],
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<()> {
    if validate_v38_spill_relation_shape(records)? > posting_count
        || primary.is_empty()
        || maximum_rows_per_posting == 0
    {
        return Err(invalid("V38 spill relation authority differs"));
    }
    let observed_primary: Vec<_> = records
        .iter()
        .filter(|record| record.owner_role == 0)
        .map(|record| {
            (
                record.source_ordinal,
                record.feature_row_id,
                record.posting_ordinal,
                record.posting_local_ordinal,
            )
        })
        .collect();
    let expected_primary: Vec<_> = primary
        .iter()
        .map(|record| {
            (
                record.source_ordinal,
                record.feature_row_id,
                record.posting_ordinal,
                record.posting_local_ordinal,
            )
        })
        .collect();
    if observed_primary != expected_primary {
        return Err(invalid("V38 spill primary ownership differs"));
    }
    let mut next_alternate = vec![0_u32; posting_count as usize];
    for record in primary {
        let posting = record.posting_ordinal as usize;
        if posting >= next_alternate.len() {
            return Err(invalid("V38 spill primary posting differs"));
        }
        next_alternate[posting] = next_alternate[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill primary population overflows"))?;
    }
    for record in records {
        let posting = record.posting_ordinal as usize;
        if posting >= next_alternate.len() {
            return Err(invalid("V38 spill posting local ordinal differs"));
        }
        if record.owner_role == 1 {
            if record.posting_local_ordinal != next_alternate[posting] {
                return Err(invalid("V38 spill alternate local order differs"));
            }
            next_alternate[posting] = next_alternate[posting]
                .checked_add(1)
                .ok_or_else(|| invalid("V38 spill total population overflows"))?;
        }
    }
    if next_alternate
        .iter()
        .any(|population| *population == 0 || *population > maximum_rows_per_posting)
    {
        return Err(invalid("V38 spill posting local range differs"));
    }
    Ok(())
}

pub(crate) fn encode_v38_spill_relation_parquet(records: &[V38SpillRecord]) -> Result<Vec<u8>> {
    encode_v38_spill_relation_parquet_with_row_group_size(records, 1_048_576)
}

pub(crate) fn encode_v38_spill_relation_parquet_with_row_group_size(
    records: &[V38SpillRecord],
    maximum_row_group_rows: usize,
) -> Result<Vec<u8>> {
    validate_v38_spill_relation_shape(records)?;
    let schema = Arc::new(v38_spill_relation_schema());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                records.iter().map(|record| record.source_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                records.iter().map(|record| record.feature_row_id),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.posting_ordinal),
            )),
            Arc::new(UInt8Array::from_iter_values(
                records.iter().map(|record| record.owner_role),
            )),
            Arc::new(UInt32Array::from_iter_values(
                records.iter().map(|record| record.posting_local_ordinal),
            )),
            Arc::new(Float32Array::from(
                records
                    .iter()
                    .map(|record| record.alternate_violation_bits.map(f32::from_bits))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut bytes,
        schema,
        Some(parquet_properties(maximum_row_group_rows)?),
    )?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn decode_v38_spill_relation_parquet(
    bytes: &[u8],
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38SpillRecord>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let maximum_rows = primary
        .len()
        .checked_mul(2)
        .ok_or_else(|| invalid("V38 spill relation row bound overflows"))?;
    let observed_rows = usize::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid("V38 spill relation row count differs"))?;
    if builder.schema().as_ref() != &v38_spill_relation_schema()
        || builder.metadata().num_row_groups() == 0
        || observed_rows < primary.len()
        || observed_rows > maximum_rows
    {
        return Err(invalid("V38 spill relation Parquet schema differs"));
    }
    let mut records = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 6
            || batch.columns()[..5]
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V38 spill relation Parquet batch differs"));
        }
        let source = column::<UInt64Array>(&batch, 0, "V38 source ordinal differs")?;
        let feature = column::<UInt64Array>(&batch, 1, "V38 feature row differs")?;
        let posting = column::<UInt32Array>(&batch, 2, "V38 posting ordinal differs")?;
        let role = column::<UInt8Array>(&batch, 3, "V38 owner role differs")?;
        let local = column::<UInt32Array>(&batch, 4, "V38 posting local ordinal differs")?;
        let violation = column::<Float32Array>(&batch, 5, "V38 alternate violation differs")?;
        for row in 0..batch.num_rows() {
            records.push(V38SpillRecord {
                source_ordinal: source.value(row),
                feature_row_id: feature.value(row),
                posting_ordinal: posting.value(row),
                owner_role: role.value(row),
                posting_local_ordinal: local.value(row),
                alternate_violation_bits: (!violation.is_null(row))
                    .then(|| violation.value(row).to_bits()),
            });
        }
    }
    validate_v38_spill_relation_against_primary(
        &records,
        primary,
        posting_count,
        maximum_rows_per_posting,
    )?;
    Ok(records)
}

pub(crate) fn summarize_v38_spill_relation(
    records: &[V38SpillRecord],
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38PostingSummary>> {
    if validate_v38_spill_relation_shape(records)? > posting_count || posting_count == 0 {
        return Err(invalid("V38 spill posting summary authority differs"));
    }
    let mut populations = vec![(0_u64, 0_u64); posting_count as usize];
    let mut next_primary = vec![0_u32; posting_count as usize];
    for record in records {
        let posting = record.posting_ordinal as usize;
        if posting >= populations.len() {
            return Err(invalid("V38 spill posting summary ordinal differs"));
        }
        if record.owner_role == 0 {
            if record.posting_local_ordinal != next_primary[posting] {
                return Err(invalid("V38 spill primary local order differs"));
            }
            next_primary[posting] = next_primary[posting]
                .checked_add(1)
                .ok_or_else(|| invalid("V38 spill primary population overflows"))?;
            populations[posting].0 += 1;
        } else {
            populations[posting].1 += 1;
        }
    }
    let mut next_alternate = next_primary.clone();
    for record in records.iter().filter(|record| record.owner_role == 1) {
        let posting = record.posting_ordinal as usize;
        if record.posting_local_ordinal != next_alternate[posting] {
            return Err(invalid("V38 spill alternate local order differs"));
        }
        next_alternate[posting] = next_alternate[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill total population overflows"))?;
    }
    let mut summaries = Vec::with_capacity(posting_count as usize);
    for posting in 0..posting_count as usize {
        let (primary_population, alternate_population) = populations[posting];
        let total_population = primary_population
            .checked_add(alternate_population)
            .ok_or_else(|| invalid("V38 spill posting population overflows"))?;
        if primary_population == 0
            || total_population > u64::from(maximum_rows_per_posting)
            || u64::from(next_alternate[posting]) != total_population
        {
            return Err(invalid("V38 spill posting population differs"));
        }
        summaries.push(V38PostingSummary {
            posting_ordinal: posting as u32,
            primary_population,
            alternate_population,
            total_population,
            projected_payload_bytes: total_population
                .checked_mul(u64::from(V38_PROJECTED_RECORD_BYTES))
                .ok_or_else(|| invalid("V38 spill payload projection overflows"))?,
            projected_framing_allowance_bytes: V38_PROJECTED_FRAMING_ALLOWANCE_BYTES,
        });
    }
    Ok(summaries)
}

fn validate_v38_posting_summaries(summaries: &[V38PostingSummary]) -> Result<()> {
    if summaries.is_empty() {
        return Err(invalid("V38 spill posting summary is empty"));
    }
    for (index, summary) in summaries.iter().enumerate() {
        if summary.posting_ordinal != index as u32
            || summary.primary_population == 0
            || summary.total_population
                != summary
                    .primary_population
                    .checked_add(summary.alternate_population)
                    .ok_or_else(|| invalid("V38 spill posting population overflows"))?
            || summary.projected_payload_bytes
                != summary
                    .total_population
                    .checked_mul(u64::from(V38_PROJECTED_RECORD_BYTES))
                    .ok_or_else(|| invalid("V38 spill payload projection overflows"))?
            || summary.projected_framing_allowance_bytes != V38_PROJECTED_FRAMING_ALLOWANCE_BYTES
            || summary.projected_payload_bytes > 491_520
        {
            return Err(invalid("V38 spill posting summary differs"));
        }
    }
    Ok(())
}

pub(crate) fn encode_v38_posting_summary_parquet(
    summaries: &[V38PostingSummary],
) -> Result<Vec<u8>> {
    validate_v38_posting_summaries(summaries)?;
    let schema = Arc::new(v38_posting_summary_schema());
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                summaries.iter().map(|value| value.posting_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries.iter().map(|value| value.primary_population),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries.iter().map(|value| value.alternate_population),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries.iter().map(|value| value.total_population),
            )),
            Arc::new(UInt64Array::from_iter_values(
                summaries.iter().map(|value| value.projected_payload_bytes),
            )),
            Arc::new(UInt32Array::from_iter_values(
                summaries
                    .iter()
                    .map(|value| value.projected_framing_allowance_bytes),
            )),
        ],
    )?;
    let mut bytes = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut bytes, schema, Some(parquet_properties(1_048_576)?))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn decode_v38_posting_summary_parquet(
    bytes: &[u8],
    relation: &[V38SpillRecord],
    posting_count: u32,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38PostingSummary>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let observed_rows = usize::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid("V38 spill posting row count differs"))?;
    if builder.schema().as_ref() != &v38_posting_summary_schema()
        || builder.metadata().num_row_groups() == 0
        || observed_rows != posting_count as usize
    {
        return Err(invalid("V38 spill posting Parquet schema differs"));
    }
    let mut summaries = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 6
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V38 spill posting Parquet batch differs"));
        }
        let posting = column::<UInt32Array>(&batch, 0, "V38 posting ordinal differs")?;
        let primary = column::<UInt64Array>(&batch, 1, "V38 primary population differs")?;
        let alternate = column::<UInt64Array>(&batch, 2, "V38 alternate population differs")?;
        let total = column::<UInt64Array>(&batch, 3, "V38 total population differs")?;
        let payload = column::<UInt64Array>(&batch, 4, "V38 projected payload differs")?;
        let framing = column::<UInt32Array>(&batch, 5, "V38 framing allowance differs")?;
        for row in 0..batch.num_rows() {
            summaries.push(V38PostingSummary {
                posting_ordinal: posting.value(row),
                primary_population: primary.value(row),
                alternate_population: alternate.value(row),
                total_population: total.value(row),
                projected_payload_bytes: payload.value(row),
                projected_framing_allowance_bytes: framing.value(row),
            });
        }
    }
    validate_v38_posting_summaries(&summaries)?;
    if summaries != summarize_v38_spill_relation(relation, posting_count, maximum_rows_per_posting)?
    {
        return Err(invalid(
            "V38 spill posting summary does not reproduce relation",
        ));
    }
    Ok(summaries)
}

pub(crate) fn build_v38_spill_relation(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_rows: &[Vec<f32>],
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    maximum_alternates: u64,
    maximum_rows_per_posting: u32,
    workers: u32,
) -> Result<V38SpillRelation> {
    const RESIDENT_PROPOSAL_LIMIT_BYTES: u64 = 64 * 1_048_576;

    crate::v37_relation_router::validate_v37_tree_geometry(tree)?;
    let posting_count = u32::try_from(tree.leaf_populations.len())
        .map_err(|_| invalid("V38 spill posting count exceeds authority width"))?;
    let row_count = u64::try_from(projected_rows.len())
        .map_err(|_| invalid("V38 spill row count exceeds authority width"))?;
    let auxiliary_bytes =
        project_v38_admission_auxiliary_bytes(row_count, maximum_alternates, posting_count)?;
    if projected_rows.is_empty()
        || projected_rows.len() != primary.len()
        || auxiliary_bytes > RESIDENT_PROPOSAL_LIMIT_BYTES
        || workers == 0
        || workers > V38_MAXIMUM_WORKERS
    {
        return Err(invalid("V38 spill construction authority differs"));
    }
    let pool = ThreadPoolBuilder::new()
        .num_threads(workers as usize)
        .build()
        .map_err(|_| invalid("V38 spill worker pool differs"))?;
    let proposals = pool.install(|| {
        projected_rows
            .par_iter()
            .enumerate()
            .map(|(index, row)| {
                let record = &primary[index];
                let source_ordinal = u64::try_from(index)
                    .map_err(|_| invalid("V38 spill source ordinal overflows"))?;
                if record.source_ordinal != source_ordinal
                    || row.len() != tree.dimensions
                    || row.iter().any(|value| !value.is_finite())
                {
                    return Err(invalid("V38 spill primary replay differs"));
                }
                let scores = score_v38_internal_nodes(tree, row)?;
                if replay_v38_primary_from_scores(tree, &scores, source_ordinal)?
                    != record.posting_ordinal
                {
                    return Err(invalid("V38 spill primary replay differs"));
                }
                propose_v38_alternate_owner_from_scores(tree, &scores, record.posting_ordinal)
                    .map(|proposal| (source_ordinal, proposal))
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let records = admit_v38_spill_proposals(
        primary,
        &proposals,
        posting_count,
        maximum_alternates,
        maximum_rows_per_posting,
    )?;
    let postings = summarize_v38_spill_relation(&records, posting_count, maximum_rows_per_posting)?;
    Ok(V38SpillRelation { records, postings })
}

fn squared_positive_margin(value: f32) -> Result<f32> {
    if !value.is_finite() {
        return Err(invalid("V38 spill margin is non-finite"));
    }
    let positive = if value.total_cmp(&0.0).is_gt() {
        value
    } else {
        0.0
    };
    let squared = positive * positive;
    if !squared.is_finite() {
        return Err(invalid("V38 spill squared margin is non-finite"));
    }
    Ok(if squared == 0.0 { 0.0 } else { squared })
}

fn larger_penalty(left: f32, right: f32) -> f32 {
    if left.total_cmp(&right).is_gt() {
        left
    } else {
        right
    }
}

pub(crate) fn propose_v38_alternate_owner(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_row: &[f32],
    primary_posting: u32,
) -> Result<V38SpillProposal> {
    crate::v37_relation_router::validate_v37_tree_geometry(tree)?;
    if projected_row.len() != tree.dimensions
        || projected_row.iter().any(|value| !value.is_finite())
        || primary_posting as usize >= tree.leaf_populations.len()
    {
        return Err(invalid("V38 spill row authority differs"));
    }
    let scores = score_v38_internal_nodes(tree, projected_row)?;
    propose_v38_alternate_owner_from_scores(tree, &scores, primary_posting)
}

fn score_v38_internal_nodes(
    tree: &crate::v37_relation_router::V37BalancedTree,
    projected_row: &[f32],
) -> Result<Vec<Option<f32>>> {
    let mut scores = vec![None; tree.nodes.len()];
    for (index, node) in tree.nodes.iter().enumerate() {
        if node.posting_ordinal.is_none() {
            let (score, backend) = crate::v37_relation_router::score_v37_hyperplane_fused(
                projected_row,
                &node.normal,
            )?;
            if backend != tree.fma_backend {
                return Err(invalid("V38 spill numeric backend differs"));
            }
            scores[index] = Some(score);
        }
    }
    Ok(scores)
}

fn replay_v38_primary_from_scores(
    tree: &crate::v37_relation_router::V37BalancedTree,
    scores: &[Option<f32>],
    source_ordinal: u64,
) -> Result<u32> {
    let mut node_index = 0_usize;
    loop {
        let node = tree
            .nodes
            .get(node_index)
            .ok_or_else(|| invalid("V38 spill primary replay node differs"))?;
        if let Some(posting) = node.posting_ordinal {
            return Ok(posting);
        }
        let score = scores
            .get(node_index)
            .and_then(|score| *score)
            .ok_or_else(|| invalid("V38 spill primary replay score is absent"))?;
        let is_left = crate::v37_relation_router::route_v37_corpus_member(
            score,
            source_ordinal,
            f32::from_bits(node.boundary_score_bits),
            node.boundary_source_ordinal,
        )?;
        node_index = if is_left {
            node.left_node
        } else {
            node.right_node
        }
        .ok_or_else(|| invalid("V38 spill primary replay child is absent"))?
            as usize;
    }
}

fn propose_v38_alternate_owner_from_scores(
    tree: &crate::v37_relation_router::V37BalancedTree,
    scores: &[Option<f32>],
    primary_posting: u32,
) -> Result<V38SpillProposal> {
    fn visit(
        tree: &crate::v37_relation_router::V37BalancedTree,
        scores: &[Option<f32>],
        node_index: usize,
        path_penalty: f32,
        primary_posting: u32,
        best: &mut Option<V38SpillProposal>,
    ) -> Result<()> {
        let node = tree
            .nodes
            .get(node_index)
            .ok_or_else(|| invalid("V38 spill tree node differs"))?;
        if let Some(posting) = node.posting_ordinal {
            if posting != primary_posting {
                let proposal = V38SpillProposal {
                    alternate_posting: posting,
                    alternate_violation_bits: path_penalty.to_bits(),
                };
                let replace = best.as_ref().is_none_or(|current| {
                    path_penalty
                        .total_cmp(&f32::from_bits(current.alternate_violation_bits))
                        .then_with(|| posting.cmp(&current.alternate_posting))
                        .is_lt()
                });
                if replace {
                    *best = Some(proposal);
                }
            }
            return Ok(());
        }
        let score = scores
            .get(node_index)
            .and_then(|score| *score)
            .ok_or_else(|| invalid("V38 spill node score is absent"))?;
        let boundary = f32::from_bits(node.boundary_score_bits);
        let left_penalty = larger_penalty(path_penalty, squared_positive_margin(score - boundary)?);
        let right_penalty =
            larger_penalty(path_penalty, squared_positive_margin(boundary - score)?);
        visit(
            tree,
            scores,
            node.left_node
                .ok_or_else(|| invalid("V38 spill left child is absent"))? as usize,
            left_penalty,
            primary_posting,
            best,
        )?;
        visit(
            tree,
            scores,
            node.right_node
                .ok_or_else(|| invalid("V38 spill right child is absent"))? as usize,
            right_penalty,
            primary_posting,
            best,
        )
    }

    let mut best = None;
    visit(tree, scores, 0, 0.0, primary_posting, &mut best)?;
    best.ok_or_else(|| invalid("V38 spill alternate owner is absent"))
}

#[derive(Debug, Clone, Copy)]
struct RankedProposal {
    source_ordinal: u64,
    primary_posting: u32,
    primary_population: u32,
    local_rank: u32,
    proposal: V38SpillProposal,
}

pub(crate) fn project_v38_admission_auxiliary_bytes(
    row_count: u64,
    maximum_alternates: u64,
    posting_count: u32,
) -> Result<u64> {
    use std::mem::size_of;

    let proposal_input = row_count
        .checked_mul(size_of::<(u64, V38SpillProposal)>() as u64)
        .ok_or_else(|| invalid("V38 proposal input projection overflows"))?;
    let feature_scratch = row_count
        .checked_mul(size_of::<u64>() as u64)
        .and_then(|bytes| bytes.checked_add(proposal_input))
        .ok_or_else(|| invalid("V38 feature scratch projection overflows"))?;
    let ranked = row_count
        .checked_mul(size_of::<RankedProposal>() as u64)
        .and_then(|bytes| bytes.checked_add(proposal_input))
        .and_then(|bytes| {
            maximum_alternates
                .checked_mul(size_of::<(u64, V38SpillProposal)>() as u64)
                .and_then(|accepted| bytes.checked_add(accepted))
        })
        .ok_or_else(|| invalid("V38 ranked proposal projection overflows"))?;
    let posting_scratch = u64::from(posting_count)
        .checked_mul((2 * size_of::<u32>() + size_of::<Vec<(u64, V38SpillProposal)>>()) as u64)
        .ok_or_else(|| invalid("V38 posting scratch projection overflows"))?;
    feature_scratch
        .max(ranked)
        .checked_add(posting_scratch)
        .ok_or_else(|| invalid("V38 admission projection overflows"))
}

pub(crate) fn admit_v38_spill_proposals(
    primary: &[crate::v37_relation_router::V37OwnershipRecord],
    proposals: &[(u64, V38SpillProposal)],
    posting_count: u32,
    maximum_alternates: u64,
    maximum_rows_per_posting: u32,
) -> Result<Vec<V38SpillRecord>> {
    if primary.is_empty()
        || primary.len() != proposals.len()
        || posting_count < 2
        || maximum_alternates == 0
        || maximum_rows_per_posting == 0
    {
        return Err(invalid("V38 spill admission authority differs"));
    }
    let posting_count_usize = posting_count as usize;
    let mut populations = vec![0_u32; posting_count_usize];
    let mut feature_ids = Vec::with_capacity(primary.len());
    for (index, record) in primary.iter().enumerate() {
        let posting = record.posting_ordinal as usize;
        if record.source_ordinal != index as u64
            || posting >= posting_count_usize
            || record.posting_local_ordinal != populations[posting]
        {
            return Err(invalid("V38 primary ownership differs"));
        }
        feature_ids.push(record.feature_row_id);
        populations[posting] = populations[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 primary population overflows"))?;
    }
    feature_ids.sort_unstable();
    if feature_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("V38 primary feature IDs differ"));
    }
    drop(feature_ids);
    if populations
        .iter()
        .any(|population| *population == 0 || *population > maximum_rows_per_posting)
    {
        return Err(invalid("V38 primary local ordinals differ"));
    }
    let primary_populations = populations.clone();

    let mut ranked = Vec::with_capacity(proposals.len());
    for &(source_ordinal, proposal) in proposals {
        let index = primary
            .binary_search_by_key(&source_ordinal, |record| record.source_ordinal)
            .map_err(|_| invalid("V38 spill proposal source differs"))?;
        let primary_posting = primary[index].posting_ordinal;
        let violation = f32::from_bits(proposal.alternate_violation_bits);
        if proposal.alternate_posting >= posting_count
            || proposal.alternate_posting == primary_posting
            || !violation.is_finite()
            || violation.total_cmp(&0.0).is_lt()
        {
            return Err(invalid("V38 spill proposal differs"));
        }
        ranked.push(RankedProposal {
            source_ordinal,
            primary_posting,
            primary_population: 0,
            local_rank: 0,
            proposal,
        });
    }
    ranked.sort_unstable_by_key(|proposal| proposal.source_ordinal);
    if ranked
        .iter()
        .zip(primary)
        .any(|(proposal, record)| proposal.source_ordinal != record.source_ordinal)
    {
        return Err(invalid("V38 spill proposal coverage differs"));
    }

    ranked.sort_unstable_by(|left, right| {
        left.primary_posting
            .cmp(&right.primary_posting)
            .then_with(|| {
                f32::from_bits(left.proposal.alternate_violation_bits)
                    .total_cmp(&f32::from_bits(right.proposal.alternate_violation_bits))
            })
            .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
    });
    let mut local_counts = vec![0_u32; posting_count_usize];
    for proposal in &mut ranked {
        let posting = proposal.primary_posting as usize;
        local_counts[posting] = local_counts[posting]
            .checked_add(1)
            .ok_or_else(|| invalid("V38 spill local rank overflows"))?;
        proposal.local_rank = local_counts[posting];
        proposal.primary_population = populations[posting];
    }
    if local_counts != populations {
        return Err(invalid("V38 spill primary proposal population differs"));
    }
    ranked.sort_unstable_by(|left, right| {
        (u128::from(left.local_rank) * u128::from(right.primary_population))
            .cmp(&(u128::from(right.local_rank) * u128::from(left.primary_population)))
            .then_with(|| {
                f32::from_bits(left.proposal.alternate_violation_bits)
                    .total_cmp(&f32::from_bits(right.proposal.alternate_violation_bits))
            })
            .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
            .then_with(|| {
                left.proposal
                    .alternate_posting
                    .cmp(&right.proposal.alternate_posting)
            })
    });

    let maximum_alternates = usize::try_from(maximum_alternates)
        .map_err(|_| invalid("V38 spill alternate limit exceeds address space"))?;
    let mut accepted = Vec::with_capacity(maximum_alternates.min(primary.len()));
    for ranked in ranked {
        if accepted.len() == maximum_alternates {
            break;
        }
        let target = ranked.proposal.alternate_posting as usize;
        if populations[target] >= maximum_rows_per_posting {
            continue;
        }
        populations[target] += 1;
        accepted.push((ranked.source_ordinal, ranked.proposal));
    }

    let mut accepted_by_posting = vec![Vec::<(u64, V38SpillProposal)>::new(); posting_count_usize];
    for accepted in accepted {
        accepted_by_posting[accepted.1.alternate_posting as usize].push(accepted);
    }
    let mut alternate_records = Vec::new();
    for (posting, accepted) in accepted_by_posting.iter_mut().enumerate() {
        accepted.sort_unstable_by_key(|accepted| accepted.0);
        let primary_population = primary_populations[posting] as usize;
        for (index, &(source_ordinal, proposal)) in accepted.iter().enumerate() {
            let primary_record = &primary[primary
                .binary_search_by_key(&source_ordinal, |record| record.source_ordinal)
                .map_err(|_| invalid("V38 accepted source differs"))?];
            alternate_records.push(V38SpillRecord {
                source_ordinal,
                feature_row_id: primary_record.feature_row_id,
                posting_ordinal: posting as u32,
                owner_role: 1,
                posting_local_ordinal: u32::try_from(primary_population + index)
                    .map_err(|_| invalid("V38 alternate local ordinal overflows"))?,
                alternate_violation_bits: Some(proposal.alternate_violation_bits),
            });
        }
    }
    let mut relation = Vec::with_capacity(primary.len() + alternate_records.len());
    relation.extend(primary.iter().map(|record| V38SpillRecord {
        source_ordinal: record.source_ordinal,
        feature_row_id: record.feature_row_id,
        posting_ordinal: record.posting_ordinal,
        owner_role: 0,
        posting_local_ordinal: record.posting_local_ordinal,
        alternate_violation_bits: None,
    }));
    relation.extend(alternate_records);
    relation.sort_unstable_by(|left, right| {
        left.source_ordinal
            .cmp(&right.source_ordinal)
            .then_with(|| left.owner_role.cmp(&right.owner_role))
    });
    Ok(relation)
}

#[cfg(test)]
mod tests {
    use super::{
        V38ArtifactIdentity, V38CeilingAuthority, V38ConstructionAuthority, V38OutputTarget,
        V38PostingSummary, V38SpillProposal, V38SpillRelation, V38SpillSpec,
        admit_v38_spill_proposals, build_v38_spill_relation, canonical_v38_ceiling_authority_bytes,
        canonical_v38_construction_authority_bytes, decode_v38_posting_summary_parquet,
        decode_v38_spill_relation_parquet, encode_v38_posting_summary_parquet,
        encode_v38_spill_relation_parquet, encode_v38_spill_relation_parquet_with_row_group_size,
        project_v38_admission_auxiliary_bytes, project_v38_spill_capacity,
        propose_v38_alternate_owner, summarize_v38_spill_relation,
        validate_v38_ceiling_authority_bytes, validate_v38_construction_authority_bytes,
        validate_v38_spill_spec,
    };
    use crate::v37_relation_router::{V37BalancedNode, V37BalancedTree, V37OwnershipRecord};

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn artifact(role: &str, byte: u8) -> V38ArtifactIdentity {
        V38ArtifactIdentity {
            blake3: digest(byte.wrapping_add(1)),
            encoded_bytes: 100 + u64::from(byte),
            role: role.to_owned(),
            sha256: digest(byte),
            uri: format!("s3://borsuk-test/v38/{role}-{byte}"),
        }
    }

    fn registered_spec() -> V38SpillSpec {
        V38SpillSpec {
            corpus_rows: 1_000_000,
            posting_count: 123,
            maximum_alternate_assignments: 250_000,
            maximum_owners_per_row: 2,
            maximum_rows_per_posting: 10_240,
            projected_record_bytes: 48,
            projected_framing_allowance_bytes: 32_768,
            selected_postings: 14,
        }
    }

    fn construction_authority() -> V38ConstructionAuthority {
        V38ConstructionAuthority {
            algorithm: "query-blind-hyperplane-boundary-spill-v1".to_owned(),
            inputs: vec![
                artifact("v37-authority", 1),
                artifact("v37-construction-result", 3),
                artifact("v37-tree", 5),
                artifact("v37-ownership", 7),
                artifact("source", 9),
            ],
            metric: "squared-l2".to_owned(),
            numeric_backend: "aarch64-neon-fma".to_owned(),
            outputs: vec![
                V38OutputTarget {
                    role: "spill-relation".to_owned(),
                    uri: "s3://borsuk-test/v38/spill-relation.parquet".to_owned(),
                },
                V38OutputTarget {
                    role: "spill-postings".to_owned(),
                    uri: "s3://borsuk-test/v38/spill-postings.parquet".to_owned(),
                },
            ],
            schema: "borsuk-v38-boundary-spill-authority-v1".to_owned(),
            source_archive: artifact("source-archive", 11),
            source_commit: digest(13),
            spec: registered_spec(),
            workers: 32,
        }
    }

    fn ceiling_authority() -> V38CeilingAuthority {
        V38CeilingAuthority {
            aggregate_gate_ppm: 998_000,
            construction_result: artifact("v38-construction-result", 21),
            development_ground_truth: artifact("gt100", 23),
            gt_neighbors: 100,
            maximum_certificate_bytes: 256,
            maximum_query_solver_nodes: 250_000,
            maximum_solver_nodes: 25_000_000,
            minimum_gate_ppm: 800_000,
            posting_summary: artifact("spill-postings", 25),
            query_count: 1_000,
            relation: artifact("spill-relation", 27),
            schema: "borsuk-v38-boundary-spill-ceiling-authority-v1".to_owned(),
            selected_postings: 14,
        }
    }

    fn three_leaf_tree(root_normal: [f32; 2]) -> V37BalancedTree {
        let backend =
            crate::v37_relation_router::score_v37_hyperplane_fused(&[0.0, 0.0], &[1.0, 0.0])
                .unwrap()
                .1
                .to_owned();
        V37BalancedTree {
            dimensions: 2,
            seed: 37,
            fma_backend: backend,
            nodes: vec![
                V37BalancedNode {
                    normal: root_normal.to_vec(),
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 0,
                    left_node: Some(1),
                    right_node: Some(2),
                    posting_ordinal: None,
                    population: 6,
                },
                V37BalancedNode {
                    normal: vec![],
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 1,
                    left_node: None,
                    right_node: None,
                    posting_ordinal: Some(0),
                    population: 2,
                },
                V37BalancedNode {
                    normal: vec![0.0, 1.0],
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 2,
                    left_node: Some(3),
                    right_node: Some(4),
                    posting_ordinal: None,
                    population: 4,
                },
                V37BalancedNode {
                    normal: vec![],
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 3,
                    left_node: None,
                    right_node: None,
                    posting_ordinal: Some(1),
                    population: 2,
                },
                V37BalancedNode {
                    normal: vec![],
                    boundary_score_bits: 0.0_f32.to_bits(),
                    boundary_source_ordinal: 4,
                    left_node: None,
                    right_node: None,
                    posting_ordinal: Some(2),
                    population: 2,
                },
            ],
            leaf_populations: vec![2, 2, 2],
            assignments: vec![],
        }
    }

    #[test]
    fn v38_boundary_authority_projects_exact_registered_capacity() {
        let projection = project_v38_spill_capacity(&registered_spec()).unwrap();
        assert_eq!(projection.total_posting_capacity, 1_259_520);
        assert_eq!(projection.maximum_total_assignments, 1_250_000);
        assert_eq!(projection.maximum_posting_payload_bytes, 491_520);
        assert_eq!(projection.maximum_complete_object_bytes, 524_288);
        validate_v38_spill_spec(&registered_spec()).unwrap();
    }

    #[test]
    fn v38_boundary_authority_rejects_nonregistered_and_overflowing_specs() {
        for mutate in [
            |spec: &mut V38SpillSpec| spec.maximum_alternate_assignments = 250_001,
            |spec: &mut V38SpillSpec| spec.maximum_owners_per_row = 3,
            |spec: &mut V38SpillSpec| spec.maximum_rows_per_posting = 10_241,
            |spec: &mut V38SpillSpec| spec.projected_record_bytes = 49,
            |spec: &mut V38SpillSpec| spec.selected_postings = 15,
        ] {
            let mut changed = registered_spec();
            mutate(&mut changed);
            assert!(validate_v38_spill_spec(&changed).is_err());
        }
        let mut overflow = registered_spec();
        overflow.corpus_rows = u64::MAX;
        overflow.posting_count = u32::MAX;
        assert!(project_v38_spill_capacity(&overflow).is_err());
    }

    #[test]
    fn v38_boundary_authority_canonicalizes_and_binds_construction_roles() {
        let authority = construction_authority();
        let bytes = canonical_v38_construction_authority_bytes(&authority).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            validate_v38_construction_authority_bytes(&bytes).unwrap(),
            bytes
        );

        let mut changed = authority.clone();
        changed.inputs[0].role = "construction-manifest".to_owned();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
        let mut changed = authority.clone();
        changed.outputs[0].uri = changed.inputs[0].uri.clone();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
        let mut changed = authority;
        changed.numeric_backend = "scalar-control".to_owned();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_authority_rejects_schema_type_and_digest_drift() {
        let bytes = canonical_v38_construction_authority_bytes(&construction_authority()).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("extra".to_owned(), true.into());
        assert!(
            validate_v38_construction_authority_bytes(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["workers"] = "32".into();
        assert!(
            validate_v38_construction_authority_bytes(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        let mut changed = construction_authority();
        changed.inputs[2].sha256 = digest(0xff).to_uppercase();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_authority_ceiling_binds_exact_solver_and_artifact_roles() {
        let authority = ceiling_authority();
        let bytes = canonical_v38_ceiling_authority_bytes(&authority).unwrap();
        assert_eq!(validate_v38_ceiling_authority_bytes(&bytes).unwrap(), bytes);
        for mutate in [
            |value: &mut V38CeilingAuthority| value.selected_postings = 13,
            |value: &mut V38CeilingAuthority| value.maximum_solver_nodes += 1,
            |value: &mut V38CeilingAuthority| value.aggregate_gate_ppm = 997_999,
            |value: &mut V38CeilingAuthority| value.minimum_gate_ppm = 799_999,
        ] {
            let mut changed = authority.clone();
            mutate(&mut changed);
            assert!(canonical_v38_ceiling_authority_bytes(&changed).is_err());
        }
        let mut changed = authority;
        changed.relation.role = "spill-postings".to_owned();
        assert!(canonical_v38_ceiling_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_geometry_scores_wrong_side_path_without_renormalizing() {
        let tree = three_leaf_tree([1.000_001, 0.0]);
        let proposal = propose_v38_alternate_owner(&tree, &[-2.0, 1.0], 0).unwrap();
        assert_eq!(proposal.alternate_posting, 1);
        let left_score = (-2.0_f32).mul_add(1.000_001, 0.0);
        assert_eq!(
            proposal.alternate_violation_bits,
            (left_score * left_score).to_bits()
        );

        let proposal = propose_v38_alternate_owner(&tree, &[2.0, -1.0], 1).unwrap();
        assert_eq!(proposal.alternate_posting, 2);
        assert_eq!(proposal.alternate_violation_bits, 1.0_f32.to_bits());
    }

    #[test]
    fn v38_boundary_geometry_excludes_primary_and_breaks_zero_margin_ties_by_posting() {
        let tree = three_leaf_tree([1.0, 0.0]);
        let proposal = propose_v38_alternate_owner(&tree, &[0.0, 0.0], 0).unwrap();
        assert_eq!(proposal.alternate_posting, 1);
        assert_eq!(proposal.alternate_violation_bits, 0.0_f32.to_bits());
        assert!(propose_v38_alternate_owner(&tree, &[f32::NAN, 0.0], 0).is_err());
        assert!(propose_v38_alternate_owner(&tree, &[0.0], 0).is_err());
        assert!(propose_v38_alternate_owner(&tree, &[0.0, 0.0], 9).is_err());
    }

    #[test]
    fn v38_boundary_geometry_exercises_registered_192d_fused_blocks() {
        let mut tree = three_leaf_tree([1.0, 0.0]);
        tree.dimensions = 192;
        tree.nodes[0].normal = vec![0.0; 192];
        tree.nodes[0].normal[0] = 1.0;
        tree.nodes[2].normal = vec![0.0; 192];
        tree.nodes[2].normal[1] = 1.0;
        let mut row = vec![0.0; 192];
        row[0] = -2.0;
        row[1] = 1.0;
        for node in [&tree.nodes[0], &tree.nodes[2]] {
            let fused = crate::v37_relation_router::score_v37_hyperplane_fused(&row, &node.normal)
                .unwrap()
                .0;
            let scalar =
                crate::v37_relation_router::score_v37_hyperplane_scalar(&row, &node.normal)
                    .unwrap();
            assert_eq!(fused.to_bits(), scalar.to_bits());
        }
        let proposal = propose_v38_alternate_owner(&tree, &row, 0).unwrap();
        assert_eq!(proposal.alternate_posting, 1);
        assert_eq!(proposal.alternate_violation_bits, 4.0_f32.to_bits());
    }

    fn primary_record(
        source_ordinal: u64,
        feature_row_id: u64,
        posting_ordinal: u32,
        posting_local_ordinal: u32,
    ) -> V37OwnershipRecord {
        V37OwnershipRecord {
            source_ordinal,
            feature_row_id,
            posting_ordinal,
            posting_local_ordinal,
        }
    }

    fn proposal(alternate_posting: u32, violation: f32) -> V38SpillProposal {
        V38SpillProposal {
            alternate_posting,
            alternate_violation_bits: violation.to_bits(),
        }
    }

    #[test]
    fn v38_boundary_admission_uses_exact_fractional_fairness_and_capacity() {
        let primary = vec![
            primary_record(0, 100, 1, 0),
            primary_record(1, 101, 1, 1),
            primary_record(2, 102, 2, 0),
            primary_record(3, 103, 2, 1),
            primary_record(4, 104, 0, 0),
            primary_record(5, 105, 0, 1),
            primary_record(6, 106, 0, 2),
            primary_record(7, 107, 0, 3),
        ];
        let proposals = vec![
            (0, proposal(2, 0.5)),
            (1, proposal(0, 8.0)),
            (2, proposal(0, 5.0)),
            (3, proposal(0, 6.0)),
            (4, proposal(1, 4.0)),
            (5, proposal(1, 3.0)),
            (6, proposal(1, 2.0)),
            (7, proposal(2, 1.0)),
        ];
        let relation = admit_v38_spill_proposals(&primary, &proposals, 3, 2, 5).unwrap();
        let alternates: Vec<_> = relation
            .iter()
            .filter(|record| record.owner_role == 1)
            .map(|record| {
                (
                    record.source_ordinal,
                    record.posting_ordinal,
                    record.posting_local_ordinal,
                )
            })
            .collect();
        assert_eq!(alternates, vec![(0, 2, 2), (7, 2, 3)]);
    }

    #[test]
    fn v38_boundary_admission_never_falls_back_and_appends_by_source_ordinal() {
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
            primary_record(4, 104, 2, 0),
            primary_record(5, 105, 2, 1),
        ];
        let proposals = vec![
            (3, proposal(2, 1.0)),
            (2, proposal(2, 0.5)),
            (1, proposal(2, 2.0)),
            (0, proposal(2, 3.0)),
            (4, proposal(0, 4.0)),
            (5, proposal(0, 5.0)),
        ];
        let relation = admit_v38_spill_proposals(&primary, &proposals, 3, 4, 3).unwrap();
        let alternates: Vec<_> = relation
            .iter()
            .filter(|record| record.owner_role == 1)
            .map(|record| {
                (
                    record.source_ordinal,
                    record.posting_ordinal,
                    record.posting_local_ordinal,
                )
            })
            .collect();
        assert_eq!(alternates, vec![(2, 2, 2), (4, 0, 2)]);
    }

    fn codec_fixture() -> (Vec<V37OwnershipRecord>, Vec<super::V38SpillRecord>) {
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
        ];
        let proposals = vec![
            (0, proposal(1, 0.25)),
            (1, proposal(1, 0.5)),
            (2, proposal(0, 0.75)),
            (3, proposal(0, 1.0)),
        ];
        let relation = admit_v38_spill_proposals(&primary, &proposals, 2, 2, 3).unwrap();
        (primary, relation)
    }

    #[test]
    fn v38_boundary_codec_round_trips_exact_relation_and_posting_summary() {
        let (primary, relation) = codec_fixture();
        let relation_bytes = encode_v38_spill_relation_parquet(&relation).unwrap();
        assert_eq!(
            decode_v38_spill_relation_parquet(&relation_bytes, &primary, 2, 3).unwrap(),
            relation
        );

        let summaries = summarize_v38_spill_relation(&relation, 2, 3).unwrap();
        assert_eq!(
            summaries,
            vec![
                V38PostingSummary {
                    posting_ordinal: 0,
                    primary_population: 2,
                    alternate_population: 1,
                    total_population: 3,
                    projected_payload_bytes: 144,
                    projected_framing_allowance_bytes: 32_768,
                },
                V38PostingSummary {
                    posting_ordinal: 1,
                    primary_population: 2,
                    alternate_population: 1,
                    total_population: 3,
                    projected_payload_bytes: 144,
                    projected_framing_allowance_bytes: 32_768,
                },
            ]
        );
        let summary_bytes = encode_v38_posting_summary_parquet(&summaries).unwrap();
        assert_eq!(
            decode_v38_posting_summary_parquet(&summary_bytes, &relation, 2, 3).unwrap(),
            summaries
        );
    }

    #[test]
    fn v38_boundary_codec_rejects_order_owner_population_and_violation_drift() {
        let (primary, relation) = codec_fixture();
        let valid = encode_v38_spill_relation_parquet(&relation).unwrap();
        assert!(
            decode_v38_spill_relation_parquet(&valid[..valid.len() - 1], &primary, 2, 3).is_err()
        );

        let mut changed = relation.clone();
        changed.swap(0, 1);
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[0].owner_role = 1;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[1].alternate_violation_bits = Some(f32::NAN.to_bits());
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[1].feature_row_id += 1;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[1].posting_ordinal = changed[0].posting_ordinal;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
        let mut changed = relation.clone();
        changed[0].posting_ordinal = u32::MAX;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());

        let summaries = summarize_v38_spill_relation(&relation, 2, 3).unwrap();
        let mut changed = summaries.clone();
        changed[0].projected_payload_bytes += 1;
        assert!(encode_v38_posting_summary_parquet(&changed).is_err());
        let mut changed = summaries;
        changed[1].posting_ordinal = 0;
        assert!(encode_v38_posting_summary_parquet(&changed).is_err());
    }

    #[test]
    fn v38_boundary_codec_accepts_multigroup_and_rejects_alternate_ordinal_swap() {
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
        ];
        let relation = vec![
            super::V38SpillRecord {
                source_ordinal: 0,
                feature_row_id: 100,
                posting_ordinal: 0,
                owner_role: 0,
                posting_local_ordinal: 0,
                alternate_violation_bits: None,
            },
            super::V38SpillRecord {
                source_ordinal: 0,
                feature_row_id: 100,
                posting_ordinal: 1,
                owner_role: 1,
                posting_local_ordinal: 2,
                alternate_violation_bits: Some(0.25_f32.to_bits()),
            },
            super::V38SpillRecord {
                source_ordinal: 1,
                feature_row_id: 101,
                posting_ordinal: 0,
                owner_role: 0,
                posting_local_ordinal: 1,
                alternate_violation_bits: None,
            },
            super::V38SpillRecord {
                source_ordinal: 1,
                feature_row_id: 101,
                posting_ordinal: 1,
                owner_role: 1,
                posting_local_ordinal: 3,
                alternate_violation_bits: Some(0.5_f32.to_bits()),
            },
            super::V38SpillRecord {
                source_ordinal: 2,
                feature_row_id: 102,
                posting_ordinal: 1,
                owner_role: 0,
                posting_local_ordinal: 0,
                alternate_violation_bits: None,
            },
            super::V38SpillRecord {
                source_ordinal: 3,
                feature_row_id: 103,
                posting_ordinal: 1,
                owner_role: 0,
                posting_local_ordinal: 1,
                alternate_violation_bits: None,
            },
        ];
        let encoded = encode_v38_spill_relation_parquet_with_row_group_size(&relation, 2).unwrap();
        assert_eq!(
            decode_v38_spill_relation_parquet(&encoded, &primary, 2, 4).unwrap(),
            relation
        );
        let mut changed = relation;
        changed[1].posting_local_ordinal = 3;
        changed[3].posting_local_ordinal = 2;
        assert!(encode_v38_spill_relation_parquet(&changed).is_err());
    }

    #[test]
    fn v38_boundary_builder_projects_registered_auxiliary_memory_below_64_mib() {
        let bytes = project_v38_admission_auxiliary_bytes(1_000_000, 250_000, 123).unwrap();
        assert_eq!(bytes, 52_003_936);
        assert!(bytes <= 64 * 1_048_576);
        assert!(project_v38_admission_auxiliary_bytes(u64::MAX, 250_000, 123).is_err());
    }

    #[test]
    fn v38_boundary_builder_replays_primary_and_is_worker_deterministic() {
        let tree = three_leaf_tree([1.0, 0.0]);
        let rows = vec![
            vec![-1.0, 0.0],
            vec![-2.0, 0.0],
            vec![1.0, -1.0],
            vec![2.0, -2.0],
            vec![1.0, 1.0],
            vec![2.0, 2.0],
        ];
        let primary = vec![
            primary_record(0, 100, 0, 0),
            primary_record(1, 101, 0, 1),
            primary_record(2, 102, 1, 0),
            primary_record(3, 103, 1, 1),
            primary_record(4, 104, 2, 0),
            primary_record(5, 105, 2, 1),
        ];
        let one: V38SpillRelation =
            build_v38_spill_relation(&tree, &rows, &primary, 3, 3, 1).unwrap();
        let four = build_v38_spill_relation(&tree, &rows, &primary, 3, 3, 4).unwrap();
        assert_eq!(one, four);
        assert_eq!(one.records.len(), 8);
        assert_eq!(
            one.postings
                .iter()
                .map(|value| value.total_population)
                .sum::<u64>(),
            8
        );

        let mut changed = primary.clone();
        changed[0].posting_ordinal = 1;
        assert!(build_v38_spill_relation(&tree, &rows, &changed, 3, 3, 1).is_err());
        let mut changed = rows;
        changed[0][0] = f32::NAN;
        assert!(build_v38_spill_relation(&tree, &changed, &primary, 3, 3, 1).is_err());
    }
}
