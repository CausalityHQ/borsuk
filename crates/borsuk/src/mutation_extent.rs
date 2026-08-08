use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    ListArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
    builder::{Float32Builder, ListBuilder, StringBuilder, UInt32Builder},
    types::Float32Type,
    types::UInt32Type,
};
use arrow_ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema};

use crate::{
    BorsukError, Result, StorageEncoding, VectorRecord,
    mutation::{CanonicalMutation, MutationOperation, MutationVersion},
};

const SCHEMA_VERSION: &str = "31";
const OBJECT_ROLE: &str = "mutation_extent";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MutationExtentIdentity {
    pub(crate) stripe: u16,
    pub(crate) lease_epoch: u64,
    pub(crate) sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationExtentIdState {
    Inserted,
    Live,
    Deleted,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DecodedMutationExtent {
    pub(crate) identity: MutationExtentIdentity,
    pub(crate) dimensions: usize,
    pub(crate) max_version: MutationVersion,
    pub(crate) mutations: Vec<CanonicalMutation>,
    pub(crate) id_states: Vec<MutationExtentIdState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MutationExtentMetadata {
    pub(crate) identity: MutationExtentIdentity,
    pub(crate) dimensions: usize,
    pub(crate) row_count: usize,
    pub(crate) max_version: MutationVersion,
}

pub(crate) fn encode_mutation_extent(
    identity: MutationExtentIdentity,
    dimensions: usize,
    mutations: &[CanonicalMutation],
    id_states: &[MutationExtentIdState],
) -> Result<Vec<u8>> {
    validate_identity(identity)?;
    if dimensions == 0 {
        return Err(BorsukError::InvalidRecordInput(
            "mutation extent dimensions must be positive".to_owned(),
        ));
    }
    if mutations.is_empty() {
        return Err(BorsukError::InvalidRecordInput(
            "mutation extent must contain at least one mutation".to_owned(),
        ));
    }
    if mutations.len() != id_states.len() {
        return Err(BorsukError::InvalidRecordInput(
            "mutation extent rows and ID states must have equal length".to_owned(),
        ));
    }
    for (mutation, id_state) in mutations.iter().zip(id_states) {
        match (mutation.operation(), mutation.record(), id_state) {
            (
                MutationOperation::Put,
                Some(record),
                MutationExtentIdState::Inserted | MutationExtentIdState::Live,
            ) => {
                if record.id != *mutation.id()
                    || record.vector.len() != dimensions
                    || record.mutation_stamp() != Some(mutation.stamp())
                {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "mutation extent record `{}` has inconsistent ID, dimensions, or stamp",
                        mutation.id()
                    )));
                }
                if record.vector.iter().any(|value| !value.is_finite()) {
                    return Err(BorsukError::InvalidRecordInput(format!(
                        "mutation extent record `{}` contains a non-finite vector value",
                        mutation.id()
                    )));
                }
            }
            (MutationOperation::Delete, None, MutationExtentIdState::Deleted) => {}
            _ => {
                return Err(BorsukError::InvalidStorage(
                    "canonical mutation operation and record payload disagree".to_owned(),
                ));
            }
        }
    }

    let max_version = mutations
        .iter()
        .map(|mutation| mutation.stamp().version())
        .max()
        .expect("non-empty mutations have a maximum version");
    let schema = Arc::new(mutation_extent_schema(
        identity,
        dimensions,
        mutations.len(),
        max_version,
    )?);
    let metadata_json = mutations
        .iter()
        .map(|mutation| {
            mutation
                .record()
                .filter(|record| !record.metadata.is_empty())
                .map(|record| serde_json::to_string(&record.metadata))
                .transpose()
                .map_err(|error| {
                    BorsukError::InvalidRecordInput(format!(
                        "mutation metadata is not JSON-serializable: {error}"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let multimodal = encode_multimodal_columns(mutations)?;

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                mutations.iter().map(|mutation| mutation.id().as_bytes()),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(mutations.iter().map(
                |mutation| match mutation.operation() {
                    MutationOperation::Put => "put",
                    MutationOperation::Delete => "delete",
                },
            ))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(id_states.iter().map(
                |state| match state {
                    MutationExtentIdState::Inserted => "inserted",
                    MutationExtentIdState::Live => "live",
                    MutationExtentIdState::Deleted => "deleted",
                },
            ))) as ArrayRef,
            Arc::new(UInt64Array::from_iter_values(
                mutations
                    .iter()
                    .map(|mutation| mutation.stamp().version().hlc()),
            )) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                mutations
                    .iter()
                    .map(|mutation| mutation.stamp().version().writer()),
            )?) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                mutations.iter().map(|mutation| mutation.stamp().digest()),
            )?) as ArrayRef,
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    mutations.iter().map(|mutation| {
                        mutation.record().map(|record| {
                            record.vector.iter().copied().map(Some).collect::<Vec<_>>()
                        })
                    }),
                    i32::try_from(dimensions).map_err(|_| {
                        BorsukError::InvalidRecordInput(
                            "mutation extent dimensions exceed i32".to_owned(),
                        )
                    })?,
                ),
            ) as ArrayRef,
            Arc::new(StringArray::from_iter(
                metadata_json.iter().map(Option::as_deref),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter(mutations.iter().map(|mutation| {
                mutation.record().and_then(|record| record.text.as_deref())
            }))) as ArrayRef,
            Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(
                mutations.iter().map(|mutation| {
                    mutation.record().map(|record| {
                        record
                            .text_term_ids
                            .iter()
                            .copied()
                            .map(Some)
                            .collect::<Vec<_>>()
                    })
                }),
            )) as ArrayRef,
            Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(
                mutations.iter().map(|mutation| {
                    mutation.record().map(|record| {
                        record
                            .text_term_freqs
                            .iter()
                            .copied()
                            .map(Some)
                            .collect::<Vec<_>>()
                    })
                }),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter(mutations.iter().map(|mutation| {
                mutation.record().map(|record| match record.storage {
                    StorageEncoding::Auto => "auto",
                    StorageEncoding::Dense => "dense",
                    StorageEncoding::Sparse => "sparse",
                })
            }))) as ArrayRef,
            Arc::new(multimodal.dense_names) as ArrayRef,
            Arc::new(multimodal.dense_values) as ArrayRef,
            Arc::new(multimodal.sparse_names) as ArrayRef,
            Arc::new(multimodal.sparse_indices) as ArrayRef,
            Arc::new(multimodal.sparse_values) as ArrayRef,
            Arc::new(multimodal.late_names) as ArrayRef,
            Arc::new(multimodal.late_dimensions) as ArrayRef,
            Arc::new(multimodal.late_element_types) as ArrayRef,
            Arc::new(multimodal.late_values) as ArrayRef,
        ],
    )?;

    let mut output = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new_with_options(&mut output, &schema, IpcWriteOptions::default())?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(output)
}

struct EncodedMultimodalColumns {
    dense_names: ListArray,
    dense_values: ListArray,
    sparse_names: ListArray,
    sparse_indices: ListArray,
    sparse_values: ListArray,
    late_names: ListArray,
    late_dimensions: ListArray,
    late_element_types: ListArray,
    late_values: ListArray,
}

fn encode_multimodal_columns(mutations: &[CanonicalMutation]) -> Result<EncodedMultimodalColumns> {
    let mut dense_names = ListBuilder::new(StringBuilder::new());
    let mut dense_values = ListBuilder::new(ListBuilder::new(Float32Builder::new()));
    let mut sparse_names = ListBuilder::new(StringBuilder::new());
    let mut sparse_indices = ListBuilder::new(ListBuilder::new(UInt32Builder::new()));
    let mut sparse_values = ListBuilder::new(ListBuilder::new(Float32Builder::new()));
    let mut late_names = ListBuilder::new(StringBuilder::new());
    let mut late_dimensions = ListBuilder::new(UInt32Builder::new());
    let mut late_element_types = ListBuilder::new(StringBuilder::new());
    let mut late_values = ListBuilder::new(ListBuilder::new(Float32Builder::new()));

    for mutation in mutations {
        let Some(record) = mutation.record() else {
            dense_names.append(false);
            dense_values.append(false);
            sparse_names.append(false);
            sparse_indices.append(false);
            sparse_values.append(false);
            late_names.append(false);
            late_dimensions.append(false);
            late_element_types.append(false);
            late_values.append(false);
            continue;
        };

        for (name, vector) in &record.extra_vectors {
            dense_names.values().append_value(name);
            dense_values.values().values().append_slice(vector);
            dense_values.values().append(true);
        }
        dense_names.append(true);
        dense_values.append(true);

        for (name, vector) in &record.extra_sparse {
            sparse_names.values().append_value(name);
            sparse_indices
                .values()
                .values()
                .append_slice(vector.indices());
            sparse_indices.values().append(true);
            sparse_values
                .values()
                .values()
                .append_slice(vector.values());
            sparse_values.values().append(true);
        }
        sparse_names.append(true);
        sparse_indices.append(true);
        sparse_values.append(true);

        for (name, vector) in &record.extra_multi_vectors {
            late_names.values().append_value(name);
            late_dimensions
                .values()
                .append_value(u32::try_from(vector.dimensions()).map_err(|_| {
                    BorsukError::InvalidRecordInput(
                        "late-interaction dimensions exceed u32".to_owned(),
                    )
                })?);
            late_element_types
                .values()
                .append_value(vector.element_type().as_str());
            for token in vector.tokens() {
                late_values.values().values().append_slice(token);
            }
            late_values.values().append(true);
        }
        late_names.append(true);
        late_dimensions.append(true);
        late_element_types.append(true);
        late_values.append(true);
    }

    Ok(EncodedMultimodalColumns {
        dense_names: dense_names.finish(),
        dense_values: dense_values.finish(),
        sparse_names: sparse_names.finish(),
        sparse_indices: sparse_indices.finish(),
        sparse_values: sparse_values.finish(),
        late_names: late_names.finish(),
        late_dimensions: late_dimensions.finish(),
        late_element_types: late_element_types.finish(),
        late_values: late_values.finish(),
    })
}

pub(crate) fn decode_mutation_extent(bytes: &[u8]) -> Result<DecodedMutationExtent> {
    validate_stream_end(bytes)?;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let metadata = metadata_from_schema(&schema)?;

    let mut mutations = Vec::new();
    let mut id_states = Vec::new();
    for batch in &mut reader {
        decode_batch(&batch?, metadata.dimensions, &mut mutations, &mut id_states)?;
    }
    if mutations.len() != metadata.row_count {
        return Err(BorsukError::InvalidStorage(
            "mutation extent row-count metadata does not match its batches".to_owned(),
        ));
    }
    let max_version = mutations
        .iter()
        .map(|mutation| mutation.stamp().version())
        .max()
        .expect("decoded mutations have a maximum version");
    if max_version != metadata.max_version {
        return Err(BorsukError::InvalidStorage(
            "mutation extent max-version metadata does not match its rows".to_owned(),
        ));
    }
    Ok(DecodedMutationExtent {
        identity: metadata.identity,
        dimensions: metadata.dimensions,
        max_version,
        mutations,
        id_states,
    })
}

pub(crate) fn inspect_mutation_extent(bytes: &[u8]) -> Result<MutationExtentMetadata> {
    validate_stream_end(bytes)?;
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    metadata_from_schema(&reader.schema())
}

fn validate_stream_end(bytes: &[u8]) -> Result<()> {
    const ARROW_STREAM_END: &[u8; 8] = b"\xff\xff\xff\xff\0\0\0\0";
    if !bytes.ends_with(ARROW_STREAM_END) {
        return Err(BorsukError::InvalidStorage(
            "mutation extent is truncated or contains bytes after the Arrow stream".to_owned(),
        ));
    }
    Ok(())
}

fn metadata_from_schema(schema: &Schema) -> Result<MutationExtentMetadata> {
    let identity = MutationExtentIdentity {
        stripe: parse_metadata(schema, "borsuk.stripe")?,
        lease_epoch: parse_metadata(schema, "borsuk.lease_epoch")?,
        sequence: parse_metadata(schema, "borsuk.sequence")?,
    };
    validate_identity(identity)?;
    require_metadata(schema, "borsuk.object_role", OBJECT_ROLE)?;
    require_metadata(schema, "borsuk.schema_version", SCHEMA_VERSION)?;
    let dimensions: usize = parse_metadata(schema, "borsuk.dimensions")?;
    let row_count: usize = parse_metadata(schema, "borsuk.row_count")?;
    if dimensions == 0 || row_count == 0 {
        return Err(BorsukError::InvalidStorage(
            "mutation extent dimensions and row count must be positive".to_owned(),
        ));
    }
    let max_hlc: u64 = parse_metadata(schema, "borsuk.max_mutation_hlc")?;
    let max_writer =
        uuid::Uuid::parse_str(required_metadata(schema, "borsuk.max_mutation_writer")?)
            .map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "mutation extent max writer is not a UUID: {error}"
                ))
            })?
            .into_bytes();
    validate_schema(schema, dimensions)?;
    Ok(MutationExtentMetadata {
        identity,
        dimensions,
        row_count,
        max_version: MutationVersion::from_parts(max_hlc, max_writer),
    })
}

fn mutation_extent_schema(
    identity: MutationExtentIdentity,
    dimensions: usize,
    row_count: usize,
    max_version: MutationVersion,
) -> Result<Schema> {
    let dimensions_i32 = i32::try_from(dimensions).map_err(|_| {
        BorsukError::InvalidRecordInput("mutation extent dimensions exceed i32".to_owned())
    })?;
    Ok(Schema::new_with_metadata(
        vec![
            Field::new("record_id", DataType::Binary, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("id_state", DataType::Utf8, false),
            Field::new("mutation_hlc", DataType::UInt64, false),
            Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
            Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    dimensions_i32,
                ),
                true,
            ),
            Field::new("metadata_json", DataType::Utf8, true),
            Field::new("text", DataType::Utf8, true),
            Field::new(
                "text_term_ids",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
                true,
            ),
            Field::new(
                "text_term_freqs",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
                true,
            ),
            Field::new("storage", DataType::Utf8, true),
            list_field("extra_dense_names", DataType::Utf8),
            nested_list_field("extra_dense_values", DataType::Float32),
            list_field("extra_sparse_names", DataType::Utf8),
            nested_list_field("extra_sparse_indices", DataType::UInt32),
            nested_list_field("extra_sparse_values", DataType::Float32),
            list_field("late_interaction_names", DataType::Utf8),
            list_field("late_interaction_dimensions", DataType::UInt32),
            list_field("late_interaction_element_types", DataType::Utf8),
            nested_list_field("late_interaction_values", DataType::Float32),
        ],
        HashMap::from([
            ("borsuk.object_role".to_owned(), OBJECT_ROLE.to_owned()),
            (
                "borsuk.schema_version".to_owned(),
                SCHEMA_VERSION.to_owned(),
            ),
            ("borsuk.stripe".to_owned(), identity.stripe.to_string()),
            (
                "borsuk.lease_epoch".to_owned(),
                identity.lease_epoch.to_string(),
            ),
            ("borsuk.sequence".to_owned(), identity.sequence.to_string()),
            ("borsuk.dimensions".to_owned(), dimensions.to_string()),
            ("borsuk.row_count".to_owned(), row_count.to_string()),
            (
                "borsuk.max_mutation_hlc".to_owned(),
                max_version.hlc().to_string(),
            ),
            (
                "borsuk.max_mutation_writer".to_owned(),
                uuid::Uuid::from_bytes(max_version.writer()).to_string(),
            ),
        ]),
    ))
}

fn list_field(name: &str, child: DataType) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(child, true))),
        true,
    )
}

fn nested_list_field(name: &str, child: DataType) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(child, true))),
            true,
        ))),
        true,
    )
}

fn validate_schema(schema: &Schema, dimensions: usize) -> Result<()> {
    let expected = mutation_extent_schema(
        MutationExtentIdentity {
            stripe: 0,
            lease_epoch: 1,
            sequence: 1,
        },
        dimensions,
        1,
        MutationVersion::from_parts(0, [0; 16]),
    )?;
    if schema.fields() != expected.fields() {
        return Err(BorsukError::InvalidStorage(
            "mutation extent Arrow schema does not match standard schema v31".to_owned(),
        ));
    }
    Ok(())
}

fn decode_batch(
    batch: &RecordBatch,
    dimensions: usize,
    mutations: &mut Vec<CanonicalMutation>,
    id_states: &mut Vec<MutationExtentIdState>,
) -> Result<()> {
    let ids = column::<BinaryArray>(batch, "record_id")?;
    let operations = column::<StringArray>(batch, "operation")?;
    let encoded_id_states = column::<StringArray>(batch, "id_state")?;
    let hlcs = column::<UInt64Array>(batch, "mutation_hlc")?;
    let writers = column::<FixedSizeBinaryArray>(batch, "mutation_writer")?;
    let digests = column::<FixedSizeBinaryArray>(batch, "mutation_digest")?;
    let vectors = column::<FixedSizeListArray>(batch, "vector")?;
    let metadata_json = column::<StringArray>(batch, "metadata_json")?;
    let texts = column::<StringArray>(batch, "text")?;
    let term_ids = column::<ListArray>(batch, "text_term_ids")?;
    let term_freqs = column::<ListArray>(batch, "text_term_freqs")?;
    let storage = column::<StringArray>(batch, "storage")?;
    let dense_names = column::<ListArray>(batch, "extra_dense_names")?;
    let dense_values = column::<ListArray>(batch, "extra_dense_values")?;
    let sparse_names = column::<ListArray>(batch, "extra_sparse_names")?;
    let sparse_indices = column::<ListArray>(batch, "extra_sparse_indices")?;
    let sparse_values = column::<ListArray>(batch, "extra_sparse_values")?;
    let late_names = column::<ListArray>(batch, "late_interaction_names")?;
    let late_dimensions = column::<ListArray>(batch, "late_interaction_dimensions")?;
    let late_element_types = column::<ListArray>(batch, "late_interaction_element_types")?;
    let late_values = column::<ListArray>(batch, "late_interaction_values")?;

    for row in 0..batch.num_rows() {
        let writer: [u8; 16] = writers.value(row).try_into().map_err(|_| {
            BorsukError::InvalidStorage("mutation writer is not 16 bytes".to_owned())
        })?;
        let digest: [u8; 32] = digests.value(row).try_into().map_err(|_| {
            BorsukError::InvalidStorage("mutation digest is not 32 bytes".to_owned())
        })?;
        let version = MutationVersion::from_parts(hlcs.value(row), writer);
        let id = crate::RecordId::from(ids.value(row));
        let id_state = match encoded_id_states.value(row) {
            "inserted" => MutationExtentIdState::Inserted,
            "live" => MutationExtentIdState::Live,
            "deleted" => MutationExtentIdState::Deleted,
            other => {
                return Err(BorsukError::InvalidStorage(format!(
                    "mutation extent ID state `{other}` is invalid"
                )));
            }
        };
        let mutation = match (operations.value(row), id_state) {
            ("put", MutationExtentIdState::Inserted | MutationExtentIdState::Live) => {
                if vectors.is_null(row) || storage.is_null(row) {
                    return Err(BorsukError::InvalidStorage(
                        "put mutation is missing vector or storage".to_owned(),
                    ));
                }
                let values = vectors.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "mutation vector child is not Float32".to_owned(),
                        )
                    })?;
                if values.len() != dimensions || values.null_count() != 0 {
                    return Err(BorsukError::InvalidStorage(
                        "mutation vector has invalid dimensions or null coordinates".to_owned(),
                    ));
                }
                let mut record = VectorRecord::new_bytes(id.as_bytes(), values.values().to_vec());
                record.storage = match storage.value(row) {
                    "auto" => StorageEncoding::Auto,
                    "dense" => StorageEncoding::Dense,
                    "sparse" => StorageEncoding::Sparse,
                    value => {
                        return Err(BorsukError::InvalidStorage(format!(
                            "mutation extent has unknown storage value `{value}`"
                        )));
                    }
                };
                if !metadata_json.is_null(row) {
                    record.metadata =
                        serde_json::from_str(metadata_json.value(row)).map_err(|error| {
                            BorsukError::InvalidStorage(format!(
                                "mutation metadata JSON is invalid: {error}"
                            ))
                        })?;
                }
                decode_multimodal_columns(
                    row,
                    &mut record,
                    MultimodalColumns {
                        dense_names,
                        dense_values,
                        sparse_names,
                        sparse_indices,
                        sparse_values,
                        late_names,
                        late_dimensions,
                        late_element_types,
                        late_values,
                    },
                )?;
                if !texts.is_null(row) {
                    record.text = Some(texts.value(row).to_owned());
                }
                record.text_term_ids = list_u32_value(term_ids, row, "text_term_ids")?;
                record.text_term_freqs = list_u32_value(term_freqs, row, "text_term_freqs")?;
                CanonicalMutation::put(version, record)?
            }
            ("delete", MutationExtentIdState::Deleted) => {
                if !vectors.is_null(row)
                    || !metadata_json.is_null(row)
                    || !texts.is_null(row)
                    || !term_ids.is_null(row)
                    || !term_freqs.is_null(row)
                    || !storage.is_null(row)
                    || !dense_names.is_null(row)
                    || !dense_values.is_null(row)
                    || !sparse_names.is_null(row)
                    || !sparse_indices.is_null(row)
                    || !sparse_values.is_null(row)
                    || !late_names.is_null(row)
                    || !late_dimensions.is_null(row)
                    || !late_element_types.is_null(row)
                    || !late_values.is_null(row)
                {
                    return Err(BorsukError::InvalidStorage(
                        "delete mutation contains put-only fields".to_owned(),
                    ));
                }
                CanonicalMutation::delete(version, id)
            }
            (value, _) => {
                return Err(BorsukError::InvalidStorage(format!(
                    "mutation extent has unknown operation `{value}`"
                )));
            }
        };
        if mutation.stamp().digest() != digest {
            return Err(BorsukError::InvalidStorage(
                "mutation extent canonical digest does not match its logical row".to_owned(),
            ));
        }
        mutations.push(mutation);
        id_states.push(id_state);
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct MultimodalColumns<'a> {
    dense_names: &'a ListArray,
    dense_values: &'a ListArray,
    sparse_names: &'a ListArray,
    sparse_indices: &'a ListArray,
    sparse_values: &'a ListArray,
    late_names: &'a ListArray,
    late_dimensions: &'a ListArray,
    late_element_types: &'a ListArray,
    late_values: &'a ListArray,
}

fn decode_multimodal_columns(
    row: usize,
    record: &mut VectorRecord,
    columns: MultimodalColumns<'_>,
) -> Result<()> {
    let dense_names = list_string_value(columns.dense_names, row, "extra_dense_names")?;
    let dense_values = nested_f32_value(columns.dense_values, row, "extra_dense_values")?;
    if dense_names.len() != dense_values.len() {
        return Err(BorsukError::InvalidStorage(
            "mutation extent dense names and values differ in length".to_owned(),
        ));
    }
    for (name, vector) in dense_names.into_iter().zip(dense_values) {
        if name.is_empty() || record.extra_vectors.insert(name, vector).is_some() {
            return Err(BorsukError::InvalidStorage(
                "mutation extent has an empty or duplicate dense field name".to_owned(),
            ));
        }
    }

    let sparse_names = list_string_value(columns.sparse_names, row, "extra_sparse_names")?;
    let sparse_indices = nested_u32_value(columns.sparse_indices, row, "extra_sparse_indices")?;
    let sparse_values = nested_f32_value(columns.sparse_values, row, "extra_sparse_values")?;
    if sparse_names.len() != sparse_indices.len() || sparse_names.len() != sparse_values.len() {
        return Err(BorsukError::InvalidStorage(
            "mutation extent sparse names, indices, and values differ in length".to_owned(),
        ));
    }
    for ((name, indices), values) in sparse_names
        .into_iter()
        .zip(sparse_indices)
        .zip(sparse_values)
    {
        let vector = crate::SparseVector::new(indices, values).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "mutation extent sparse field `{name}` is invalid: {error}"
            ))
        })?;
        if name.is_empty() || record.extra_sparse.insert(name, vector).is_some() {
            return Err(BorsukError::InvalidStorage(
                "mutation extent has an empty or duplicate sparse field name".to_owned(),
            ));
        }
    }

    let late_names = list_string_value(columns.late_names, row, "late_interaction_names")?;
    let late_dimensions =
        list_u32_value(columns.late_dimensions, row, "late_interaction_dimensions")?;
    let late_element_types = list_string_value(
        columns.late_element_types,
        row,
        "late_interaction_element_types",
    )?;
    let late_values = nested_f32_value(columns.late_values, row, "late_interaction_values")?;
    if late_names.len() != late_dimensions.len()
        || late_names.len() != late_element_types.len()
        || late_names.len() != late_values.len()
    {
        return Err(BorsukError::InvalidStorage(
            "mutation extent late-interaction columns differ in length".to_owned(),
        ));
    }
    for (((name, dimensions), element_type), values) in late_names
        .into_iter()
        .zip(late_dimensions)
        .zip(late_element_types)
        .zip(late_values)
    {
        let dimensions = usize::try_from(dimensions).map_err(|_| {
            BorsukError::InvalidStorage("late-interaction dimensions exceed usize".to_owned())
        })?;
        let element_type = element_type.parse().map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "late-interaction element type `{element_type}` is invalid: {error}"
            ))
        })?;
        let vector = crate::LateInteractionVector::from_flat(dimensions, element_type, values)?;
        if name.is_empty() || record.extra_multi_vectors.insert(name, vector).is_some() {
            return Err(BorsukError::InvalidStorage(
                "mutation extent has an empty or duplicate late-interaction field name".to_owned(),
            ));
        }
    }
    Ok(())
}

fn list_string_value(array: &ListArray, row: usize, label: &str) -> Result<Vec<String>> {
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!(
            "put mutation is missing `{label}`"
        )));
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("`{label}` is not Utf8")))?;
    if values.null_count() != 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "`{label}` contains null values"
        )));
    }
    Ok(values.iter().flatten().map(ToOwned::to_owned).collect())
}

fn nested_f32_value(array: &ListArray, row: usize, label: &str) -> Result<Vec<Vec<f32>>> {
    nested_primitive_value::<Float32Array, f32>(array, row, label, |values| {
        values.values().to_vec()
    })
}

fn nested_u32_value(array: &ListArray, row: usize, label: &str) -> Result<Vec<Vec<u32>>> {
    nested_primitive_value::<UInt32Array, u32>(array, row, label, |values| values.values().to_vec())
}

fn nested_primitive_value<T, V>(
    array: &ListArray,
    row: usize,
    label: &str,
    copy: impl Fn(&T) -> Vec<V>,
) -> Result<Vec<Vec<V>>>
where
    T: Array + 'static,
{
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!(
            "put mutation is missing `{label}`"
        )));
    }
    let nested = array.value(row);
    let nested = nested
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("`{label}` is not a nested list")))?;
    let mut decoded = Vec::with_capacity(nested.len());
    for index in 0..nested.len() {
        if nested.is_null(index) {
            return Err(BorsukError::InvalidStorage(format!(
                "`{label}` contains a null child list"
            )));
        }
        let values = nested.value(index);
        let values = values.as_any().downcast_ref::<T>().ok_or_else(|| {
            BorsukError::InvalidStorage(format!("`{label}` has the wrong child type"))
        })?;
        if values.null_count() != 0 {
            return Err(BorsukError::InvalidStorage(format!(
                "`{label}` contains null primitive values"
            )));
        }
        decoded.push(copy(values));
    }
    Ok(decoded)
}

fn list_u32_value(array: &ListArray, row: usize, label: &str) -> Result<Vec<u32>> {
    if array.is_null(row) {
        return Err(BorsukError::InvalidStorage(format!(
            "put mutation is missing `{label}`"
        )));
    }
    let values = array.value(row);
    let values = values
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| BorsukError::InvalidStorage(format!("`{label}` is not UInt32")))?;
    if values.null_count() != 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "`{label}` contains null values"
        )));
    }
    Ok(values.values().to_vec())
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| BorsukError::InvalidStorage(format!("mutation extent is missing `{name}`")))?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!("mutation extent `{name}` has the wrong type"))
        })
}

fn validate_identity(identity: MutationExtentIdentity) -> Result<()> {
    if identity.stripe >= crate::lane_log::GROUP_COMMIT_STRIPE_COUNT
        || identity.lease_epoch == 0
        || identity.sequence == 0
    {
        return Err(BorsukError::InvalidRecordInput(
            "mutation extent identity is outside the persisted stripe/epoch/sequence bounds"
                .to_owned(),
        ));
    }
    Ok(())
}

fn required_metadata<'a>(schema: &'a Schema, key: &str) -> Result<&'a str> {
    schema
        .metadata()
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!("mutation extent metadata is missing `{key}`"))
        })
}

fn require_metadata(schema: &Schema, key: &str, expected: &str) -> Result<()> {
    let actual = required_metadata(schema, key)?;
    if actual != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "mutation extent metadata `{key}` is `{actual}`, expected `{expected}`"
        )));
    }
    Ok(())
}

fn parse_metadata<T>(schema: &Schema, key: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    required_metadata(schema, key)?.parse().map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "mutation extent metadata `{key}` is invalid: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow_array::{ArrayRef, FixedSizeBinaryArray, RecordBatch};
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
    use arrow_schema::DataType;

    use super::{
        MutationExtentIdState, MutationExtentIdentity, decode_mutation_extent,
        encode_mutation_extent,
    };
    use crate::mutation::{CanonicalMutation, MutationVersion};
    use crate::{MetaValue, SparseVector, VectorElementType, VectorRecord};

    fn put(version: MutationVersion) -> CanonicalMutation {
        let mut record = VectorRecord::new("entity", vec![0.25, -0.5]);
        record
            .extra_vectors
            .insert("dense".to_owned(), vec![1.0, 2.0]);
        record.extra_sparse.insert(
            "sparse".to_owned(),
            SparseVector::new(vec![2, 9], vec![0.5, 1.5]).unwrap(),
        );
        record.extra_multi_vectors.insert(
            "late".to_owned(),
            crate::LateInteractionVector::new(
                vec![vec![0.1, 0.2], vec![0.3, 0.4]],
                VectorElementType::Float32,
            )
            .unwrap(),
        );
        record.text = Some("hello world".to_owned());
        record.text_term_ids = vec![1, 7];
        record.text_term_freqs = vec![2, 1];
        record
            .metadata
            .insert("tenant".to_owned(), MetaValue::Str("a".to_owned()));
        CanonicalMutation::put(version, record).unwrap()
    }

    #[test]
    fn stock_arrow_reader_sees_typed_mutation_schema() {
        let identity = MutationExtentIdentity {
            stripe: 3,
            lease_epoch: 7,
            sequence: 11,
        };
        let mutations = vec![
            put(MutationVersion::from_parts(100, [1; 16])),
            CanonicalMutation::delete(
                MutationVersion::from_parts(101, [2; 16]),
                crate::RecordId::from("deleted"),
            ),
        ];
        let bytes = encode_mutation_extent(
            identity,
            2,
            &mutations,
            &[MutationExtentIdState::Live, MutationExtentIdState::Deleted],
        )
        .unwrap();
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        let schema = reader.schema();

        assert_eq!(
            schema
                .metadata()
                .get("borsuk.object_role")
                .map(String::as_str),
            Some("mutation_extent")
        );
        assert_eq!(
            schema.field_with_name("mutation_hlc").unwrap().data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            schema
                .field_with_name("mutation_writer")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            schema
                .field_with_name("mutation_digest")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(32)
        );
        assert!(schema.field_with_name("multimodal_json").is_err());
        assert!(matches!(
            schema
                .field_with_name("extra_dense_values")
                .unwrap()
                .data_type(),
            DataType::List(field) if matches!(field.data_type(), DataType::List(_))
        ));
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 2);
        assert!(reader.next().is_none());
    }

    #[test]
    fn standard_extent_round_trips_multimodal_put_and_delete() {
        let identity = MutationExtentIdentity {
            stripe: 63,
            lease_epoch: 99,
            sequence: 5,
        };
        let mutations = vec![
            put(MutationVersion::from_parts(50_000, [8; 16])),
            CanonicalMutation::delete(
                MutationVersion::from_parts(50_001, [9; 16]),
                crate::RecordId::from("gone"),
            ),
        ];

        let id_states = [
            MutationExtentIdState::Inserted,
            MutationExtentIdState::Deleted,
        ];
        let encoded = encode_mutation_extent(identity, 2, &mutations, &id_states).unwrap();
        let decoded = decode_mutation_extent(&encoded).unwrap();

        assert_eq!(decoded.identity, identity);
        assert_eq!(decoded.dimensions, 2);
        assert_eq!(decoded.max_version, mutations[1].stamp().version());
        assert_eq!(decoded.mutations, mutations);
        assert_eq!(decoded.id_states, id_states);
    }

    #[test]
    fn canonical_digest_corruption_fails_closed() {
        let identity = MutationExtentIdentity {
            stripe: 1,
            lease_epoch: 2,
            sequence: 3,
        };
        let encoded = encode_mutation_extent(
            identity,
            2,
            &[put(MutationVersion::from_parts(77, [3; 16]))],
            &[MutationExtentIdState::Live],
        )
        .unwrap();
        let mut reader = StreamReader::try_new(Cursor::new(encoded), None).unwrap();
        let schema = reader.schema();
        let batch = reader.next().unwrap().unwrap();
        let mut columns = batch.columns().to_vec();
        let digest_column = schema.index_of("mutation_digest").unwrap();
        columns[digest_column] =
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0_u8; 32]].into_iter()).unwrap())
                as ArrayRef;
        let corrupt = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new_with_options(&mut bytes, &schema, IpcWriteOptions::default())
                    .unwrap();
            writer.write(&corrupt).unwrap();
            writer.finish().unwrap();
        }

        assert!(decode_mutation_extent(&bytes).is_err());
    }
}
