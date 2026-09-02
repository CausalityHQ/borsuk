use std::{
    collections::BTreeSet,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::tree::build_v26_dual_tree_layout_with_workers;
use crate::{
    Result, V26ConstructionRow, V26LayoutAuthority, V26Node, V26ObjectIdentity, V26RowPages,
    V26Tree, canonical_json_value, exact_lower_hex, invalid, projected_steps,
    validate_v26_dual_tree_layout,
};

fn vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("element", DataType::Float32, false)),
        96,
    )
}

pub fn v26_construction_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("vector", vector_type(), false),
    ])
}

pub fn v26_source_map_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("dataset_ordinal", DataType::UInt64, false),
    ])
}

pub fn v26_tree_schema() -> Schema {
    Schema::new(vec![
        Field::new("node_ordinal", DataType::UInt32, false),
        Field::new("left", DataType::UInt32, true),
        Field::new("right", DataType::UInt32, true),
        Field::new("direction_ordinal", DataType::UInt8, false),
        Field::new("threshold", DataType::Float32, false),
        Field::new("split_gap", DataType::Float32, false),
        Field::new("leaf_page", DataType::UInt32, true),
    ])
}

pub fn v26_page_assignments_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26LocalObjectPath {
    pub identity: V26ObjectIdentity,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26LayoutBuildRequest {
    pub manifest: V26LocalObjectPath,
    pub construction_rows: V26LocalObjectPath,
    pub source_map: V26LocalObjectPath,
    pub output_dir: PathBuf,
    pub output_uri_prefix: String,
    pub worker_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutBuildOutput {
    pub authority: V26LayoutAuthority,
    pub inputs: Vec<V26ObjectIdentity>,
    pub outputs: Vec<V26ObjectIdentity>,
    pub row_count: u64,
    pub leaves_per_tree: u32,
    pub page_count: u32,
    pub projection_steps: u64,
    pub worker_count: u32,
}

fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut file = fs::File::open(path)
        .map_err(|error| invalid(&format!("V26 local object open failed: {error}")))?;
    let encoded_bytes = file
        .metadata()
        .map_err(|error| invalid(&format!("V26 local object metadata failed: {error}")))?
        .len();
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| invalid(&format!("V26 local object hash failed: {error}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((encoded_bytes, format!("{:x}", hasher.finalize())))
}

fn authenticate(object: &V26LocalObjectPath, role: &str) -> Result<()> {
    if object.identity.role != role
        || object.identity.digest_algorithm != "sha256"
        || !exact_lower_hex(&object.identity.digest, 64)
        || object.identity.encoded_bytes == 0
        || !object.identity.uri.starts_with("s3://")
    {
        return Err(invalid("V26 local object identity differs"));
    }
    let (encoded_bytes, digest) = sha256_file(&object.path)?;
    if encoded_bytes != object.identity.encoded_bytes || digest != object.identity.digest {
        return Err(invalid("V26 local object bytes differ"));
    }
    Ok(())
}

fn read_manifest(object: &V26LocalObjectPath) -> Result<V26LayoutAuthority> {
    authenticate(object, "layout-manifest")?;
    let bytes = fs::read(&object.path)
        .map_err(|error| invalid(&format!("V26 layout manifest read failed: {error}")))?;
    if bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n') {
        return Err(invalid("V26 layout manifest bytes differ"));
    }
    let authority: V26LayoutAuthority = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(&format!("V26 layout manifest parse failed: {error}")))?;
    let mut expected = serde_json::to_vec(&canonical_json_value(
        serde_json::to_value(&authority)
            .map_err(|error| invalid(&format!("V26 layout manifest failed: {error}")))?,
    ))
    .map_err(|error| invalid(&format!("V26 layout manifest failed: {error}")))?;
    expected.push(b'\n');
    if bytes != expected || object.identity.generation != authority.generation {
        return Err(invalid("V26 layout manifest authority differs"));
    }
    Ok(authority)
}

fn open_reader(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<fs::File>> {
    let file = fs::File::open(path)
        .map_err(|error| invalid(&format!("V26 Parquet open failed: {error}")))?;
    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| invalid(&format!("V26 Parquet metadata failed: {error}")))
}

fn read_inputs(
    request: &V26LayoutBuildRequest,
    authority: &V26LayoutAuthority,
) -> Result<Vec<V26ConstructionRow>> {
    authenticate(&request.construction_rows, "construction-parquet")?;
    authenticate(&request.source_map, "source-map-parquet")?;
    if request.construction_rows.identity.generation != authority.generation
        || request.source_map.identity.generation != authority.generation
    {
        return Err(invalid("V26 input generation differs"));
    }
    let expected_rows_i64 = i64::try_from(authority.expected_rows)
        .map_err(|_| invalid("V26 input row count overflows"))?;
    let expected_rows_usize = usize::try_from(authority.expected_rows)
        .map_err(|_| invalid("V26 input row count overflows"))?;
    let construction = open_reader(&request.construction_rows.path)?;
    let source = open_reader(&request.source_map.path)?;
    if construction.schema().as_ref() != &v26_construction_schema()
        || source.schema().as_ref() != &v26_source_map_schema()
        || construction.metadata().file_metadata().num_rows() != expected_rows_i64
        || source.metadata().file_metadata().num_rows() != expected_rows_i64
    {
        return Err(invalid("V26 input Parquet authority differs"));
    }
    let mut rows = Vec::with_capacity(expected_rows_usize);
    for batch in construction
        .build()
        .map_err(|error| invalid(&format!("V26 construction reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 construction batch failed: {error}")))?;
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 construction ordinal differs"))?;
        let vectors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V26 construction vector differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
            || vectors.values().null_count() != 0
        {
            return Err(invalid("V26 construction nullability differs"));
        }
        let flat = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V26 construction vector child differs"))?;
        let value_offset = vectors
            .offset()
            .checked_mul(96)
            .ok_or_else(|| invalid("V26 construction vector offset overflows"))?;
        for index in 0..batch.num_rows() {
            let source_ordinal = ordinals.value(index);
            if source_ordinal != rows.len() as u64 {
                return Err(invalid("V26 construction inventory differs"));
            }
            let start = value_offset
                .checked_add(
                    index
                        .checked_mul(96)
                        .ok_or_else(|| invalid("V26 construction vector offset overflows"))?,
                )
                .ok_or_else(|| invalid("V26 construction vector offset overflows"))?;
            let vector: [f32; 96] = flat.values()[start..start + 96].try_into().unwrap();
            let norm = vector.iter().map(|value| value * value).sum::<f32>();
            if vector.iter().any(|value| !value.is_finite())
                || !norm.is_finite()
                || (norm - 1.0).abs() > 1.0e-4
            {
                return Err(invalid("V26 construction vector authority differs"));
            }
            rows.push(V26ConstructionRow {
                source_ordinal,
                vector,
            });
        }
    }
    let mut observed_source = Vec::with_capacity(rows.len());
    let mut datasets = BTreeSet::new();
    for batch in source
        .build()
        .map_err(|error| invalid(&format!("V26 source-map reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 source-map batch failed: {error}")))?;
        let sources = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 source-map ordinal differs"))?;
        let dataset = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 dataset ordinal differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 source-map nullability differs"));
        }
        for index in 0..batch.num_rows() {
            let source_ordinal = sources.value(index);
            if source_ordinal != observed_source.len() as u64
                || !datasets.insert(dataset.value(index))
            {
                return Err(invalid("V26 source-map inventory differs"));
            }
            observed_source.push(source_ordinal);
        }
    }
    if rows.len() as u64 != authority.expected_rows || observed_source.len() != rows.len() {
        return Err(invalid("V26 input row count differs"));
    }
    Ok(rows)
}

fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build()
}

fn write_batch(path: &Path, batch: RecordBatch) -> Result<()> {
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| invalid(&format!("V26 output create failed: {error}")))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_properties()))
        .map_err(|error| invalid(&format!("V26 output writer failed: {error}")))?;
    writer
        .write(&batch)
        .map_err(|error| invalid(&format!("V26 output write failed: {error}")))?;
    writer
        .close()
        .map_err(|error| invalid(&format!("V26 output close failed: {error}")))?;
    fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| invalid(&format!("V26 output sync failed: {error}")))
}

fn tree_batch(tree: &V26Tree) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(v26_tree_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                tree.nodes.iter().map(|node| node.node_ordinal),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(
                tree.nodes.iter().map(|node| node.left).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                tree.nodes.iter().map(|node| node.right).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from_iter_values(
                tree.nodes.iter().map(|node| node.direction_ordinal),
            )),
            Arc::new(Float32Array::from_iter_values(
                tree.nodes.iter().map(|node| node.threshold),
            )),
            Arc::new(Float32Array::from_iter_values(
                tree.nodes.iter().map(|node| node.split_gap),
            )),
            Arc::new(UInt32Array::from(
                tree.nodes
                    .iter()
                    .map(|node| node.leaf_page)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 tree batch failed: {error}")))
}

fn assignments_batch(rows: &[V26RowPages]) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(v26_page_assignments_schema()),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.source_ordinal),
            )) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.primary_page),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.replica_page),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V26 assignment batch failed: {error}")))
}

fn output_identity(
    role: &str,
    path: &Path,
    prefix: &str,
    generation: &str,
) -> Result<V26ObjectIdentity> {
    let (encoded_bytes, digest) = sha256_file(path)?;
    Ok(V26ObjectIdentity {
        role: role.to_owned(),
        uri: format!("{prefix}{}", path.file_name().unwrap().to_string_lossy()),
        digest_algorithm: "sha256".to_owned(),
        digest,
        encoded_bytes,
        generation: generation.to_owned(),
    })
}

fn validate_uri_inventory(request: &V26LayoutBuildRequest) -> Result<()> {
    let mut uris = BTreeSet::new();
    for uri in [
        request.manifest.identity.uri.clone(),
        request.construction_rows.identity.uri.clone(),
        request.source_map.identity.uri.clone(),
        format!("{}page-assignments.parquet", request.output_uri_prefix),
        format!("{}primary-tree.parquet", request.output_uri_prefix),
        format!("{}replica-tree.parquet", request.output_uri_prefix),
    ] {
        if !uris.insert(uri) {
            return Err(invalid("V26 object URI roles overlap"));
        }
    }
    Ok(())
}

pub fn run_v26_layout_build(request: &V26LayoutBuildRequest) -> Result<V26LayoutBuildOutput> {
    if request.worker_count == 0
        || !request.output_uri_prefix.starts_with("s3://")
        || !request.output_uri_prefix.ends_with('/')
        || request.output_dir.exists()
    {
        return Err(invalid("V26 layout build request differs"));
    }
    validate_uri_inventory(request)?;
    let authority = read_manifest(&request.manifest)?;
    let rows = read_inputs(request, &authority)?;
    let (primary, replica, assignments) =
        build_v26_dual_tree_layout_with_workers(&authority, &rows, request.worker_count)?;
    fs::create_dir(&request.output_dir)
        .map_err(|error| invalid(&format!("V26 output directory failed: {error}")))?;
    let result = (|| -> Result<V26LayoutBuildOutput> {
        write_batch(
            &request.output_dir.join("primary-tree.parquet"),
            tree_batch(&primary)?,
        )?;
        write_batch(
            &request.output_dir.join("replica-tree.parquet"),
            tree_batch(&replica)?,
        )?;
        write_batch(
            &request.output_dir.join("page-assignments.parquet"),
            assignments_batch(&assignments)?,
        )?;
        let outputs = vec![
            output_identity(
                "page-assignments-parquet",
                &request.output_dir.join("page-assignments.parquet"),
                &request.output_uri_prefix,
                &authority.generation,
            )?,
            output_identity(
                "primary-tree-parquet",
                &request.output_dir.join("primary-tree.parquet"),
                &request.output_uri_prefix,
                &authority.generation,
            )?,
            output_identity(
                "replica-tree-parquet",
                &request.output_dir.join("replica-tree.parquet"),
                &request.output_uri_prefix,
                &authority.generation,
            )?,
        ];
        let leaves = authority
            .expected_rows
            .div_ceil(u64::from(authority.page_capacity));
        let output = V26LayoutBuildOutput {
            authority: authority.clone(),
            inputs: vec![
                request.construction_rows.identity.clone(),
                request.manifest.identity.clone(),
                request.source_map.identity.clone(),
            ],
            outputs,
            row_count: authority.expected_rows,
            leaves_per_tree: u32::try_from(leaves)
                .map_err(|_| invalid("V26 leaf count overflows"))?,
            page_count: u32::try_from(
                leaves
                    .checked_mul(2)
                    .ok_or_else(|| invalid("V26 page count overflows"))?,
            )
            .map_err(|_| invalid("V26 page count overflows"))?,
            projection_steps: projected_steps(authority.expected_rows, leaves)?
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 projection work overflows"))?,
            worker_count: u32::try_from(request.worker_count)
                .map_err(|_| invalid("V26 worker count overflows"))?,
        };
        validate_v26_layout_build_output(request, &output)?;
        Ok(output)
    })();
    if result.is_err() {
        let _ = fs::remove_file(request.output_dir.join("primary-tree.parquet"));
        let _ = fs::remove_file(request.output_dir.join("replica-tree.parquet"));
        let _ = fs::remove_file(request.output_dir.join("page-assignments.parquet"));
        let _ = fs::remove_dir(&request.output_dir);
    }
    result
}

fn read_tree(path: &Path, expected_rows: i64, seed: u64) -> Result<V26Tree> {
    let reader = open_reader(path)?;
    if reader.schema().as_ref() != &v26_tree_schema()
        || reader.metadata().file_metadata().num_rows() != expected_rows
    {
        return Err(invalid("V26 tree Parquet authority differs"));
    }
    let mut nodes = Vec::new();
    for batch in reader
        .build()
        .map_err(|error| invalid(&format!("V26 tree reader failed: {error}")))?
    {
        let batch = batch.map_err(|error| invalid(&format!("V26 tree batch failed: {error}")))?;
        let u32s = |column| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid("V26 tree u32 differs"))
        };
        let ordinals = u32s(0)?;
        let left = u32s(1)?;
        let right = u32s(2)?;
        let directions = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("V26 tree direction differs"))?;
        let thresholds = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V26 tree threshold differs"))?;
        let gaps = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V26 tree gap differs"))?;
        let pages = u32s(6)?;
        for row in 0..batch.num_rows() {
            nodes.push(V26Node {
                node_ordinal: ordinals.value(row),
                left: (!left.is_null(row)).then(|| left.value(row)),
                right: (!right.is_null(row)).then(|| right.value(row)),
                direction_ordinal: directions.value(row),
                threshold: thresholds.value(row),
                split_gap: gaps.value(row),
                leaf_page: (!pages.is_null(row)).then(|| pages.value(row)),
            });
        }
    }
    Ok(V26Tree {
        seed,
        root: 0,
        nodes,
    })
}

fn read_assignments(path: &Path, expected_rows: i64) -> Result<Vec<V26RowPages>> {
    let reader = open_reader(path)?;
    if reader.schema().as_ref() != &v26_page_assignments_schema()
        || reader.metadata().file_metadata().num_rows() != expected_rows
    {
        return Err(invalid("V26 assignment Parquet authority differs"));
    }
    let mut rows = Vec::new();
    for batch in reader
        .build()
        .map_err(|error| invalid(&format!("V26 assignment reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V26 assignment batch failed: {error}")))?;
        let sources = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V26 assignment ordinal differs"))?;
        let primary = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V26 primary page differs"))?;
        let replica = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V26 replica page differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V26 assignment nullability differs"));
        }
        for row in 0..batch.num_rows() {
            rows.push(V26RowPages {
                source_ordinal: sources.value(row),
                primary_page: primary.value(row),
                replica_page: replica.value(row),
            });
        }
    }
    Ok(rows)
}

pub fn validate_v26_layout_build_output(
    request: &V26LayoutBuildRequest,
    output: &V26LayoutBuildOutput,
) -> Result<()> {
    validate_uri_inventory(request)?;
    let observed_authority = read_manifest(&request.manifest)?;
    authenticate(&request.construction_rows, "construction-parquet")?;
    authenticate(&request.source_map, "source-map-parquet")?;
    if output.authority != observed_authority
        || output.authority.generation != request.manifest.identity.generation
        || output.inputs
            != vec![
                request.construction_rows.identity.clone(),
                request.manifest.identity.clone(),
                request.source_map.identity.clone(),
            ]
        || output.row_count != output.authority.expected_rows
        || output.worker_count as usize != request.worker_count
        || output.outputs.len() != 3
    {
        return Err(invalid("V26 layout build output differs"));
    }
    for (identity, role, name) in [
        (
            &output.outputs[0],
            "page-assignments-parquet",
            "page-assignments.parquet",
        ),
        (
            &output.outputs[1],
            "primary-tree-parquet",
            "primary-tree.parquet",
        ),
        (
            &output.outputs[2],
            "replica-tree-parquet",
            "replica-tree.parquet",
        ),
    ] {
        let observed = V26LocalObjectPath {
            identity: identity.clone(),
            path: request.output_dir.join(name),
        };
        authenticate(&observed, role)?;
        if identity.generation != output.authority.generation
            || identity.uri != format!("{}{name}", request.output_uri_prefix)
        {
            return Err(invalid("V26 output identity differs"));
        }
    }
    let node_count = i64::from(output.leaves_per_tree) * 2 - 1;
    let primary = read_tree(
        &request.output_dir.join("primary-tree.parquet"),
        node_count,
        output.authority.primary_seed,
    )?;
    let replica = read_tree(
        &request.output_dir.join("replica-tree.parquet"),
        node_count,
        output.authority.replica_seed,
    )?;
    let assignment_rows = i64::try_from(output.row_count)
        .map_err(|_| invalid("V26 assignment row count overflows"))?;
    let assignments = read_assignments(
        &request.output_dir.join("page-assignments.parquet"),
        assignment_rows,
    )?;
    validate_v26_dual_tree_layout(&output.authority, &primary, &replica, &assignments)?;
    let leaves = output
        .row_count
        .div_ceil(u64::from(output.authority.page_capacity));
    if output.leaves_per_tree as u64 != leaves
        || output.page_count as u64 != leaves * 2
        || output.projection_steps
            != projected_steps(output.row_count, leaves)?
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 projection work overflows"))?
    {
        return Err(invalid("V26 layout build counts differ"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write, sync::Arc};

    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field};
    use parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        basic::Compression,
        file::properties::{WriterProperties, WriterVersion},
    };
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use super::{
        V26LayoutBuildRequest, V26LocalObjectPath, assignments_batch, output_identity,
        read_assignments, run_v26_layout_build, v26_construction_schema,
        v26_page_assignments_schema, v26_source_map_schema, v26_tree_schema,
        validate_v26_layout_build_output,
    };
    use crate::{V26LayoutAuthority, V26ObjectIdentity, canonical_json_value};

    fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn identity(role: &str, path: &std::path::Path) -> V26LocalObjectPath {
        let bytes = fs::read(path).unwrap();
        V26LocalObjectPath {
            identity: V26ObjectIdentity {
                role: role.to_owned(),
                uri: format!("s3://v26-input/{role}"),
                digest_algorithm: "sha256".to_owned(),
                digest: format!("{:x}", Sha256::digest(&bytes)),
                encoded_bytes: bytes.len() as u64,
                generation: "v26-local-test".to_owned(),
            },
            path: path.to_owned(),
        }
    }

    fn fixture() -> (
        TempDir,
        V26LocalObjectPath,
        V26LocalObjectPath,
        V26LocalObjectPath,
    ) {
        let temp = TempDir::new().unwrap();
        let authority = V26LayoutAuthority {
            schema: "borsuk-v26-dual-tree-layout-v1".to_owned(),
            generation: "v26-local-test".to_owned(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            primary_seed: 0x5632_362d_5452_4545,
            replica_seed: 0x5632_362d_5245_504c,
            page_capacity: 704,
            expected_rows: 1_409,
        };
        let manifest_path = temp.path().join("manifest.json");
        let mut manifest_bytes = serde_json::to_vec(&canonical_json_value(
            serde_json::to_value(&authority).unwrap(),
        ))
        .unwrap();
        manifest_bytes.push(b'\n');
        fs::write(&manifest_path, manifest_bytes).unwrap();

        let ordinals = UInt64Array::from_iter_values(0..authority.expected_rows);
        let mut flat = Vec::with_capacity(authority.expected_rows as usize * 96);
        for ordinal in 0..authority.expected_rows as usize {
            for dimension in 0..96 {
                flat.push(if dimension == ordinal % 96 { 1.0 } else { 0.0 });
            }
        }
        let vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
            Arc::new(Float32Array::from(flat)),
            None,
        )
        .unwrap();
        let construction = RecordBatch::try_new(
            Arc::new(v26_construction_schema()),
            vec![Arc::new(ordinals.clone()) as ArrayRef, Arc::new(vectors)],
        )
        .unwrap();
        let construction_path = temp.path().join("construction.parquet");
        write_parquet(&construction_path, &construction);

        let source_map = RecordBatch::try_new(
            Arc::new(v26_source_map_schema()),
            vec![
                Arc::new(ordinals) as ArrayRef,
                Arc::new(UInt64Array::from_iter_values(
                    10_000..10_000 + authority.expected_rows,
                )),
            ],
        )
        .unwrap();
        let source_map_path = temp.path().join("source-map.parquet");
        write_parquet(&source_map_path, &source_map);
        (
            temp,
            identity("layout-manifest", &manifest_path),
            identity("construction-parquet", &construction_path),
            identity("source-map-parquet", &source_map_path),
        )
    }

    fn request(
        manifest: V26LocalObjectPath,
        construction_rows: V26LocalObjectPath,
        source_map: V26LocalObjectPath,
        output_dir: std::path::PathBuf,
        worker_count: usize,
    ) -> V26LayoutBuildRequest {
        V26LayoutBuildRequest {
            manifest,
            construction_rows,
            source_map,
            output_dir,
            output_uri_prefix: "s3://v26-output/layout-a/".to_owned(),
            worker_count,
        }
    }

    #[test]
    fn v26_layout_local_authenticates_construction_only_and_emits_parquet() {
        // Break caught: parsing before authentication or emitting a nondeterministic layout.
        let (temp, manifest, construction, source_map) = fixture();
        let first_dir = temp.path().join("out-one");
        let second_dir = temp.path().join("out-four");
        let first = run_v26_layout_build(&request(
            manifest.clone(),
            construction.clone(),
            source_map.clone(),
            first_dir.clone(),
            1,
        ))
        .unwrap();
        let second_request = request(manifest, construction, source_map, second_dir.clone(), 4);
        let second = run_v26_layout_build(&second_request).unwrap();
        assert_eq!(first.row_count, 1_409);
        assert_eq!(first.leaves_per_tree, 3);
        assert_eq!(first.page_count, 6);
        assert_eq!(first.projection_steps, 6_494_208);
        assert_eq!(first.outputs, second.outputs);
        validate_v26_layout_build_output(&second_request, &second).unwrap();

        for (name, schema, rows) in [
            (
                "page-assignments.parquet",
                v26_page_assignments_schema(),
                1_409,
            ),
            ("primary-tree.parquet", v26_tree_schema(), 5),
            ("replica-tree.parquet", v26_tree_schema(), 5),
        ] {
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(second_dir.join(name)).unwrap(),
            )
            .unwrap();
            assert_eq!(reader.schema().as_ref(), &schema);
            assert_eq!(reader.metadata().file_metadata().num_rows(), rows);
            assert_eq!(
                fs::read(first_dir.join(name)).unwrap(),
                fs::read(second_dir.join(name)).unwrap()
            );
        }
    }

    #[test]
    fn v26_layout_local_rejects_query_truth_and_result_roles() {
        // Break caught: construction gains a query/evaluation capability.
        for forbidden in ["pseudoqueries-parquet", "truth-parquet", "prior-result"] {
            let (temp, manifest, mut construction, source_map) = fixture();
            construction.identity.role = forbidden.to_owned();
            let output = temp.path().join("forbidden-output");
            assert!(
                run_v26_layout_build(&request(
                    manifest,
                    construction,
                    source_map,
                    output.clone(),
                    1
                ))
                .is_err()
            );
            assert!(!output.exists());
        }
    }

    #[test]
    fn v26_layout_local_rejects_input_output_uri_role_overlap() {
        // Break caught: one immutable URI is assigned both an input and output role.
        let (temp, manifest, mut construction, source_map) = fixture();
        construction.identity.uri = "s3://v26-output/layout-a/page-assignments.parquet".to_owned();
        let output_dir = temp.path().join("overlap-output");
        assert!(
            run_v26_layout_build(&request(
                manifest,
                construction,
                source_map,
                output_dir.clone(),
                1,
            ))
            .is_err()
        );
        assert!(!output_dir.exists());
    }

    #[test]
    fn v26_layout_local_rejects_output_schema_topology_and_identity_drift() {
        // Break caught: a validated output is modified before receipt sealing.
        let (temp, manifest, construction, source_map) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, source_map, output_dir.clone(), 1);
        let output = run_v26_layout_build(&request).unwrap();
        let assignments = output_dir.join("page-assignments.parquet");
        fs::OpenOptions::new()
            .append(true)
            .open(assignments)
            .unwrap()
            .write_all(b"drift")
            .unwrap();
        assert!(validate_v26_layout_build_output(&request, &output).is_err());
    }

    #[test]
    fn v26_layout_local_reauthenticates_inputs_before_sealing_output() {
        // Break caught: an authenticated construction input changes while the layout is built.
        let (temp, manifest, construction, source_map) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, source_map, output_dir, 1);
        let output = run_v26_layout_build(&request).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&request.construction_rows.path)
            .unwrap()
            .write_all(b"drift")
            .unwrap();
        assert!(validate_v26_layout_build_output(&request, &output).is_err());
    }

    #[test]
    fn v26_layout_local_rejects_rehashed_semantic_parquet_drift() {
        // Break caught: byte authority is refreshed around a duplicate assignment ordinal.
        let (temp, manifest, construction, source_map) = fixture();
        let output_dir = temp.path().join("outputs");
        let request = request(manifest, construction, source_map, output_dir.clone(), 1);
        let mut output = run_v26_layout_build(&request).unwrap();
        let assignment_path = output_dir.join("page-assignments.parquet");
        let mut assignments = read_assignments(&assignment_path, 1_409).unwrap();
        assignments[1].source_ordinal = assignments[0].source_ordinal;
        fs::remove_file(&assignment_path).unwrap();
        write_parquet(&assignment_path, &assignments_batch(&assignments).unwrap());
        output.outputs[0] = output_identity(
            "page-assignments-parquet",
            &assignment_path,
            &request.output_uri_prefix,
            &output.authority.generation,
        )
        .unwrap();
        assert!(validate_v26_layout_build_output(&request, &output).is_err());
    }
}
