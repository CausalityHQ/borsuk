use std::{io::Cursor, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    RecordBatch, UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v23_rabitq::{V23_RABITQ_MIN_ALIGNMENT, V23RaBitQObjectIdentity},
};

const DIMENSIONS: i32 = 96;
const SIGN_CODE_BYTES: i32 = 12;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23RaBitQRowPlanes {
    pub(crate) sign_codes: Vec<[u8; 12]>,
    pub(crate) residual_norms: Vec<f32>,
    pub(crate) alignments: Vec<f32>,
    pub(crate) primary_pages: Vec<u32>,
    pub(crate) replica_pages: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23RaBitQGeometry {
    pub(crate) leaf_offsets: Vec<u64>,
    pub(crate) centroids: Vec<[f16; 96]>,
    pub(crate) rotation: [[f32; 96]; 96],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23RaBitQGeometryBytes {
    pub(crate) leaf_offsets: Vec<u8>,
    pub(crate) centroids: Vec<u8>,
    pub(crate) rotation: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23RaBitQGeometryIdentities {
    pub(crate) leaf_offsets: V23RaBitQObjectIdentity,
    pub(crate) centroids: V23RaBitQObjectIdentity,
    pub(crate) rotation: V23RaBitQObjectIdentity,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn row_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "sign_code",
            DataType::FixedSizeBinary(SIGN_CODE_BYTES),
            false,
        ),
        Field::new("residual_norm", DataType::Float32, false),
        Field::new("alignment", DataType::Float32, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

fn offset_schema() -> Schema {
    Schema::new(vec![Field::new("leaf_offset", DataType::UInt64, false)])
}

fn fixed_list_schema(name: &str, element: DataType) -> Schema {
    Schema::new(vec![Field::new(
        name,
        DataType::FixedSizeList(Arc::new(Field::new("element", element, false)), DIMENSIONS),
        false,
    )])
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    {
        let mut writer =
            FileWriter::try_new_with_options(&mut bytes, batch.schema().as_ref(), options)?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}

fn read_one_batch(bytes: &[u8], schema: &Schema) -> Result<RecordBatch> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != schema {
        return Err(invalid("V23 RaBitQ Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V23 RaBitQ Arrow batch is missing"))??;
    if reader.next().is_some() || batch.num_columns() != schema.fields().len() {
        return Err(invalid("V23 RaBitQ Arrow batch count differs"));
    }
    if batch
        .columns()
        .iter()
        .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V23 RaBitQ Arrow nullability differs"));
    }
    Ok(batch)
}

fn authenticate_bytes(bytes: &[u8], identity: &V23RaBitQObjectIdentity, role: &str) -> Result<()> {
    if identity.role != role
        || identity.blake3.is_some()
        || identity.encoded_bytes != bytes.len() as u64
        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid("V23 RaBitQ Arrow byte authority differs"));
    }
    Ok(())
}

fn validate_row_planes(value: &V23RaBitQRowPlanes) -> Result<()> {
    let rows = value.sign_codes.len();
    if rows == 0
        || value.residual_norms.len() != rows
        || value.alignments.len() != rows
        || value.primary_pages.len() != rows
        || value.replica_pages.len() != rows
    {
        return Err(invalid("V23 RaBitQ row-plane cardinality differs"));
    }
    for ordinal in 0..rows {
        let norm = value.residual_norms[ordinal];
        let alignment = value.alignments[ordinal];
        let primary = value.primary_pages[ordinal];
        let replica = value.replica_pages[ordinal];
        if !norm.is_finite()
            || norm < 0.0
            || !alignment.is_finite()
            || !(V23_RABITQ_MIN_ALIGNMENT - 1.0e-6..=1.0).contains(&alignment)
            || primary == u32::MAX
            || (replica != u32::MAX && replica == primary)
            || (norm == 0.0
                && (value.sign_codes[ordinal] != [0; 12]
                    || alignment.to_bits() != 1.0f32.to_bits()))
        {
            return Err(invalid("V23 RaBitQ row-plane value differs"));
        }
    }
    Ok(())
}

pub(crate) fn encode_v23_rabitq_row_planes(value: &V23RaBitQRowPlanes) -> Result<Vec<u8>> {
    validate_row_planes(value)?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            value.sign_codes.iter().map(<[_; 12]>::as_slice),
        )?),
        Arc::new(Float32Array::from(value.residual_norms.clone())),
        Arc::new(Float32Array::from(value.alignments.clone())),
        Arc::new(UInt32Array::from(value.primary_pages.clone())),
        Arc::new(UInt32Array::from(value.replica_pages.clone())),
    ];
    encode_batch(&RecordBatch::try_new(Arc::new(row_schema()), columns)?)
}

pub(crate) fn read_v23_rabitq_row_planes(
    bytes: &[u8],
    identity: &V23RaBitQObjectIdentity,
) -> Result<V23RaBitQRowPlanes> {
    authenticate_bytes(bytes, identity, "row-codes")?;
    let schema = row_schema();
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &schema {
        return Err(invalid("V23 RaBitQ Arrow schema differs"));
    }
    let mut value = V23RaBitQRowPlanes {
        sign_codes: Vec::new(),
        residual_norms: Vec::new(),
        alignments: Vec::new(),
        primary_pages: Vec::new(),
        replica_pages: Vec::new(),
    };
    for batch in &mut reader {
        let batch = batch?;
        if batch.num_rows() == 0
            || batch.num_columns() != schema.fields().len()
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V23 RaBitQ Arrow row batch differs"));
        }
        let codes = batch.columns()[0]
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid("V23 RaBitQ sign-code column differs"))?;
        let residual_norms = batch.columns()[1]
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V23 RaBitQ residual-norm column differs"))?;
        let alignments = batch.columns()[2]
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V23 RaBitQ alignment column differs"))?;
        let primary_pages = batch.columns()[3]
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V23 RaBitQ primary-page column differs"))?;
        let replica_pages = batch.columns()[4]
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V23 RaBitQ replica-page column differs"))?;
        for ordinal in 0..batch.num_rows() {
            value.sign_codes.push(
                codes
                    .value(ordinal)
                    .try_into()
                    .map_err(|_| invalid("V23 RaBitQ sign-code width differs"))?,
            );
        }
        value
            .residual_norms
            .extend_from_slice(residual_norms.values());
        value.alignments.extend_from_slice(alignments.values());
        value
            .primary_pages
            .extend_from_slice(primary_pages.values());
        value
            .replica_pages
            .extend_from_slice(replica_pages.values());
    }
    validate_row_planes(&value)?;
    Ok(value)
}

fn validate_geometry(value: &V23RaBitQGeometry, expected_rows: Option<u64>) -> Result<()> {
    if value.leaf_offsets.len() < 2
        || value.centroids.len() + 1 != value.leaf_offsets.len()
        || value.leaf_offsets[0] != 0
        || value.leaf_offsets.windows(2).any(|pair| pair[0] > pair[1])
        || expected_rows.is_some_and(|rows| value.leaf_offsets.last().copied() != Some(rows))
        || value
            .centroids
            .iter()
            .flatten()
            .any(|component| !component.is_finite())
        || value
            .rotation
            .iter()
            .flatten()
            .any(|value| !value.is_finite())
    {
        return Err(invalid("V23 RaBitQ geometry authority differs"));
    }
    for left in 0..96 {
        for right in left..96 {
            let dot = (0..96)
                .map(|dimension| {
                    f64::from(value.rotation[left][dimension])
                        * f64::from(value.rotation[right][dimension])
                })
                .sum::<f64>();
            let expected = if left == right { 1.0 } else { 0.0 };
            if (dot - expected).abs() > 1.0e-4 {
                return Err(invalid("V23 RaBitQ rotation orthogonality differs"));
            }
        }
    }
    Ok(())
}

fn encode_fixed_f16(rows: &[[f16; 96]]) -> Result<Vec<u8>> {
    let child = Arc::new(Field::new("element", DataType::Float16, false));
    let values = Arc::new(Float16Array::from_iter_values(
        rows.iter().flatten().copied(),
    ));
    let array = FixedSizeListArray::try_new(child, DIMENSIONS, values, None)?;
    encode_batch(&RecordBatch::try_new(
        Arc::new(fixed_list_schema("centroid", DataType::Float16)),
        vec![Arc::new(array)],
    )?)
}

fn encode_fixed_f32(rows: &[[f32; 96]], name: &str) -> Result<Vec<u8>> {
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    let values = Arc::new(Float32Array::from_iter_values(
        rows.iter().flatten().copied(),
    ));
    let array = FixedSizeListArray::try_new(child, DIMENSIONS, values, None)?;
    encode_batch(&RecordBatch::try_new(
        Arc::new(fixed_list_schema(name, DataType::Float32)),
        vec![Arc::new(array)],
    )?)
}

pub(crate) fn encode_v23_rabitq_geometry(
    value: &V23RaBitQGeometry,
) -> Result<V23RaBitQGeometryBytes> {
    validate_geometry(value, value.leaf_offsets.last().copied())?;
    let offsets = RecordBatch::try_new(
        Arc::new(offset_schema()),
        vec![Arc::new(UInt64Array::from(value.leaf_offsets.clone()))],
    )?;
    Ok(V23RaBitQGeometryBytes {
        leaf_offsets: encode_batch(&offsets)?,
        centroids: encode_fixed_f16(&value.centroids)?,
        rotation: encode_fixed_f32(&value.rotation, "rotation")?,
    })
}

fn read_offsets(bytes: &[u8]) -> Result<Vec<u64>> {
    let batch = read_one_batch(bytes, &offset_schema())?;
    let values = batch.columns()[0]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V23 RaBitQ leaf-offset column differs"))?;
    Ok(values.values().to_vec())
}

fn read_f16_rows(bytes: &[u8]) -> Result<Vec<[f16; 96]>> {
    let batch = read_one_batch(bytes, &fixed_list_schema("centroid", DataType::Float16))?;
    let values = batch.columns()[0]
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V23 RaBitQ centroid column differs"))?
        .values();
    let values = values
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| invalid("V23 RaBitQ centroid child differs"))?;
    values
        .values()
        .chunks_exact(96)
        .map(|row| {
            row.try_into()
                .map_err(|_| invalid("V23 RaBitQ centroid width differs"))
        })
        .collect()
}

fn read_f32_rows(bytes: &[u8], name: &str) -> Result<Vec<[f32; 96]>> {
    let batch = read_one_batch(bytes, &fixed_list_schema(name, DataType::Float32))?;
    let values = batch.columns()[0]
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V23 RaBitQ rotation column differs"))?
        .values();
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V23 RaBitQ rotation child differs"))?;
    values
        .values()
        .chunks_exact(96)
        .map(|row| {
            row.try_into()
                .map_err(|_| invalid("V23 RaBitQ rotation width differs"))
        })
        .collect()
}

pub(crate) fn read_v23_rabitq_geometry(
    bytes: &V23RaBitQGeometryBytes,
    identities: &V23RaBitQGeometryIdentities,
    expected_rows: u64,
) -> Result<V23RaBitQGeometry> {
    authenticate_bytes(
        &bytes.leaf_offsets,
        &identities.leaf_offsets,
        "leaf-offsets",
    )?;
    authenticate_bytes(&bytes.centroids, &identities.centroids, "centroids")?;
    authenticate_bytes(&bytes.rotation, &identities.rotation, "rotation")?;
    let rotation = read_f32_rows(&bytes.rotation, "rotation")?;
    let value = V23RaBitQGeometry {
        leaf_offsets: read_offsets(&bytes.leaf_offsets)?,
        centroids: read_f16_rows(&bytes.centroids)?,
        rotation: rotation
            .try_into()
            .map_err(|_| invalid("V23 RaBitQ rotation row count differs"))?,
    };
    validate_geometry(&value, Some(expected_rows))?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow_array::{
        ArrayRef, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, RecordBatch,
    };
    use arrow_ipc::{
        MetadataVersion,
        reader::FileReader,
        writer::{FileWriter, IpcWriteOptions},
    };
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use sha2::{Digest, Sha256};

    use super::{
        V23RaBitQGeometry, V23RaBitQGeometryIdentities, V23RaBitQRowPlanes,
        encode_v23_rabitq_geometry, encode_v23_rabitq_row_planes, read_v23_rabitq_geometry,
        read_v23_rabitq_row_planes,
    };
    use crate::v23_rabitq::V23RaBitQObjectIdentity;

    fn identity(role: &str, bytes: &[u8]) -> V23RaBitQObjectIdentity {
        V23RaBitQObjectIdentity {
            role: role.to_string(),
            uri: format!("s3://borsuk-v23-rabitq/{role}"),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            blake3: None,
            encoded_bytes: bytes.len() as u64,
        }
    }

    fn row_planes() -> V23RaBitQRowPlanes {
        V23RaBitQRowPlanes {
            sign_codes: vec![[0; 12], [0x55; 12], [0xaa; 12]],
            residual_norms: vec![0.0, 1.5, 2.5],
            alignments: vec![1.0, 0.75, 0.5],
            primary_pages: vec![0, 1, 2],
            replica_pages: vec![u32::MAX, 2, 0],
        }
    }

    fn geometry() -> V23RaBitQGeometry {
        let mut rotation = [[0.0; 96]; 96];
        for (ordinal, row) in rotation.iter_mut().enumerate() {
            row[ordinal] = 1.0;
        }
        V23RaBitQGeometry {
            leaf_offsets: vec![0, 1, 1, 3],
            centroids: vec![[f16::from_f32(0.0); 96]; 3],
            rotation,
        }
    }

    fn rewrite_schema(bytes: &[u8], fields: Vec<Field>) -> Vec<u8> {
        let mut reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        let columns = batch.columns().iter().cloned().collect::<Vec<ArrayRef>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        let mut output = Vec::new();
        let mut writer =
            FileWriter::try_new_with_options(&mut output, batch.schema().as_ref(), options)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        output
    }

    fn rewrite_columns(
        bytes: &[u8],
        fields: Vec<Field>,
        order: &[usize],
        copies: usize,
    ) -> Vec<u8> {
        let mut reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
        let original = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        let columns = order
            .iter()
            .map(|&ordinal| Arc::clone(&original.columns()[ordinal]))
            .collect::<Vec<ArrayRef>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        let mut output = Vec::new();
        let mut writer =
            FileWriter::try_new_with_options(&mut output, batch.schema().as_ref(), options)
                .unwrap();
        for _ in 0..copies {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        drop(writer);
        output
    }

    #[test]
    fn v23_rabitq_arrow_row_planes_roundtrip_and_byte_authority() {
        let expected = row_planes();
        let bytes = encode_v23_rabitq_row_planes(&expected).unwrap();
        let id = identity("row-codes", &bytes);
        assert_eq!(read_v23_rabitq_row_planes(&bytes, &id).unwrap(), expected);

        let mut wrong = id.clone();
        wrong.sha256 = "0".repeat(64);
        assert!(read_v23_rabitq_row_planes(&bytes, &wrong).is_err());
        let mut corrupted = bytes.clone();
        let index = corrupted.len() / 2;
        corrupted[index] ^= 1;
        assert!(read_v23_rabitq_row_planes(&corrupted, &id).is_err());
    }

    #[test]
    fn v23_rabitq_arrow_row_planes_reject_schema_and_value_drift() {
        let expected = row_planes();
        let bytes = encode_v23_rabitq_row_planes(&expected).unwrap();
        let reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
        let fields = reader
            .schema()
            .fields()
            .iter()
            .map(|v| v.as_ref().clone())
            .collect::<Vec<_>>();

        let mut renamed = fields.clone();
        renamed[0] = Field::new(
            "sign_codes",
            renamed[0].data_type().clone(),
            renamed[0].is_nullable(),
        );
        let mutation = rewrite_schema(&bytes, renamed);
        assert!(read_v23_rabitq_row_planes(&mutation, &identity("row-codes", &mutation)).is_err());

        let mut nullable = fields.clone();
        nullable[1] = Field::new(nullable[1].name(), nullable[1].data_type().clone(), true);
        let mutation = rewrite_schema(&bytes, nullable);
        assert!(read_v23_rabitq_row_planes(&mutation, &identity("row-codes", &mutation)).is_err());

        let mut extra = fields.clone();
        extra.push(fields[1].clone().with_name("extra"));
        let mutation = rewrite_columns(&bytes, extra, &[0, 1, 2, 3, 4, 1], 1);
        assert!(read_v23_rabitq_row_planes(&mutation, &identity("row-codes", &mutation)).is_err());

        let reordered_fields = vec![
            fields[1].clone(),
            fields[0].clone(),
            fields[2].clone(),
            fields[3].clone(),
            fields[4].clone(),
        ];
        let mutation = rewrite_columns(&bytes, reordered_fields, &[1, 0, 2, 3, 4], 1);
        assert!(read_v23_rabitq_row_planes(&mutation, &identity("row-codes", &mutation)).is_err());

        let mutation = rewrite_columns(&bytes, fields, &[0, 1, 2, 3, 4], 2);
        let decoded =
            read_v23_rabitq_row_planes(&mutation, &identity("row-codes", &mutation)).unwrap();
        assert_eq!(decoded.sign_codes.len(), expected.sign_codes.len() * 2);
        assert_eq!(
            &decoded.sign_codes[..expected.sign_codes.len()],
            &expected.sign_codes
        );
        assert_eq!(
            &decoded.sign_codes[expected.sign_codes.len()..],
            &expected.sign_codes
        );

        let mut reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let mut columns = batch.columns().to_vec();
        columns[0] = Arc::new(
            FixedSizeBinaryArray::try_from_iter([[0_u8; 11]; 3].iter().map(<[_; 11]>::as_slice))
                .unwrap(),
        );
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|value| value.as_ref().clone())
            .collect::<Vec<_>>();
        fields[0] = Field::new("sign_code", DataType::FixedSizeBinary(11), false);
        let mutation = super::encode_batch(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
        )
        .unwrap();
        assert!(read_v23_rabitq_row_planes(&mutation, &identity("row-codes", &mutation)).is_err());

        let mut invalid = expected.clone();
        invalid.residual_norms.pop();
        assert!(encode_v23_rabitq_row_planes(&invalid).is_err());
        let mut invalid = expected.clone();
        invalid.alignments[1] = f32::NAN;
        assert!(encode_v23_rabitq_row_planes(&invalid).is_err());
        let mut invalid = expected.clone();
        invalid.alignments[1] = 0.01;
        assert!(encode_v23_rabitq_row_planes(&invalid).is_err());
        let mut invalid = expected.clone();
        invalid.primary_pages[1] = u32::MAX;
        assert!(encode_v23_rabitq_row_planes(&invalid).is_err());
        let mut invalid = expected.clone();
        invalid.sign_codes[0][0] = 1;
        assert!(encode_v23_rabitq_row_planes(&invalid).is_err());
        let mut invalid = expected;
        invalid.replica_pages[1] = invalid.primary_pages[1];
        assert!(encode_v23_rabitq_row_planes(&invalid).is_err());
    }

    #[test]
    fn v23_rabitq_arrow_geometry_rejects_schema_offsets_and_rotation_drift() {
        let expected = geometry();
        let bytes = encode_v23_rabitq_geometry(&expected).unwrap();
        let ids = V23RaBitQGeometryIdentities {
            leaf_offsets: identity("leaf-offsets", &bytes.leaf_offsets),
            centroids: identity("centroids", &bytes.centroids),
            rotation: identity("rotation", &bytes.rotation),
        };
        assert_eq!(read_v23_rabitq_geometry(&bytes, &ids, 3).unwrap(), expected);

        let mut invalid = expected.clone();
        invalid.leaf_offsets[2] = 4;
        assert!(encode_v23_rabitq_geometry(&invalid).is_err());
        let mut invalid = expected.clone();
        invalid.leaf_offsets[3] = 2;
        let invalid_bytes = encode_v23_rabitq_geometry(&invalid).unwrap();
        let invalid_ids = V23RaBitQGeometryIdentities {
            leaf_offsets: identity("leaf-offsets", &invalid_bytes.leaf_offsets),
            centroids: identity("centroids", &invalid_bytes.centroids),
            rotation: identity("rotation", &invalid_bytes.rotation),
        };
        assert!(read_v23_rabitq_geometry(&invalid_bytes, &invalid_ids, 3).is_err());

        let duplicate_offsets = rewrite_columns(
            &bytes.leaf_offsets,
            vec![Field::new("leaf_offset", DataType::UInt64, false)],
            &[0],
            2,
        );
        let duplicate_bytes = super::V23RaBitQGeometryBytes {
            leaf_offsets: duplicate_offsets,
            centroids: bytes.centroids.clone(),
            rotation: bytes.rotation.clone(),
        };
        let duplicate_ids = V23RaBitQGeometryIdentities {
            leaf_offsets: identity("leaf-offsets", &duplicate_bytes.leaf_offsets),
            centroids: identity("centroids", &duplicate_bytes.centroids),
            rotation: identity("rotation", &duplicate_bytes.rotation),
        };
        assert!(read_v23_rabitq_geometry(&duplicate_bytes, &duplicate_ids, 3).is_err());

        let child = Arc::new(Field::new("item", DataType::Float16, false));
        let values = Arc::new(Float16Array::from_iter_values(
            expected.centroids.iter().flatten().copied(),
        ));
        let array = FixedSizeListArray::try_new(Arc::clone(&child), 96, values, None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "centroid",
            DataType::FixedSizeList(child, 96),
            false,
        )]));
        let wrong_child =
            super::encode_batch(&RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap())
                .unwrap();
        let wrong_child_bytes = super::V23RaBitQGeometryBytes {
            leaf_offsets: bytes.leaf_offsets.clone(),
            centroids: wrong_child,
            rotation: bytes.rotation.clone(),
        };
        let wrong_child_ids = V23RaBitQGeometryIdentities {
            leaf_offsets: identity("leaf-offsets", &wrong_child_bytes.leaf_offsets),
            centroids: identity("centroids", &wrong_child_bytes.centroids),
            rotation: identity("rotation", &wrong_child_bytes.rotation),
        };
        assert!(read_v23_rabitq_geometry(&wrong_child_bytes, &wrong_child_ids, 3).is_err());

        let mut invalid = expected.clone();
        invalid.centroids[0][0] = f16::NAN;
        assert!(encode_v23_rabitq_geometry(&invalid).is_err());
        let mut invalid = expected;
        invalid.rotation[0][1] = 0.25;
        assert!(encode_v23_rabitq_geometry(&invalid).is_err());
    }
}
