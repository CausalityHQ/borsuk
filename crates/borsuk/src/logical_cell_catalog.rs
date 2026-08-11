use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt16Array, UInt32Array,
    UInt64Array, types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::{
    BorsukError, LogicalCellId, Result,
    metric::{VectorMetric, unit_l2_normalized},
};

pub(crate) const LOGICAL_CELL_CATALOG_FORMAT_VERSION: u16 = 34;
const BLAKE3_HEX_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogicalCellCatalogRef {
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) routing_epoch: u64,
    pub(crate) cell_count: u32,
    pub(crate) dimensions: u32,
    pub(crate) encoded_bytes: u64,
}

impl LogicalCellCatalogRef {
    pub(crate) fn new(
        checksum: String,
        routing_epoch: u64,
        cell_count: u32,
        dimensions: u32,
        encoded_bytes: usize,
    ) -> Result<Self> {
        validate_checksum(&checksum)?;
        if routing_epoch == 0 {
            return Err(BorsukError::InvalidStorage(
                "logical-cell catalog routing epoch must be nonzero".to_string(),
            ));
        }
        if cell_count == 0 {
            return Err(BorsukError::InvalidStorage(
                "logical-cell catalog must contain at least one cell".to_string(),
            ));
        }
        if dimensions == 0 {
            return Err(BorsukError::InvalidStorage(
                "logical-cell catalog dimensions must be nonzero".to_string(),
            ));
        }
        let encoded_bytes = u64::try_from(encoded_bytes).map_err(|_| {
            BorsukError::InvalidStorage(
                "logical-cell catalog encoded length does not fit u64".to_string(),
            )
        })?;
        if encoded_bytes == 0 {
            return Err(BorsukError::InvalidStorage(
                "logical-cell catalog must not be empty".to_string(),
            ));
        }
        let path = logical_cell_catalog_path(&checksum)?;
        Ok(Self {
            path,
            checksum,
            routing_epoch,
            cell_count,
            dimensions,
            encoded_bytes,
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let encoded_bytes = usize::try_from(self.encoded_bytes).map_err(|_| {
            BorsukError::InvalidStorage(
                "logical-cell catalog encoded length does not fit usize".to_string(),
            )
        })?;
        let canonical = Self::new(
            self.checksum.clone(),
            self.routing_epoch,
            self.cell_count,
            self.dimensions,
            encoded_bytes,
        )?;
        if self.path != canonical.path {
            return Err(BorsukError::InvalidStorage(format!(
                "logical-cell catalog path must be `{}`, got `{}`",
                canonical.path, self.path
            )));
        }
        Ok(())
    }

    pub(crate) fn decoded_resident_bytes(&self) -> Result<u64> {
        let cells = u64::from(self.cell_count);
        let dimensions = u64::from(self.dimensions);
        let ids = cells
            .checked_mul(
                u64::try_from(std::mem::size_of::<LogicalCellId>()).map_err(|_| {
                    BorsukError::InvalidStorage("logical-cell ID size does not fit u64".to_string())
                })?,
            )
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "logical-cell catalog resident byte estimate overflow".to_string(),
                )
            })?;
        let centroids = cells
            .checked_mul(dimensions)
            .and_then(|values| values.checked_mul(u64::try_from(std::mem::size_of::<f32>()).ok()?))
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "logical-cell catalog resident byte estimate overflow".to_string(),
                )
            })?;
        ids.checked_add(centroids).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "logical-cell catalog resident byte estimate overflow".to_string(),
            )
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LogicalCellCatalog {
    pub(crate) cells: Vec<LogicalCellId>,
    pub(crate) centroids: Vec<Vec<f32>>,
}

impl LogicalCellCatalog {
    pub(crate) fn from_centroids(
        routing_epoch: u64,
        dimensions: usize,
        metric: VectorMetric,
        centroids: Vec<Vec<f32>>,
    ) -> Result<Self> {
        if routing_epoch == 0 {
            return Err(BorsukError::InvalidStorage(
                "logical-cell catalog routing epoch must be nonzero".to_string(),
            ));
        }
        if dimensions == 0 {
            return Err(BorsukError::InvalidStorage(
                "logical-cell catalog dimensions must be nonzero".to_string(),
            ));
        }
        if centroids.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "logical-cell catalog must contain at least one centroid".to_string(),
            ));
        }
        let count = u32::try_from(centroids.len()).map_err(|_| {
            BorsukError::InvalidStorage(
                "logical-cell count exceeds the u32 catalog limit".to_string(),
            )
        })?;
        let centroids = centroids
            .into_iter()
            .enumerate()
            .map(|(ordinal, centroid)| {
                if centroid.len() != dimensions {
                    return Err(BorsukError::DimensionMismatch {
                        expected: dimensions,
                        actual: centroid.len(),
                    });
                }
                if centroid.iter().any(|value| !value.is_finite()) {
                    return Err(BorsukError::InvalidStorage(format!(
                        "logical-cell centroid {ordinal} contains a non-finite value"
                    )));
                }
                if metric.uses_normalized_euclidean_geometry() {
                    let norm_squared = centroid.iter().try_fold(0.0_f64, |sum, value| {
                        let value = f64::from(*value);
                        let next = sum + value * value;
                        next.is_finite().then_some(next).ok_or_else(|| {
                            BorsukError::InvalidStorage(format!(
                                "logical-cell centroid {ordinal} has an unrepresentable norm"
                            ))
                        })
                    })?;
                    if norm_squared.sqrt() <= f64::from(f32::EPSILON) {
                        return Err(BorsukError::InvalidStorage(format!(
                            "logical-cell centroid {ordinal} has zero norm"
                        )));
                    }
                    let normalized = unit_l2_normalized(&centroid);
                    validate_stored_centroid(
                        u32::try_from(ordinal).map_err(|_| {
                            BorsukError::InvalidStorage(
                                "logical-cell centroid ordinal exceeds u32".to_string(),
                            )
                        })?,
                        &normalized,
                        &metric,
                    )?;
                    Ok(normalized)
                } else {
                    Ok(centroid)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let cells = (0..count)
            .map(|ordinal| LogicalCellId::new(routing_epoch, ordinal))
            .collect();
        Ok(Self { cells, centroids })
    }

    pub(crate) fn routing_epoch(&self) -> u64 {
        self.cells[0].routing_epoch
    }

    pub(crate) fn dimensions(&self) -> usize {
        self.centroids[0].len()
    }
}

pub(crate) fn logical_cell_catalog_path(checksum: &str) -> Result<String> {
    validate_checksum(checksum)?;
    Ok(format!(
        "logical-cell-catalogs/{}/{}.parquet",
        &checksum[..2],
        checksum
    ))
}

pub(crate) fn logical_cell_catalog_to_parquet(catalog: &LogicalCellCatalog) -> Result<Vec<u8>> {
    let row_count = catalog.cells.len();
    if row_count == 0 || row_count != catalog.centroids.len() {
        return Err(BorsukError::InvalidStorage(
            "logical-cell catalog IDs and centroids must be nonempty and aligned".to_string(),
        ));
    }
    let routing_epoch = catalog.routing_epoch();
    let dimensions = catalog.dimensions();
    let dimensions_i32 = i32::try_from(dimensions).map_err(|_| {
        BorsukError::InvalidStorage(
            "logical-cell catalog dimensions exceed Arrow's i32 limit".to_string(),
        )
    })?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("routing_epoch", DataType::UInt64, false),
        Field::new("cell_ordinal", DataType::UInt32, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new_list_field(DataType::Float32, true)),
                dimensions_i32,
            ),
            false,
        ),
    ]));
    let centroid_values = catalog
        .centroids
        .iter()
        .map(|centroid| Some(centroid.iter().copied().map(Some).collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt16Array::from_iter_values(
            (0..row_count).map(|_| LOGICAL_CELL_CATALOG_FORMAT_VERSION),
        )),
        Arc::new(UInt64Array::from_iter_values(
            (0..row_count).map(|_| routing_epoch),
        )),
        Arc::new(UInt32Array::from_iter_values(
            catalog.cells.iter().map(|cell| cell.cell_ordinal),
        )),
        Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                centroid_values,
                dimensions_i32,
            ),
        ),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn logical_cell_catalog_from_parquet(
    bytes: &[u8],
    reference: &LogicalCellCatalogRef,
    metric: VectorMetric,
) -> Result<LogicalCellCatalog> {
    reference.validate()?;
    if u64::try_from(bytes.len()).ok() != Some(reference.encoded_bytes) {
        return Err(BorsukError::InvalidStorage(
            "logical-cell catalog encoded length does not match its reference".to_string(),
        ));
    }
    if blake3::hash(bytes).to_hex().as_str() != reference.checksum {
        return Err(BorsukError::InvalidStorage(
            "logical-cell catalog checksum does not match its reference".to_string(),
        ));
    }
    let dimensions = usize::try_from(reference.dimensions).map_err(|_| {
        BorsukError::InvalidStorage("logical-cell catalog dimensions do not fit usize".to_string())
    })?;
    let expected_schema = Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("routing_epoch", DataType::UInt64, false),
        Field::new("cell_ordinal", DataType::UInt32, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new_list_field(DataType::Float32, true)),
                i32::try_from(dimensions).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "logical-cell catalog dimensions exceed Arrow's i32 limit".to_string(),
                    )
                })?,
            ),
            false,
        ),
    ]);
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    if builder.schema().as_ref() != &expected_schema {
        return Err(BorsukError::InvalidStorage(format!(
            "logical-cell catalog schema mismatch: expected {expected_schema:?}, got {:?}",
            builder.schema()
        )));
    }
    let mut centroids = Vec::with_capacity(reference.cell_count as usize);
    let mut expected_ordinal = 0_u32;
    for batch in builder.build()? {
        let batch = batch?;
        let versions = downcast::<UInt16Array>(&batch, 0, "format_version")?;
        let epochs = downcast::<UInt64Array>(&batch, 1, "routing_epoch")?;
        let ordinals = downcast::<UInt32Array>(&batch, 2, "cell_ordinal")?;
        let vectors = downcast::<FixedSizeListArray>(&batch, 3, "centroid")?;
        for row in 0..batch.num_rows() {
            if versions.is_null(row)
                || epochs.is_null(row)
                || ordinals.is_null(row)
                || vectors.is_null(row)
            {
                return Err(BorsukError::InvalidStorage(
                    "logical-cell catalog columns must not contain nulls".to_string(),
                ));
            }
            if versions.value(row) != LOGICAL_CELL_CATALOG_FORMAT_VERSION {
                return Err(BorsukError::InvalidStorage(format!(
                    "unsupported logical-cell catalog version {}",
                    versions.value(row)
                )));
            }
            if epochs.value(row) != reference.routing_epoch {
                return Err(BorsukError::InvalidStorage(
                    "logical-cell catalog routing epoch does not match its reference".to_string(),
                ));
            }
            if ordinals.value(row) != expected_ordinal {
                return Err(BorsukError::InvalidStorage(format!(
                    "logical-cell catalog ordinal must be {expected_ordinal}, got {}",
                    ordinals.value(row)
                )));
            }
            let values = vectors.value(row);
            let values = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "logical-cell centroid values must be Float32".to_string(),
                    )
                })?;
            if values.len() != dimensions || values.null_count() != 0 {
                return Err(BorsukError::InvalidStorage(
                    "logical-cell centroid dimensions or nullability are invalid".to_string(),
                ));
            }
            let centroid = values.values().to_vec();
            validate_stored_centroid(expected_ordinal, &centroid, &metric)?;
            centroids.push(centroid);
            expected_ordinal = expected_ordinal.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("logical-cell catalog ordinal exceeds u32".to_string())
            })?;
        }
    }
    if expected_ordinal != reference.cell_count {
        return Err(BorsukError::InvalidStorage(format!(
            "logical-cell catalog contains {expected_ordinal} rows, expected {}",
            reference.cell_count
        )));
    }
    let cells = (0..reference.cell_count)
        .map(|ordinal| LogicalCellId::new(reference.routing_epoch, ordinal))
        .collect();
    Ok(LogicalCellCatalog { cells, centroids })
}

fn validate_checksum(checksum: &str) -> Result<()> {
    if checksum.len() != BLAKE3_HEX_LEN
        || !checksum
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(BorsukError::InvalidStorage(
            "logical-cell catalog checksum must be 64 lowercase hexadecimal characters".to_string(),
        ));
    }
    Ok(())
}

fn downcast<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    column: usize,
    name: &str,
) -> Result<&'a T> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "logical-cell catalog column `{name}` has an invalid type"
            ))
        })
}

fn validate_stored_centroid(ordinal: u32, centroid: &[f32], metric: &VectorMetric) -> Result<()> {
    if centroid.iter().any(|value| !value.is_finite()) {
        return Err(BorsukError::InvalidStorage(format!(
            "logical-cell centroid {ordinal} contains a non-finite value"
        )));
    }
    if metric.uses_normalized_euclidean_geometry() {
        let norm_squared = centroid.iter().fold(0.0_f64, |sum, value| {
            let value = f64::from(*value);
            sum + value * value
        });
        if !norm_squared.is_finite() || (norm_squared.sqrt() - 1.0).abs() > 1.0e-5 {
            return Err(BorsukError::InvalidStorage(format!(
                "logical-cell centroid {ordinal} is not unit-L2-normalized"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_for(
        bytes: &[u8],
        routing_epoch: u64,
        cell_count: u32,
        dimensions: u32,
    ) -> LogicalCellCatalogRef {
        LogicalCellCatalogRef::new(
            blake3::hash(bytes).to_hex().to_string(),
            routing_epoch,
            cell_count,
            dimensions,
            bytes.len(),
        )
        .unwrap()
    }

    fn rewrite_catalog(bytes: &[u8], mutate: impl FnOnce(RecordBatch) -> RecordBatch) -> Vec<u8> {
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
            .unwrap()
            .build()
            .unwrap();
        let batch = mutate(reader.next().unwrap().unwrap());
        assert!(reader.next().is_none());
        let mut output = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut output,
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        output
    }

    fn replace_column(batch: RecordBatch, index: usize, column: ArrayRef) -> RecordBatch {
        let mut columns = batch.columns().to_vec();
        columns[index] = column;
        RecordBatch::try_new(batch.schema(), columns).unwrap()
    }

    #[test]
    fn cosine_catalog_round_trips_as_canonical_normalized_rows() {
        let catalog = LogicalCellCatalog::from_centroids(
            7,
            4,
            VectorMetric::Cosine,
            vec![
                vec![3.0, 4.0, 0.0, 0.0],
                vec![0.0, 0.0, 5.0, 12.0],
                vec![1.0, 0.0, 0.0, 0.0],
            ],
        )
        .unwrap();
        let bytes = logical_cell_catalog_to_parquet(&catalog).unwrap();
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let reference = LogicalCellCatalogRef::new(checksum, 7, 3, 4, bytes.len()).unwrap();

        let decoded =
            logical_cell_catalog_from_parquet(&bytes, &reference, VectorMetric::Cosine).unwrap();

        assert_eq!(decoded, catalog);
        assert_eq!(
            decoded.cells,
            vec![
                LogicalCellId::new(7, 0),
                LogicalCellId::new(7, 1),
                LogicalCellId::new(7, 2),
            ]
        );
        assert_eq!(decoded.centroids[0], vec![0.6, 0.8, 0.0, 0.0]);
        assert!((decoded.centroids[1][2] - 5.0 / 13.0).abs() < 1.0e-6);
        assert!((decoded.centroids[1][3] - 12.0 / 13.0).abs() < 1.0e-6);
        assert_eq!(
            reference.path,
            format!(
                "logical-cell-catalogs/{}/{}.parquet",
                &reference.checksum[..2],
                reference.checksum
            )
        );
    }

    #[test]
    fn catalog_constructor_rejects_invalid_geometry_and_shape() {
        assert!(
            LogicalCellCatalog::from_centroids(0, 2, VectorMetric::Cosine, vec![vec![1.0, 0.0]],)
                .is_err()
        );
        assert!(
            LogicalCellCatalog::from_centroids(1, 0, VectorMetric::Cosine, vec![vec![]]).is_err()
        );
        assert!(
            LogicalCellCatalog::from_centroids(1, 2, VectorMetric::Cosine, Vec::new()).is_err()
        );
        assert!(
            LogicalCellCatalog::from_centroids(1, 2, VectorMetric::Cosine, vec![vec![1.0]],)
                .is_err()
        );
        assert!(
            LogicalCellCatalog::from_centroids(
                1,
                2,
                VectorMetric::Cosine,
                vec![vec![f32::NAN, 1.0]],
            )
            .is_err()
        );
        assert!(
            LogicalCellCatalog::from_centroids(1, 2, VectorMetric::Cosine, vec![vec![0.0, 0.0]],)
                .is_err()
        );
        assert!(
            LogicalCellCatalog::from_centroids(
                1,
                2,
                VectorMetric::Cosine,
                vec![vec![f32::MAX, f32::MAX]],
            )
            .is_err(),
            "finite inputs whose f32 normalization overflows must fail before publication"
        );
    }

    #[test]
    fn catalog_reference_rejects_noncanonical_identity_and_bounds() {
        let checksum = "ab".repeat(32);
        assert!(LogicalCellCatalogRef::new(checksum.clone(), 0, 1, 2, 1).is_err());
        assert!(LogicalCellCatalogRef::new(checksum.clone(), 1, 0, 2, 1).is_err());
        assert!(LogicalCellCatalogRef::new(checksum.clone(), 1, 1, 0, 1).is_err());
        assert!(LogicalCellCatalogRef::new(checksum.clone(), 1, 1, 2, 0).is_err());
        assert!(LogicalCellCatalogRef::new("AB".repeat(32), 1, 1, 2, 1).is_err());
        assert!(LogicalCellCatalogRef::new("a".repeat(63), 1, 1, 2, 1).is_err());

        let mut reference = LogicalCellCatalogRef::new(checksum, 1, 1, 2, 1).unwrap();
        reference.path = "logical-cell-catalogs/ff/wrong.parquet".to_string();
        assert!(reference.validate().is_err());
    }

    #[test]
    fn catalog_decoder_authenticates_bytes_before_parquet_decode() {
        let catalog =
            LogicalCellCatalog::from_centroids(7, 2, VectorMetric::Euclidean, vec![vec![1.0, 2.0]])
                .unwrap();
        let bytes = logical_cell_catalog_to_parquet(&catalog).unwrap();
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let reference = LogicalCellCatalogRef::new(checksum, 7, 1, 2, bytes.len()).unwrap();

        let mut corrupt = bytes.clone();
        corrupt[0] ^= 1;
        assert!(
            logical_cell_catalog_from_parquet(&corrupt, &reference, VectorMetric::Euclidean)
                .is_err()
        );

        let short_reference = LogicalCellCatalogRef::new(
            blake3::hash(&bytes).to_hex().to_string(),
            7,
            1,
            2,
            bytes.len() - 1,
        )
        .unwrap();
        assert!(
            logical_cell_catalog_from_parquet(&bytes, &short_reference, VectorMetric::Euclidean,)
                .is_err()
        );
    }

    #[test]
    fn catalog_decoder_rejects_version_epoch_ordinal_and_geometry_drift() {
        let catalog = LogicalCellCatalog::from_centroids(
            7,
            2,
            VectorMetric::Euclidean,
            vec![vec![2.0, 0.0], vec![0.0, 1.0]],
        )
        .unwrap();
        let bytes = logical_cell_catalog_to_parquet(&catalog).unwrap();

        let old_version = rewrite_catalog(&bytes, |batch| {
            replace_column(batch, 0, Arc::new(UInt16Array::from_iter_values([33, 33])))
        });
        assert!(
            logical_cell_catalog_from_parquet(
                &old_version,
                &reference_for(&old_version, 7, 2, 2),
                VectorMetric::Euclidean,
            )
            .is_err()
        );

        let wrong_epoch = rewrite_catalog(&bytes, |batch| {
            replace_column(batch, 1, Arc::new(UInt64Array::from_iter_values([7, 8])))
        });
        assert!(
            logical_cell_catalog_from_parquet(
                &wrong_epoch,
                &reference_for(&wrong_epoch, 7, 2, 2),
                VectorMetric::Euclidean,
            )
            .is_err()
        );

        let skipped_ordinal = rewrite_catalog(&bytes, |batch| {
            replace_column(batch, 2, Arc::new(UInt32Array::from_iter_values([0, 2])))
        });
        assert!(
            logical_cell_catalog_from_parquet(
                &skipped_ordinal,
                &reference_for(&skipped_ordinal, 7, 2, 2),
                VectorMetric::Euclidean,
            )
            .is_err()
        );

        let non_finite = rewrite_catalog(&bytes, |batch| {
            replace_column(
                batch,
                3,
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        [
                            Some(vec![Some(f32::NAN), Some(0.0)]),
                            Some(vec![Some(0.0), Some(1.0)]),
                        ],
                        2,
                    ),
                ),
            )
        });
        assert!(
            logical_cell_catalog_from_parquet(
                &non_finite,
                &reference_for(&non_finite, 7, 2, 2),
                VectorMetric::Euclidean,
            )
            .is_err()
        );

        let non_normalized = rewrite_catalog(&bytes, |batch| batch);
        assert!(
            logical_cell_catalog_from_parquet(
                &non_normalized,
                &reference_for(&non_normalized, 7, 2, 2),
                VectorMetric::Cosine,
            )
            .is_err(),
            "a Euclidean catalog must not be accepted as normalized cosine geometry"
        );
    }

    #[test]
    fn catalog_decoder_rejects_schema_count_and_dimension_drift() {
        let catalog =
            LogicalCellCatalog::from_centroids(7, 2, VectorMetric::Euclidean, vec![vec![1.0, 2.0]])
                .unwrap();
        let bytes = logical_cell_catalog_to_parquet(&catalog).unwrap();

        assert!(
            logical_cell_catalog_from_parquet(
                &bytes,
                &reference_for(&bytes, 7, 2, 2),
                VectorMetric::Euclidean,
            )
            .is_err()
        );
        assert!(
            logical_cell_catalog_from_parquet(
                &bytes,
                &reference_for(&bytes, 7, 1, 3),
                VectorMetric::Euclidean,
            )
            .is_err()
        );

        let renamed = rewrite_catalog(&bytes, |batch| {
            let mut fields = batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            fields[2] = Field::new("wrong_ordinal", DataType::UInt32, false);
            RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap()
        });
        assert!(
            logical_cell_catalog_from_parquet(
                &renamed,
                &reference_for(&renamed, 7, 1, 2),
                VectorMetric::Euclidean,
            )
            .is_err()
        );
    }

    #[test]
    fn catalog_16k_by_768_stays_below_the_content_addressed_multipart_boundary() {
        const CELLS: usize = 16_000;
        const DIMENSIONS: usize = 768;
        const MULTIPART_BOUNDARY_BYTES: usize = 64 * 1024 * 1024;
        let centroids = (0..CELLS)
            .map(|cell| {
                (0..DIMENSIONS)
                    .map(|dimension| {
                        let mixed = (cell as u32)
                            .wrapping_mul(0x9e37_79b9)
                            .wrapping_add((dimension as u32).wrapping_mul(0x85eb_ca6b))
                            .rotate_left(((cell + dimension) % 31) as u32);
                        f32::from_bits(0x3f00_0000 | (mixed & 0x007f_ffff)) - 1.0
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let catalog =
            LogicalCellCatalog::from_centroids(1, DIMENSIONS, VectorMetric::Euclidean, centroids)
                .unwrap();

        let bytes = logical_cell_catalog_to_parquet(&catalog).unwrap();
        let reference = reference_for(&bytes, 1, CELLS as u32, DIMENSIONS as u32);
        let decoded =
            logical_cell_catalog_from_parquet(&bytes, &reference, VectorMetric::Euclidean).unwrap();

        assert!(bytes.len() < MULTIPART_BOUNDARY_BYTES);
        assert_eq!(decoded.cells.len(), CELLS);
        assert_eq!(decoded.centroids.len(), CELLS);
        assert_eq!(decoded.dimensions(), DIMENSIONS);
    }
}
