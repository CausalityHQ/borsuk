use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, StringArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};

use crate::{
    error::{BorsukError, Result},
    native_ann::{NativeArtifactRef, NativeRouterRef},
};

const PQ_WIDTH: usize = 16;
const CODEWORDS_PER_SUBSPACE: usize = 256;
const CODEBOOK_KINDS: usize = 2;
const SUMMARY_CODES_PER_PAGE: usize = 2;
const MUTATION_DIRECTORY_BYTES_PER_ENTRY: u64 = 96;
const DELTA_ROW_FIXED_BYTES: u64 = 72;
const THREE_GIB: u64 = 3 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeRouterArtifacts {
    pub(crate) row_codebooks: Box<[f32]>,
    pub(crate) summary_codebooks: Box<[f32]>,
    pub(crate) row_codes: Box<[u8]>,
    pub(crate) summary_codes: Box<[u8]>,
    pub(crate) page_count: u32,
    pub(crate) physical_rows: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeResidentWorksheet {
    pub(crate) physical_rows: u64,
    pub(crate) page_count: u64,
    pub(crate) dimensions: u64,
    pub(crate) codebook_and_sq8_bytes: u64,
    pub(crate) mutation_entries: u64,
    pub(crate) resident_delta_rows: u64,
    pub(crate) response_concurrency: u64,
    pub(crate) response_bytes_each: u64,
    pub(crate) workspace_bytes: u64,
    pub(crate) runtime_reserve_bytes: u64,
}

fn invalid(message: impl Into<String>) -> BorsukError {
    BorsukError::InvalidStorage(message.into())
}

fn checked_product(values: &[u64], name: &str) -> Result<u64> {
    values.iter().try_fold(1_u64, |product, value| {
        product
            .checked_mul(*value)
            .ok_or_else(|| invalid(format!("native ANN {name} overflows")))
    })
}

impl NativeResidentWorksheet {
    pub(crate) fn validate_under_3_gib(self) -> Result<u64> {
        if self.physical_rows == 0
            || self.page_count == 0
            || self.dimensions == 0
            || self.response_concurrency == 0
            || self.response_bytes_each == 0
            || self.runtime_reserve_bytes == 0
        {
            return Err(invalid(
                "native ANN resident worksheet contains a zero bound",
            ));
        }
        let expected_pages = self.physical_rows.div_ceil(256);
        if expected_pages != self.page_count {
            return Err(invalid("native ANN resident worksheet page count differs"));
        }
        let terms = [
            checked_product(&[self.physical_rows, PQ_WIDTH as u64], "row codes")?,
            checked_product(
                &[
                    self.page_count,
                    SUMMARY_CODES_PER_PAGE as u64,
                    PQ_WIDTH as u64,
                ],
                "summary codes",
            )?,
            self.codebook_and_sq8_bytes,
            checked_product(
                &[self.mutation_entries, MUTATION_DIRECTORY_BYTES_PER_ENTRY],
                "mutation directory",
            )?,
            checked_product(
                &[
                    self.resident_delta_rows,
                    self.dimensions
                        .checked_add(DELTA_ROW_FIXED_BYTES)
                        .ok_or_else(|| invalid("native ANN delta row width overflows"))?,
                ],
                "resident delta",
            )?,
            checked_product(
                &[self.response_concurrency, self.response_bytes_each],
                "response buffers",
            )?,
            self.workspace_bytes,
            self.runtime_reserve_bytes,
        ];
        let total = terms.iter().try_fold(0_u64, |total, term| {
            total
                .checked_add(*term)
                .ok_or_else(|| invalid("native ANN resident worksheet total overflows"))
        })?;
        if total >= THREE_GIB {
            return Err(BorsukError::RamBudgetExceeded {
                resident_bytes: total,
                budget_bytes: THREE_GIB - 1,
            });
        }
        Ok(total)
    }
}

fn authenticate(reference: &NativeArtifactRef, expected_role: &str, bytes: &[u8]) -> Result<()> {
    reference.validate(expected_role)?;
    if reference.encoded_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || reference.sha256 != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid(format!(
            "native ANN {expected_role} byte authority differs"
        )));
    }
    Ok(())
}

fn codebook_schema(centroid_width: i32) -> Schema {
    Schema::new(vec![
        Field::new("kind", DataType::Utf8, false),
        Field::new("subspace", DataType::UInt16, false),
        Field::new("codeword", DataType::UInt16, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                centroid_width,
            ),
            false,
        ),
    ])
}

fn code_schema() -> Schema {
    Schema::new(vec![Field::new(
        "code",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::UInt8, false)),
            PQ_WIDTH as i32,
        ),
        false,
    )])
}

fn summary_schema() -> Schema {
    Schema::new(vec![
        Field::new("page", DataType::UInt32, false),
        Field::new("block", DataType::UInt8, false),
        Field::new(
            "code",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::UInt8, false)),
                PQ_WIDTH as i32,
            ),
            false,
        ),
    ])
}

fn metadata_rows(
    builder: &ParquetRecordBatchReaderBuilder<Bytes>,
    expected: u64,
    role: &str,
) -> Result<()> {
    let rows = builder.metadata().file_metadata().num_rows();
    if rows < 0 || u64::try_from(rows).ok() != Some(expected) {
        return Err(invalid(format!("native ANN {role} row count differs")));
    }
    Ok(())
}

fn decode_codebooks(bytes: Bytes, dimensions: u32) -> Result<(Box<[f32]>, Box<[f32]>)> {
    let centroid_width = usize::try_from(dimensions)
        .map_err(|_| invalid("native ANN dimensions exceed usize"))?
        .div_ceil(PQ_WIDTH);
    let centroid_width_i32 = i32::try_from(centroid_width)
        .map_err(|_| invalid("native ANN centroid width exceeds i32"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    if builder.schema().as_ref() != &codebook_schema(centroid_width_i32) {
        return Err(invalid("native ANN codebook physical schema differs"));
    }
    let expected_rows = (CODEBOOK_KINDS * PQ_WIDTH * CODEWORDS_PER_SUBSPACE) as u64;
    metadata_rows(&builder, expected_rows, "codebook")?;
    let expected_values = PQ_WIDTH
        .checked_mul(CODEWORDS_PER_SUBSPACE)
        .and_then(|rows| rows.checked_mul(centroid_width))
        .ok_or_else(|| invalid("native ANN codebook allocation overflows"))?;
    let mut row_codebooks = Vec::with_capacity(expected_values);
    let mut summary_codebooks = Vec::with_capacity(expected_values);
    let mut ordinal = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 4 || batch.columns().iter().any(|array| array.null_count() != 0) {
            return Err(invalid("native ANN codebook batch shape differs"));
        }
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("native ANN codebook kind column differs"))?;
        let subspaces = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("native ANN codebook subspace column differs"))?;
        let codewords = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("native ANN codebook codeword column differs"))?;
        let centroids = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("native ANN codebook centroid column differs"))?;
        if centroids.null_count() != 0 || centroids.values().null_count() != 0 {
            return Err(invalid("native ANN codebook centroid contains nulls"));
        }
        for row in 0..batch.num_rows() {
            let kind_ordinal = ordinal / (PQ_WIDTH * CODEWORDS_PER_SUBSPACE);
            let within_kind = ordinal % (PQ_WIDTH * CODEWORDS_PER_SUBSPACE);
            let expected_subspace = within_kind / CODEWORDS_PER_SUBSPACE;
            let expected_codeword = within_kind % CODEWORDS_PER_SUBSPACE;
            let expected_kind = match kind_ordinal {
                0 => "row",
                1 => "summary",
                _ => return Err(invalid("native ANN codebook has excess rows")),
            };
            if kinds.value(row) != expected_kind
                || usize::from(subspaces.value(row)) != expected_subspace
                || usize::from(codewords.value(row)) != expected_codeword
            {
                return Err(invalid("native ANN codebook rows are not canonical"));
            }
            let centroid = centroids.value(row);
            let values = centroid
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("native ANN codebook centroid child differs"))?;
            if values.len() != centroid_width || values.null_count() != 0 {
                return Err(invalid("native ANN codebook centroid width differs"));
            }
            let start = expected_subspace * dimensions as usize / PQ_WIDTH;
            let end = (expected_subspace + 1) * dimensions as usize / PQ_WIDTH;
            let active = end - start;
            for (lane, value) in values.values().iter().copied().enumerate() {
                if !value.is_finite() || (lane >= active && value.to_bits() != 0) {
                    return Err(invalid("native ANN codebook centroid value differs"));
                }
            }
            match kind_ordinal {
                0 => row_codebooks.extend_from_slice(values.values()),
                1 => summary_codebooks.extend_from_slice(values.values()),
                _ => unreachable!(),
            }
            ordinal += 1;
        }
    }
    if ordinal != expected_rows as usize
        || row_codebooks.len() != expected_values
        || summary_codebooks.len() != expected_values
    {
        return Err(invalid("native ANN codebook materialization differs"));
    }
    Ok((
        row_codebooks.into_boxed_slice(),
        summary_codebooks.into_boxed_slice(),
    ))
}

fn decode_row_codes(bytes: Bytes, physical_rows: u64) -> Result<Box<[u8]>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    if builder.schema().as_ref() != &code_schema() {
        return Err(invalid("native ANN row-code physical schema differs"));
    }
    metadata_rows(&builder, physical_rows, "row-code")?;
    let expected_values_u64 = checked_product(&[physical_rows, PQ_WIDTH as u64], "row codes")?;
    let expected_values = usize::try_from(expected_values_u64)
        .map_err(|_| invalid("native ANN row-code allocation exceeds usize"))?;
    let mut codes = Vec::with_capacity(expected_values);
    for batch in builder.build()? {
        let batch = batch?;
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("native ANN row-code column differs"))?;
        if batch.num_columns() != 1 || lists.null_count() != 0 || lists.values().null_count() != 0 {
            return Err(invalid("native ANN row-code batch shape differs"));
        }
        for row in 0..batch.num_rows() {
            let value = lists.value(row);
            let values = value
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| invalid("native ANN row-code child differs"))?;
            if values.len() != PQ_WIDTH || values.null_count() != 0 {
                return Err(invalid("native ANN row-code width differs"));
            }
            codes.extend_from_slice(values.values());
        }
    }
    if codes.len() != expected_values {
        return Err(invalid("native ANN row-code materialization differs"));
    }
    Ok(codes.into_boxed_slice())
}

fn decode_summary_codes(bytes: Bytes, page_count: u32) -> Result<Box<[u8]>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    if builder.schema().as_ref() != &summary_schema() {
        return Err(invalid("native ANN summary-code physical schema differs"));
    }
    let expected_rows = u64::from(page_count)
        .checked_mul(SUMMARY_CODES_PER_PAGE as u64)
        .ok_or_else(|| invalid("native ANN summary row count overflows"))?;
    metadata_rows(&builder, expected_rows, "summary-code")?;
    let expected_values_u64 = checked_product(&[expected_rows, PQ_WIDTH as u64], "summary codes")?;
    let expected_values = usize::try_from(expected_values_u64)
        .map_err(|_| invalid("native ANN summary-code allocation exceeds usize"))?;
    let mut codes = Vec::with_capacity(expected_values);
    let mut ordinal = 0_u64;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 3 || batch.columns().iter().any(|array| array.null_count() != 0) {
            return Err(invalid("native ANN summary-code batch shape differs"));
        }
        let pages = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("native ANN summary page column differs"))?;
        let blocks = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("native ANN summary block column differs"))?;
        let lists = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("native ANN summary code column differs"))?;
        if lists.values().null_count() != 0 {
            return Err(invalid("native ANN summary code contains nulls"));
        }
        for row in 0..batch.num_rows() {
            let expected_page = ordinal / SUMMARY_CODES_PER_PAGE as u64;
            let expected_block = ordinal % SUMMARY_CODES_PER_PAGE as u64;
            if u64::from(pages.value(row)) != expected_page
                || u64::from(blocks.value(row)) != expected_block
            {
                return Err(invalid("native ANN summary-code rows are not canonical"));
            }
            let value = lists.value(row);
            let values = value
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| invalid("native ANN summary code child differs"))?;
            if values.len() != PQ_WIDTH || values.null_count() != 0 {
                return Err(invalid("native ANN summary-code width differs"));
            }
            codes.extend_from_slice(values.values());
            ordinal += 1;
        }
    }
    if ordinal != expected_rows || codes.len() != expected_values {
        return Err(invalid("native ANN summary-code materialization differs"));
    }
    Ok(codes.into_boxed_slice())
}

pub(crate) fn decode_native_router(
    reference: &NativeRouterRef,
    dimensions: u32,
    codebook_bytes: Bytes,
    row_code_bytes: Bytes,
    summary_code_bytes: Bytes,
) -> Result<NativeRouterArtifacts> {
    if dimensions == 0
        || reference.pq_width != PQ_WIDTH as u8
        || reference.summary_codes_per_page != SUMMARY_CODES_PER_PAGE as u8
        || reference.physical_rows == 0
        || reference.page_count == 0
        || reference.physical_rows.div_ceil(256) != u64::from(reference.page_count)
    {
        return Err(invalid("native ANN router reference shape differs"));
    }
    authenticate(&reference.codebooks, "router-codebooks", &codebook_bytes)?;
    authenticate(&reference.row_codes, "router-row-codes", &row_code_bytes)?;
    authenticate(
        &reference.summary_codes,
        "router-summary-codes",
        &summary_code_bytes,
    )?;
    let (row_codebooks, summary_codebooks) = decode_codebooks(codebook_bytes, dimensions)?;
    let row_codes = decode_row_codes(row_code_bytes, reference.physical_rows)?;
    let summary_codes = decode_summary_codes(summary_code_bytes, reference.page_count)?;
    let resident_router_bytes = row_codebooks
        .len()
        .checked_add(summary_codebooks.len())
        .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
        .and_then(|bytes| bytes.checked_add(row_codes.len()))
        .and_then(|bytes| bytes.checked_add(summary_codes.len()))
        .ok_or_else(|| invalid("native ANN router resident bytes overflow"))?;
    if resident_router_bytes >= THREE_GIB as usize {
        return Err(BorsukError::RamBudgetExceeded {
            resident_bytes: resident_router_bytes as u64,
            budget_bytes: THREE_GIB - 1,
        });
    }
    Ok(NativeRouterArtifacts {
        row_codebooks,
        summary_codebooks,
        row_codes,
        summary_codes,
        page_count: reference.page_count,
        physical_rows: reference.physical_rows,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, StringArray, UInt8Array,
        UInt16Array, UInt32Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::native_ann::{NativeArtifactRef, NativeRouterRef};

    #[derive(Clone, Copy)]
    struct CodebookShape {
        field_name: &'static str,
        outer_nullable: bool,
        child_nullable: bool,
        extra_column: bool,
        invalid_kind: bool,
        nonfinite: bool,
        nonzero_padding: bool,
        omit_last: bool,
        duplicate_last: bool,
    }

    impl Default for CodebookShape {
        fn default() -> Self {
            Self {
                field_name: "centroid",
                outer_nullable: false,
                child_nullable: false,
                extra_column: false,
                invalid_kind: false,
                nonfinite: false,
                nonzero_padding: false,
                omit_last: false,
                duplicate_last: false,
            }
        }
    }

    fn parquet_bytes(batch: RecordBatch) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    fn codebook_bytes(dimensions: usize, shape: CodebookShape) -> Vec<u8> {
        let centroid_width = dimensions.div_ceil(16);
        let mut rows = Vec::new();
        for kind in ["row", "summary"] {
            for subspace in 0_u16..16 {
                for codeword in 0_u16..256 {
                    rows.push((kind, subspace, codeword));
                }
            }
        }
        if shape.omit_last {
            rows.pop();
        }
        if shape.duplicate_last {
            let replacement = rows[rows.len() - 2];
            *rows.last_mut().unwrap() = replacement;
        }

        let mut kinds = Vec::with_capacity(rows.len());
        let mut subspaces = Vec::with_capacity(rows.len());
        let mut codewords = Vec::with_capacity(rows.len());
        let mut centroids = Vec::with_capacity(rows.len() * centroid_width);
        for (row, (kind, subspace, codeword)) in rows.into_iter().enumerate() {
            kinds.push(if shape.invalid_kind && row == 0 {
                "legacy"
            } else {
                kind
            });
            subspaces.push(subspace);
            codewords.push(codeword);
            let start = usize::from(subspace) * dimensions / 16;
            let end = (usize::from(subspace) + 1) * dimensions / 16;
            let active = end - start;
            for lane in 0..centroid_width {
                let mut value = if lane < active {
                    (row * centroid_width + lane + 1) as f32 / 16_384.0
                } else {
                    0.0
                };
                if shape.nonfinite && row == 0 && lane == 0 {
                    value = f32::NAN;
                }
                if shape.nonzero_padding && row == 0 && lane == active {
                    value = 1.0;
                }
                centroids.push(value);
            }
        }
        let child = Arc::new(Field::new(
            "element",
            DataType::Float32,
            shape.child_nullable,
        ));
        let centroid = Arc::new(
            FixedSizeListArray::try_new(
                Arc::clone(&child),
                i32::try_from(centroid_width).unwrap(),
                Arc::new(Float32Array::from(centroids)),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let mut fields = vec![
            Field::new("kind", DataType::Utf8, false),
            Field::new("subspace", DataType::UInt16, false),
            Field::new("codeword", DataType::UInt16, false),
            Field::new(
                shape.field_name,
                DataType::FixedSizeList(child, i32::try_from(centroid_width).unwrap()),
                shape.outer_nullable,
            ),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(kinds)),
            Arc::new(UInt16Array::from(subspaces)),
            Arc::new(UInt16Array::from(codewords)),
            centroid,
        ];
        if shape.extra_column {
            fields.push(Field::new("legacy", DataType::UInt8, false));
            columns.push(Arc::new(UInt8Array::from(vec![0; columns[0].len()])));
        }
        parquet_bytes(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap())
    }

    fn fixed_u8_bytes(field_name: &str, width: i32, rows: usize) -> Vec<u8> {
        let child = Arc::new(Field::new("element", DataType::UInt8, false));
        let values = (0..rows * usize::try_from(width).unwrap())
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let codes = Arc::new(
            FixedSizeListArray::try_new(
                Arc::clone(&child),
                width,
                Arc::new(UInt8Array::from(values)),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        parquet_bytes(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    field_name,
                    DataType::FixedSizeList(child, width),
                    false,
                )])),
                vec![codes],
            )
            .unwrap(),
        )
    }

    fn summary_bytes(rows: &[(u32, u8)], block_type: DataType) -> Vec<u8> {
        let child = Arc::new(Field::new("element", DataType::UInt8, false));
        let codes = Arc::new(
            FixedSizeListArray::try_new(
                Arc::clone(&child),
                16,
                Arc::new(UInt8Array::from(
                    (0..rows.len() * 16)
                        .map(|value| (value % 251) as u8)
                        .collect::<Vec<_>>(),
                )),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let block: ArrayRef = match block_type {
            DataType::UInt8 => Arc::new(UInt8Array::from(
                rows.iter().map(|(_, block)| *block).collect::<Vec<_>>(),
            )),
            DataType::UInt16 => Arc::new(UInt16Array::from(
                rows.iter()
                    .map(|(_, block)| u16::from(*block))
                    .collect::<Vec<_>>(),
            )),
            other => panic!("unsupported block type {other:?}"),
        };
        parquet_bytes(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("page", DataType::UInt32, false),
                    Field::new("block", block.data_type().clone(), false),
                    Field::new("code", DataType::FixedSizeList(child, 16), false),
                ])),
                vec![
                    Arc::new(UInt32Array::from(
                        rows.iter().map(|(page, _)| *page).collect::<Vec<_>>(),
                    )),
                    block,
                    codes,
                ],
            )
            .unwrap(),
        )
    }

    fn identity(role: &str, uri: &str, bytes: &[u8]) -> NativeArtifactRef {
        NativeArtifactRef {
            role: role.to_owned(),
            uri: uri.to_owned(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            encoded_bytes: u64::try_from(bytes.len()).unwrap(),
        }
    }

    fn fixture(dimensions: usize) -> (NativeRouterRef, Vec<u8>, Vec<u8>, Vec<u8>) {
        let codebooks = codebook_bytes(dimensions, CodebookShape::default());
        let row_codes = fixed_u8_bytes("code", 16, 512);
        let summaries = summary_bytes(&[(0, 0), (0, 1), (1, 0), (1, 1)], DataType::UInt8);
        let reference = NativeRouterRef {
            pq_width: 16,
            summary_codes_per_page: 2,
            physical_rows: 512,
            page_count: 2,
            codebooks: identity(
                "router-codebooks",
                "s3://fixture/router-codebooks.parquet",
                &codebooks,
            ),
            row_codes: identity(
                "router-row-codes",
                "s3://fixture/router-row-codes.parquet",
                &row_codes,
            ),
            summary_codes: identity(
                "router-summary-codes",
                "s3://fixture/router-summary-codes.parquet",
                &summaries,
            ),
        };
        (reference, codebooks, row_codes, summaries)
    }

    #[test]
    fn native_ann_format_round_trips_97d_and_768d_router_artifacts() {
        for dimensions in [97_u32, 768] {
            let (reference, codebooks, row_codes, summaries) = fixture(dimensions as usize);
            let decoded = decode_native_router(
                &reference,
                dimensions,
                Bytes::from(codebooks),
                Bytes::from(row_codes),
                Bytes::from(summaries),
            )
            .unwrap();
            assert_eq!(decoded.physical_rows, 512);
            assert_eq!(decoded.page_count, 2);
            assert_eq!(decoded.row_codes.len(), 512 * 16);
            assert_eq!(decoded.summary_codes.len(), 4 * 16);
            assert_eq!(
                decoded.row_codebooks.len(),
                16 * 256 * dimensions.div_ceil(16) as usize
            );
            assert_eq!(decoded.summary_codebooks.len(), decoded.row_codebooks.len());
        }
    }

    #[test]
    fn native_ann_format_rejects_schema_order_content_and_identity_drift() {
        let dimensions = 97_u32;
        let (reference, codebooks, row_codes, summaries) = fixture(dimensions as usize);

        let invalid_codebooks = [
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    field_name: "vector",
                    ..CodebookShape::default()
                },
            ),
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    outer_nullable: true,
                    ..CodebookShape::default()
                },
            ),
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    child_nullable: true,
                    ..CodebookShape::default()
                },
            ),
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    extra_column: true,
                    ..CodebookShape::default()
                },
            ),
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    invalid_kind: true,
                    ..CodebookShape::default()
                },
            ),
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    nonfinite: true,
                    ..CodebookShape::default()
                },
            ),
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    nonzero_padding: true,
                    ..CodebookShape::default()
                },
            ),
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    omit_last: true,
                    ..CodebookShape::default()
                },
            ),
            codebook_bytes(
                dimensions as usize,
                CodebookShape {
                    duplicate_last: true,
                    ..CodebookShape::default()
                },
            ),
        ];
        for invalid in invalid_codebooks {
            let mut registered = reference.clone();
            registered.codebooks = identity(
                "router-codebooks",
                "s3://fixture/router-codebooks.parquet",
                &invalid,
            );
            assert!(
                decode_native_router(
                    &registered,
                    dimensions,
                    Bytes::from(invalid),
                    Bytes::copy_from_slice(&row_codes),
                    Bytes::copy_from_slice(&summaries),
                )
                .is_err()
            );
        }

        for invalid_rows in [
            fixed_u8_bytes("vector", 16, 512),
            fixed_u8_bytes("code", 8, 512),
            fixed_u8_bytes("code", 16, 511),
        ] {
            let mut registered = reference.clone();
            registered.row_codes = identity(
                "router-row-codes",
                "s3://fixture/router-row-codes.parquet",
                &invalid_rows,
            );
            assert!(
                decode_native_router(
                    &registered,
                    dimensions,
                    Bytes::copy_from_slice(&codebooks),
                    Bytes::from(invalid_rows),
                    Bytes::copy_from_slice(&summaries),
                )
                .is_err()
            );
        }

        for invalid_summaries in [
            summary_bytes(&[(0, 0), (1, 0), (0, 1), (1, 1)], DataType::UInt8),
            summary_bytes(&[(0, 0), (0, 1), (1, 0)], DataType::UInt8),
            summary_bytes(&[(0, 0), (0, 2), (1, 0), (1, 1)], DataType::UInt8),
            summary_bytes(&[(0, 0), (0, 1), (1, 0), (1, 1)], DataType::UInt16),
        ] {
            let mut registered = reference.clone();
            registered.summary_codes = identity(
                "router-summary-codes",
                "s3://fixture/router-summary-codes.parquet",
                &invalid_summaries,
            );
            assert!(
                decode_native_router(
                    &registered,
                    dimensions,
                    Bytes::copy_from_slice(&codebooks),
                    Bytes::copy_from_slice(&row_codes),
                    Bytes::from(invalid_summaries),
                )
                .is_err()
            );
        }

        let mut digest_drift = reference.clone();
        digest_drift.row_codes.sha256 = "0".repeat(64);
        assert!(
            decode_native_router(
                &digest_drift,
                dimensions,
                Bytes::copy_from_slice(&codebooks),
                Bytes::copy_from_slice(&row_codes),
                Bytes::copy_from_slice(&summaries),
            )
            .is_err()
        );
        let mut length_drift = reference.clone();
        length_drift.summary_codes.encoded_bytes += 1;
        assert!(
            decode_native_router(
                &length_drift,
                dimensions,
                Bytes::from(codebooks),
                Bytes::from(row_codes),
                Bytes::from(summaries),
            )
            .is_err()
        );
    }

    #[test]
    fn native_ann_format_100m_resident_worksheet_is_exact_and_bounded() {
        let worksheet = NativeResidentWorksheet {
            physical_rows: 100_000_000,
            page_count: 390_625,
            dimensions: 768,
            codebook_and_sq8_bytes: 4_000_000,
            mutation_entries: 1_000_000,
            resident_delta_rows: 100_000,
            response_concurrency: 16,
            response_bytes_each: 16 * 1024 * 1024,
            workspace_bytes: 128_000_000,
            runtime_reserve_bytes: 512 * 1024 * 1024,
        };

        assert_eq!(worksheet.validate_under_3_gib().unwrap(), 2_729_806_368);

        let mut oversized = worksheet;
        oversized.workspace_bytes += 512 * 1024 * 1024;
        assert!(oversized.validate_under_3_gib().is_err());
    }
}
