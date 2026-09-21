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
    native_ann::{NativeArtifactRef, NativeBoundedRouterRef, NativeRouterRef},
};

const PQ_WIDTH: usize = 16;
const CODEWORDS_PER_SUBSPACE: usize = 256;
const CODEBOOK_KINDS: usize = 2;
const SUMMARY_CODES_PER_PAGE: usize = 2;
const MUTATION_DIRECTORY_BYTES_PER_ENTRY: u64 = 96;
const DELTA_ROW_FIXED_BYTES: u64 = 72;
const THREE_GIB: u64 = 3 * 1024 * 1024 * 1024;
const BOUNDED_PQ_WIDTH: usize = 64;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeRouterArtifacts {
    pub(crate) dimensions: u32,
    pub(crate) row_codebooks: Box<[f32]>,
    pub(crate) summary_codebooks: Box<[f32]>,
    pub(crate) row_codes: Box<[u8]>,
    pub(crate) summary_codes: Box<[u8]>,
    pub(crate) page_count: u32,
    pub(crate) physical_rows: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeBoundedRouterArtifacts {
    pub(crate) dimensions: u32,
    pub(crate) summaries: Box<[f32]>,
    pub(crate) codebooks: Box<[f32]>,
    pub(crate) row_codes: Box<[u8]>,
    pub(crate) page_count: u32,
    pub(crate) physical_rows: u64,
    pub(crate) resident_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeResidentWorksheet {
    pub(crate) physical_rows: u64,
    pub(crate) page_count: u64,
    pub(crate) dimensions: u64,
    pub(crate) summary_blocks_per_page: u64,
    pub(crate) pq_width: u64,
    pub(crate) codebook_values: u64,
    pub(crate) summary_values: u64,
    pub(crate) row_code_values: u64,
    pub(crate) sq8_scalar_values: u64,
    pub(crate) mutation_entries: u64,
    pub(crate) resident_delta_rows: u64,
    pub(crate) decoded_cache_bytes: u64,
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
    pub(crate) fn validate(self, configured_budget_bytes: u64) -> Result<u64> {
        if self.physical_rows == 0
            || self.page_count == 0
            || self.dimensions == 0
            || self.summary_blocks_per_page == 0
            || self.pq_width == 0
            || self.codebook_values == 0
            || self.summary_values == 0
            || self.row_code_values == 0
            || self.sq8_scalar_values == 0
            || self.decoded_cache_bytes == 0
            || self.response_concurrency == 0
            || self.response_bytes_each == 0
            || self.runtime_reserve_bytes == 0
            || configured_budget_bytes == 0
        {
            return Err(invalid(
                "native ANN resident worksheet contains a zero bound",
            ));
        }
        let expected_pages = self.physical_rows.div_ceil(256);
        if expected_pages != self.page_count {
            return Err(invalid("native ANN resident worksheet page count differs"));
        }
        let expected_codebook_values = checked_product(
            &[
                self.pq_width,
                CODEWORDS_PER_SUBSPACE as u64,
                self.dimensions.div_ceil(self.pq_width),
            ],
            "bounded codebook values",
        )?;
        let expected_summary_values = checked_product(
            &[
                self.page_count,
                self.summary_blocks_per_page,
                self.dimensions,
            ],
            "bounded summary values",
        )?;
        let expected_row_code_values = checked_product(
            &[self.physical_rows, self.pq_width],
            "bounded row-code values",
        )?;
        if self.pq_width != BOUNDED_PQ_WIDTH as u64
            || self.codebook_values != expected_codebook_values
            || self.summary_values != expected_summary_values
            || self.row_code_values != expected_row_code_values
            || self.sq8_scalar_values != self.dimensions * 2
        {
            return Err(invalid("native ANN resident worksheet shape differs"));
        }
        let terms = [
            self.row_code_values,
            checked_product(&[self.summary_values, 4], "summary bytes")?,
            checked_product(&[self.codebook_values, 4], "codebook bytes")?,
            checked_product(&[self.sq8_scalar_values, 4], "SQ8 scalar bytes")?,
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
            self.decoded_cache_bytes,
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
        if total > configured_budget_bytes {
            return Err(BorsukError::RamBudgetExceeded {
                resident_bytes: total,
                budget_bytes: configured_budget_bytes,
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

fn bounded_codebook_schema(centroid_width: i32) -> Schema {
    Schema::new(vec![
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

fn bounded_summary_schema(dimensions: i32) -> Schema {
    Schema::new(vec![
        Field::new("page", DataType::UInt32, false),
        Field::new("block", DataType::UInt8, false),
        Field::new(
            "summary",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                dimensions,
            ),
            false,
        ),
    ])
}

fn bounded_row_code_schema() -> Schema {
    Schema::new(vec![Field::new(
        "code",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::UInt8, false)),
            BOUNDED_PQ_WIDTH as i32,
        ),
        false,
    )])
}

fn decode_bounded_codebooks(bytes: Bytes, dimensions: u32) -> Result<Box<[f32]>> {
    let dimensions = usize::try_from(dimensions)
        .map_err(|_| invalid("native bounded ANN dimensions exceed usize"))?;
    let centroid_width = dimensions.div_ceil(BOUNDED_PQ_WIDTH);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    if builder.schema().as_ref()
        != &bounded_codebook_schema(
            i32::try_from(centroid_width)
                .map_err(|_| invalid("native bounded ANN centroid width exceeds i32"))?,
        )
    {
        return Err(invalid(
            "native bounded ANN codebook physical schema differs",
        ));
    }
    let expected_rows = BOUNDED_PQ_WIDTH * CODEWORDS_PER_SUBSPACE;
    metadata_rows(&builder, expected_rows as u64, "bounded codebook")?;
    let expected_values = expected_rows
        .checked_mul(centroid_width)
        .ok_or_else(|| invalid("native bounded ANN codebook allocation overflows"))?;
    let mut values = Vec::with_capacity(expected_values);
    let mut ordinal = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 3 || batch.columns().iter().any(|array| array.null_count() != 0) {
            return Err(invalid("native bounded ANN codebook batch differs"));
        }
        let subspaces = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("native bounded ANN codebook subspace differs"))?;
        let codewords = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("native bounded ANN codebook codeword differs"))?;
        let centroids = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("native bounded ANN codebook centroid differs"))?;
        if centroids.values().null_count() != 0 {
            return Err(invalid(
                "native bounded ANN codebook centroid contains nulls",
            ));
        }
        let centroid_values = centroids
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("native bounded ANN centroid values differ"))?;
        for row in 0..batch.num_rows() {
            let expected_subspace = ordinal / CODEWORDS_PER_SUBSPACE;
            let expected_codeword = ordinal % CODEWORDS_PER_SUBSPACE;
            if usize::from(subspaces.value(row)) != expected_subspace
                || usize::from(codewords.value(row)) != expected_codeword
            {
                return Err(invalid(
                    "native bounded ANN codebook rows are not canonical",
                ));
            }
            let active = ((expected_subspace + 1) * dimensions / BOUNDED_PQ_WIDTH)
                - (expected_subspace * dimensions / BOUNDED_PQ_WIDTH);
            let start = row * centroid_width;
            let row_values = &centroid_values.values()[start..start + centroid_width];
            if row_values.iter().any(|value| !value.is_finite())
                || row_values[active..]
                    .iter()
                    .any(|value| value.to_bits() != 0)
            {
                return Err(invalid(
                    "native bounded ANN codebook values or padding differ",
                ));
            }
            values.extend_from_slice(row_values);
            ordinal += 1;
        }
    }
    if ordinal != expected_rows || values.len() != expected_values {
        return Err(invalid(
            "native bounded ANN codebook materialization differs",
        ));
    }
    Ok(values.into_boxed_slice())
}

fn decode_bounded_summaries(
    bytes: Bytes,
    dimensions: u32,
    page_count: u32,
    blocks_per_page: u8,
) -> Result<Box<[f32]>> {
    let dimensions = usize::try_from(dimensions)
        .map_err(|_| invalid("native bounded ANN dimensions exceed usize"))?;
    let expected_rows = usize::try_from(page_count)
        .ok()
        .and_then(|pages| pages.checked_mul(usize::from(blocks_per_page)))
        .ok_or_else(|| invalid("native bounded ANN summary row count overflows"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    if builder.schema().as_ref()
        != &bounded_summary_schema(
            i32::try_from(dimensions)
                .map_err(|_| invalid("native bounded ANN dimensions exceed i32"))?,
        )
    {
        return Err(invalid(
            "native bounded ANN summary physical schema differs",
        ));
    }
    metadata_rows(&builder, expected_rows as u64, "bounded summary")?;
    let expected_values = expected_rows
        .checked_mul(dimensions)
        .ok_or_else(|| invalid("native bounded ANN summary allocation overflows"))?;
    let mut values = Vec::with_capacity(expected_values);
    let mut ordinal = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 3 || batch.columns().iter().any(|array| array.null_count() != 0) {
            return Err(invalid("native bounded ANN summary batch differs"));
        }
        let pages = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("native bounded ANN summary page differs"))?;
        let blocks = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("native bounded ANN summary block differs"))?;
        let summaries = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("native bounded ANN summary vector differs"))?;
        if summaries.values().null_count() != 0 {
            return Err(invalid("native bounded ANN summary contains nulls"));
        }
        let summary_values = summaries
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("native bounded ANN summary values differ"))?;
        for row in 0..batch.num_rows() {
            if usize::try_from(pages.value(row)).ok()
                != Some(ordinal / usize::from(blocks_per_page))
                || usize::from(blocks.value(row)) != ordinal % usize::from(blocks_per_page)
            {
                return Err(invalid("native bounded ANN summary rows are not canonical"));
            }
            let start = row * dimensions;
            let row_values = &summary_values.values()[start..start + dimensions];
            if row_values.iter().any(|value| !value.is_finite()) {
                return Err(invalid("native bounded ANN summary is non-finite"));
            }
            values.extend_from_slice(row_values);
            ordinal += 1;
        }
    }
    if ordinal != expected_rows || values.len() != expected_values {
        return Err(invalid(
            "native bounded ANN summary materialization differs",
        ));
    }
    Ok(values.into_boxed_slice())
}

fn decode_bounded_row_codes(bytes: Bytes, physical_rows: u64) -> Result<Box<[u8]>> {
    let expected_rows = usize::try_from(physical_rows)
        .map_err(|_| invalid("native bounded ANN row count exceeds usize"))?;
    let expected_values = expected_rows
        .checked_mul(BOUNDED_PQ_WIDTH)
        .ok_or_else(|| invalid("native bounded ANN row-code allocation overflows"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    if builder.schema().as_ref() != &bounded_row_code_schema() {
        return Err(invalid(
            "native bounded ANN row-code physical schema differs",
        ));
    }
    metadata_rows(&builder, physical_rows, "bounded row-code")?;
    let mut values = Vec::with_capacity(expected_values);
    let mut rows = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 1 || batch.column(0).null_count() != 0 {
            return Err(invalid("native bounded ANN row-code batch differs"));
        }
        let codes = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("native bounded ANN row-code column differs"))?;
        if codes.values().null_count() != 0 {
            return Err(invalid("native bounded ANN row-code contains nulls"));
        }
        let code_values = codes
            .values()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("native bounded ANN row-code values differ"))?;
        values.extend_from_slice(code_values.values());
        rows += batch.num_rows();
    }
    if rows != expected_rows || values.len() != expected_values {
        return Err(invalid(
            "native bounded ANN row-code materialization differs",
        ));
    }
    Ok(values.into_boxed_slice())
}

pub(crate) fn decode_native_bounded_router(
    reference: &NativeBoundedRouterRef,
    dimensions: u32,
    summary_bytes: Bytes,
    codebook_bytes: Bytes,
    row_code_bytes: Bytes,
) -> Result<NativeBoundedRouterArtifacts> {
    if dimensions == 0
        || reference.pq_width != BOUNDED_PQ_WIDTH as u8
        || reference.summary_blocks_per_page == 0
        || reference.physical_rows == 0
        || reference.page_count == 0
        || reference.physical_rows.div_ceil(256) != u64::from(reference.page_count)
    {
        return Err(invalid("native bounded ANN router reference shape differs"));
    }
    authenticate(&reference.summaries, "route-summaries", &summary_bytes)?;
    authenticate(&reference.codebooks, "route-codebooks", &codebook_bytes)?;
    authenticate(&reference.row_codes, "route-row-codes", &row_code_bytes)?;
    let summaries = decode_bounded_summaries(
        summary_bytes,
        dimensions,
        reference.page_count,
        reference.summary_blocks_per_page,
    )?;
    let codebooks = decode_bounded_codebooks(codebook_bytes, dimensions)?;
    let row_codes = decode_bounded_row_codes(row_code_bytes, reference.physical_rows)?;
    let resident_bytes = u64::try_from(row_codes.len())
        .ok()
        .and_then(|bytes| {
            u64::try_from(summaries.len())
                .ok()
                .and_then(|values| values.checked_mul(4))
                .and_then(|summary_bytes| bytes.checked_add(summary_bytes))
        })
        .and_then(|bytes| {
            u64::try_from(codebooks.len())
                .ok()
                .and_then(|values| values.checked_mul(4))
                .and_then(|codebook_bytes| bytes.checked_add(codebook_bytes))
        })
        .ok_or_else(|| invalid("native bounded ANN resident bytes overflow"))?;
    let fixed_runtime = u64::from(reference.limits.range_concurrency)
        .checked_mul(reference.limits.response_bytes_each)
        .and_then(|bytes| bytes.checked_add(reference.limits.decoded_cache_bytes))
        .and_then(|bytes| bytes.checked_add(reference.limits.workspace_bytes))
        .and_then(|bytes| bytes.checked_add(reference.limits.runtime_reserve_bytes))
        .ok_or_else(|| invalid("native bounded ANN fixed runtime bytes overflow"))?;
    let admitted = resident_bytes
        .checked_add(fixed_runtime)
        .ok_or_else(|| invalid("native bounded ANN admitted bytes overflow"))?;
    if admitted > reference.limits.resident_budget_bytes {
        return Err(BorsukError::RamBudgetExceeded {
            resident_bytes: admitted,
            budget_bytes: reference.limits.resident_budget_bytes,
        });
    }
    Ok(NativeBoundedRouterArtifacts {
        dimensions,
        summaries,
        codebooks,
        row_codes,
        page_count: reference.page_count,
        physical_rows: reference.physical_rows,
        resident_bytes,
    })
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
        dimensions,
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
    use crate::native_ann::{
        NativeArtifactRef, NativeBoundedRouteLimits, NativeBoundedRouterRef, NativeRouterRef,
    };

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

    fn bounded_codebook_bytes(
        dimensions: usize,
        field_name: &str,
        outer_nullable: bool,
        child_nullable: bool,
        nonfinite: bool,
        reverse_last_two: bool,
    ) -> Vec<u8> {
        let centroid_width = dimensions.div_ceil(64);
        let mut rows = (0_u16..64)
            .flat_map(|subspace| (0_u16..256).map(move |codeword| (subspace, codeword)))
            .collect::<Vec<_>>();
        if reverse_last_two {
            let end = rows.len();
            rows.swap(end - 2, end - 1);
        }
        let mut centroids = Vec::with_capacity(rows.len() * centroid_width);
        for (row, (subspace, _)) in rows.iter().enumerate() {
            let start = usize::from(*subspace) * dimensions / 64;
            let end = (usize::from(*subspace) + 1) * dimensions / 64;
            for lane in 0..centroid_width {
                let mut value = if lane < end - start {
                    (row * centroid_width + lane + 1) as f32 / 65_536.0
                } else {
                    0.0
                };
                if nonfinite && row == 0 && lane == 0 {
                    value = f32::NAN;
                }
                centroids.push(value);
            }
        }
        let child = Arc::new(Field::new("element", DataType::Float32, child_nullable));
        let centroid = Arc::new(
            FixedSizeListArray::try_new(
                Arc::clone(&child),
                i32::try_from(centroid_width).unwrap(),
                Arc::new(Float32Array::from(centroids)),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        parquet_bytes(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("subspace", DataType::UInt16, false),
                    Field::new("codeword", DataType::UInt16, false),
                    Field::new(
                        field_name,
                        DataType::FixedSizeList(child, i32::try_from(centroid_width).unwrap()),
                        outer_nullable,
                    ),
                ])),
                vec![
                    Arc::new(UInt16Array::from(
                        rows.iter()
                            .map(|(subspace, _)| *subspace)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(UInt16Array::from(
                        rows.iter()
                            .map(|(_, codeword)| *codeword)
                            .collect::<Vec<_>>(),
                    )),
                    centroid,
                ],
            )
            .unwrap(),
        )
    }

    fn bounded_summary_bytes(
        dimensions: usize,
        field_name: &str,
        outer_nullable: bool,
        child_nullable: bool,
        nonfinite: bool,
        reordered: bool,
    ) -> Vec<u8> {
        let mut rows = vec![(0_u32, 0_u8), (0, 1), (1, 0), (1, 1)];
        if reordered {
            rows.swap(1, 2);
        }
        let mut values = (0..rows.len() * dimensions)
            .map(|ordinal| (ordinal + 1) as f32 / 8_192.0)
            .collect::<Vec<_>>();
        if nonfinite {
            values[0] = f32::INFINITY;
        }
        let child = Arc::new(Field::new("element", DataType::Float32, child_nullable));
        let summary = Arc::new(
            FixedSizeListArray::try_new(
                Arc::clone(&child),
                i32::try_from(dimensions).unwrap(),
                Arc::new(Float32Array::from(values)),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        parquet_bytes(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("page", DataType::UInt32, false),
                    Field::new("block", DataType::UInt8, false),
                    Field::new(
                        field_name,
                        DataType::FixedSizeList(child, i32::try_from(dimensions).unwrap()),
                        outer_nullable,
                    ),
                ])),
                vec![
                    Arc::new(UInt32Array::from(
                        rows.iter().map(|(page, _)| *page).collect::<Vec<_>>(),
                    )),
                    Arc::new(UInt8Array::from(
                        rows.iter().map(|(_, block)| *block).collect::<Vec<_>>(),
                    )),
                    summary,
                ],
            )
            .unwrap(),
        )
    }

    fn bounded_fixture(dimensions: usize) -> (NativeBoundedRouterRef, Vec<u8>, Vec<u8>, Vec<u8>) {
        let summaries = bounded_summary_bytes(dimensions, "summary", false, false, false, false);
        let codebooks = bounded_codebook_bytes(dimensions, "centroid", false, false, false, false);
        let row_codes = fixed_u8_bytes("code", 64, 512);
        let reference = NativeBoundedRouterRef {
            pq_width: 64,
            summary_blocks_per_page: 2,
            physical_rows: 512,
            page_count: 2,
            summaries: identity(
                "route-summaries",
                "s3://fixture/route-summaries.parquet",
                &summaries,
            ),
            codebooks: identity(
                "route-codebooks",
                "s3://fixture/route-codebooks.parquet",
                &codebooks,
            ),
            row_codes: identity(
                "route-row-codes",
                "s3://fixture/route-row-codes.parquet",
                &row_codes,
            ),
            limits: NativeBoundedRouteLimits {
                max_summary_pages: 2,
                max_candidate_rows: 128,
                max_output_pages: 2,
                coalesce_gap_pages: 1,
                range_concurrency: 2,
                response_bytes_each: 1_048_576,
                decoded_cache_bytes: 2_097_152,
                workspace_bytes: 4_194_304,
                runtime_reserve_bytes: 8_388_608,
                resident_budget_bytes: 67_108_864,
            },
        };
        (reference, summaries, codebooks, row_codes)
    }

    #[test]
    fn native_bounded_format_materializes_strict_97d_and_768d_artifacts() {
        for dimensions in [97_u32, 768] {
            let (reference, summaries, codebooks, row_codes) = bounded_fixture(dimensions as usize);
            let decoded = decode_native_bounded_router(
                &reference,
                dimensions,
                Bytes::from(summaries),
                Bytes::from(codebooks),
                Bytes::from(row_codes),
            )
            .unwrap();
            assert_eq!(decoded.physical_rows, 512);
            assert_eq!(decoded.page_count, 2);
            assert_eq!(decoded.row_codes.len(), 512 * 64);
            assert_eq!(decoded.summaries.len(), 4 * dimensions as usize);
            assert_eq!(
                decoded.codebooks.len(),
                64 * 256 * dimensions.div_ceil(64) as usize
            );
            assert!(decoded.resident_bytes <= reference.limits.resident_budget_bytes);
        }
    }

    #[test]
    fn native_bounded_format_rejects_schema_order_content_and_identity_drift() {
        let dimensions = 97_u32;
        let (reference, summaries, codebooks, row_codes) = bounded_fixture(dimensions as usize);
        let invalid_summaries = [
            bounded_summary_bytes(97, "vector", false, false, false, false),
            bounded_summary_bytes(97, "summary", true, false, false, false),
            bounded_summary_bytes(97, "summary", false, true, false, false),
            bounded_summary_bytes(97, "summary", false, false, true, false),
            bounded_summary_bytes(97, "summary", false, false, false, true),
        ];
        for bytes in invalid_summaries {
            let mut registered = reference.clone();
            registered.summaries = identity(
                "route-summaries",
                "s3://fixture/route-summaries.parquet",
                &bytes,
            );
            assert!(
                decode_native_bounded_router(
                    &registered,
                    dimensions,
                    Bytes::from(bytes),
                    Bytes::copy_from_slice(&codebooks),
                    Bytes::copy_from_slice(&row_codes),
                )
                .is_err()
            );
        }
        let invalid_codebooks = [
            bounded_codebook_bytes(97, "vector", false, false, false, false),
            bounded_codebook_bytes(97, "centroid", true, false, false, false),
            bounded_codebook_bytes(97, "centroid", false, true, false, false),
            bounded_codebook_bytes(97, "centroid", false, false, true, false),
            bounded_codebook_bytes(97, "centroid", false, false, false, true),
        ];
        for bytes in invalid_codebooks {
            let mut registered = reference.clone();
            registered.codebooks = identity(
                "route-codebooks",
                "s3://fixture/route-codebooks.parquet",
                &bytes,
            );
            assert!(
                decode_native_bounded_router(
                    &registered,
                    dimensions,
                    Bytes::copy_from_slice(&summaries),
                    Bytes::from(bytes),
                    Bytes::copy_from_slice(&row_codes),
                )
                .is_err()
            );
        }
        for bytes in [
            fixed_u8_bytes("vector", 64, 512),
            fixed_u8_bytes("code", 32, 512),
            fixed_u8_bytes("code", 64, 511),
        ] {
            let mut registered = reference.clone();
            registered.row_codes = identity(
                "route-row-codes",
                "s3://fixture/route-row-codes.parquet",
                &bytes,
            );
            assert!(
                decode_native_bounded_router(
                    &registered,
                    dimensions,
                    Bytes::copy_from_slice(&summaries),
                    Bytes::copy_from_slice(&codebooks),
                    Bytes::from(bytes),
                )
                .is_err()
            );
        }

        let mut digest_drift = reference.clone();
        digest_drift.row_codes.sha256 = "0".repeat(64);
        assert!(
            decode_native_bounded_router(
                &digest_drift,
                dimensions,
                Bytes::from(summaries),
                Bytes::from(codebooks),
                Bytes::from(row_codes),
            )
            .is_err()
        );
    }

    #[test]
    fn native_bounded_format_one_million_worksheet_is_exact_and_budgeted() {
        let worksheet = NativeResidentWorksheet {
            physical_rows: 1_000_000,
            page_count: 3_907,
            dimensions: 768,
            summary_blocks_per_page: 2,
            pq_width: 64,
            codebook_values: 64 * 256 * 12,
            summary_values: 3_907 * 2 * 768,
            row_code_values: 1_000_000 * 64,
            sq8_scalar_values: 2 * 768,
            mutation_entries: 1_000,
            resident_delta_rows: 10_000,
            decoded_cache_bytes: 64 * 1024 * 1024,
            response_concurrency: 8,
            response_bytes_each: 2 * 1024 * 1024,
            workspace_bytes: 32 * 1024 * 1024,
            runtime_reserve_bytes: 128 * 1024 * 1024,
        };
        assert_eq!(worksheet.validate(348_951_424).unwrap(), 348_951_424);
        assert!(worksheet.validate(348_951_423).is_err());
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
    fn native_bounded_format_100m_resident_worksheet_rejects_pq64_bound() {
        let worksheet = NativeResidentWorksheet {
            physical_rows: 100_000_000,
            page_count: 390_625,
            dimensions: 768,
            summary_blocks_per_page: 2,
            pq_width: 64,
            codebook_values: 64 * 256 * 12,
            summary_values: 390_625 * 2 * 768,
            row_code_values: 100_000_000 * 64,
            sq8_scalar_values: 2 * 768,
            mutation_entries: 1_000_000,
            resident_delta_rows: 100_000,
            decoded_cache_bytes: 256 * 1024 * 1024,
            response_concurrency: 16,
            response_bytes_each: 16 * 1024 * 1024,
            workspace_bytes: 128_000_000,
            runtime_reserve_bytes: 512 * 1024 * 1024,
        };
        assert!(worksheet.validate(THREE_GIB - 1).is_err());
    }
}
