//! Bounded parent-local code object contracts; not yet a serving consumer.

use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow_array::{
    Array, BinaryArray, FixedSizeListArray, Float16Array, ListArray, RecordBatch, StructArray,
    UInt32Array, UInt64Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Fields, Schema};
use half::f16;
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result, v30_s3_pq::V30PqWidth};

const MAX_ROWS: usize = 8192;
const MAX_PARENTS: usize = 32;
const MAX_RANGES: usize = 128;
const MAX_ENCODED: usize = 524288;
const FORMAT: &str = "borsuk-v32-bounded-code-object-v1";

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V32CodeRange {
    pub(crate) logical_start: u64,
    pub(crate) row_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V32ParentCodes {
    pub(crate) code_parent_ordinal: u32,
    pub(crate) centroid: [f16; 96],
    pub(crate) ranges: Vec<V32CodeRange>,
    pub(crate) high_bits: Vec<u8>,
    pub(crate) base_codes: Vec<u8>,
    pub(crate) high_codes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V32CodeObject {
    pub(crate) parents: Vec<V32ParentCodes>,
}

/// Sequential view of a validated, immutably borrowed parent's packed planes.
pub(crate) struct V32ParentCursor<'a> {
    parent: &'a V32ParentCodes,
    range: usize,
    in_range: u32,
    local_row: usize,
    base_offset: usize,
    high_offset: usize,
}

impl<'a> Iterator for V32ParentCursor<'a> {
    type Item = (u64, V30PqWidth, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let range = self.parent.ranges.get(self.range)?;
        let logical = range.logical_start + u64::from(self.in_range);
        let high = self.parent.high_bits[self.local_row / 8] & (1 << (self.local_row % 8)) != 0;
        // Construction validated all bounds and the immutable borrow keeps
        // those bounds stable. Each packed-plane offset advances only once.
        let (width, code) = if high {
            let start = self.high_offset;
            self.high_offset += 48;
            (
                V30PqWidth::High48,
                &self.parent.high_codes[start..self.high_offset],
            )
        } else {
            let start = self.base_offset;
            self.base_offset += 24;
            (
                V30PqWidth::Base24,
                &self.parent.base_codes[start..self.base_offset],
            )
        };
        self.local_row += 1;
        self.in_range += 1;
        if self.in_range == range.row_count {
            self.range += 1;
            self.in_range = 0;
        }
        Some((logical, width, code))
    }
}

impl std::iter::FusedIterator for V32ParentCursor<'_> {}

impl V32ParentCodes {
    pub(crate) fn cursor(&self) -> Result<V32ParentCursor<'_>> {
        self.validate()?;
        Ok(V32ParentCursor {
            parent: self,
            range: 0,
            in_range: 0,
            local_row: 0,
            base_offset: 0,
            high_offset: 0,
        })
    }

    fn rows(&self) -> Result<usize> {
        self.ranges.iter().try_fold(0_usize, |sum, range| {
            sum.checked_add(range.row_count as usize)
                .ok_or_else(|| invalid("V32 code row count overflows"))
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.ranges.is_empty() || self.ranges.len() > MAX_RANGES {
            return Err(invalid("V32 code range count differs"));
        }
        if self.centroid.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V32 code centroid is nonfinite"));
        }
        let mut previous_end = 0;
        for range in &self.ranges {
            if range.row_count == 0 || range.logical_start < previous_end {
                return Err(invalid("V32 code range ordering differs"));
            }
            previous_end = range
                .logical_start
                .checked_add(u64::from(range.row_count))
                .ok_or_else(|| invalid("V32 code range endpoint overflows"))?;
        }
        let rows = self.rows()?;
        if rows == 0 || rows > MAX_ROWS || self.high_bits.len() != rows.div_ceil(8) {
            return Err(invalid("V32 code population or bitmap differs"));
        }
        if rows % 8 != 0 && self.high_bits[rows / 8] >> (rows % 8) != 0 {
            return Err(invalid("V32 code bitmap padding differs"));
        }
        let high = self
            .high_bits
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum::<usize>();
        if self.base_codes.len() != (rows - high) * 24 || self.high_codes.len() != high * 48 {
            return Err(invalid("V32 packed code lengths differ"));
        }
        Ok(())
    }

    /// Checked diagnostic addressing on a validated immutable parent.
    /// Sequential scoring must use a cursor rather than repeat range scans.
    pub(crate) fn logical(&self, mut local_row: usize) -> Result<u64> {
        for range in &self.ranges {
            if local_row < range.row_count as usize {
                return range
                    .logical_start
                    .checked_add(local_row as u64)
                    .ok_or_else(|| invalid("V32 logical lookup overflows"));
            }
            local_row -= range.row_count as usize;
        }
        Err(invalid("V32 local row outside parent"))
    }

    /// Checked random lookup, not the future sequential scorer's hot path.
    pub(crate) fn code(&self, local_row: usize) -> Result<(V30PqWidth, &[u8])> {
        if local_row >= self.rows()? {
            return Err(invalid("V32 local row outside parent"));
        }
        let byte = local_row / 8;
        let bit = local_row % 8;
        let value = *self
            .high_bits
            .get(byte)
            .ok_or_else(|| invalid("V32 bitmap lookup differs"))?;
        let prefix = self
            .high_bits
            .get(..byte)
            .ok_or_else(|| invalid("V32 bitmap prefix differs"))?;
        let high_before = prefix
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum::<usize>()
            + (value & ((1_u8 << bit) - 1)).count_ones() as usize;
        let (width, rank, codes, bytes) = if value & (1_u8 << bit) != 0 {
            (V30PqWidth::High48, high_before, &self.high_codes, 48_usize)
        } else {
            (
                V30PqWidth::Base24,
                local_row
                    .checked_sub(high_before)
                    .ok_or_else(|| invalid("V32 code rank differs"))?,
                &self.base_codes,
                24_usize,
            )
        };
        let start = rank
            .checked_mul(bytes)
            .ok_or_else(|| invalid("V32 code offset overflows"))?;
        let end = start
            .checked_add(bytes)
            .ok_or_else(|| invalid("V32 code endpoint overflows"))?;
        Ok((
            width,
            codes
                .get(start..end)
                .ok_or_else(|| invalid("V32 code slice differs"))?,
        ))
    }
}

impl V32CodeObject {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.parents.is_empty() || self.parents.len() > MAX_PARENTS {
            return Err(invalid("V32 code parent count differs"));
        }
        let mut previous = None;
        let mut rows = 0;
        let mut ranges = Vec::new();
        for parent in &self.parents {
            if previous.is_some_and(|id| parent.code_parent_ordinal <= id) {
                return Err(invalid("V32 code parent ordering differs"));
            }
            previous = Some(parent.code_parent_ordinal);
            parent.validate()?;
            rows += parent.rows()?;
            ranges.extend(
                parent
                    .ranges
                    .iter()
                    .map(|r| (r.logical_start, r.logical_start + u64::from(r.row_count))),
            );
        }
        if rows > MAX_ROWS || ranges.len() > MAX_RANGES {
            return Err(invalid("V32 code object population differs"));
        }
        ranges.sort_unstable();
        if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(invalid("V32 code object ranges overlap"));
        }
        Ok(())
    }
}

fn range_fields() -> Fields {
    vec![
        Field::new("logical_start", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt32, false),
    ]
    .into()
}

fn object_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("code_parent_ordinal", DataType::UInt32, false),
            Field::new(
                "centroid",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float16, false)),
                    96,
                ),
                false,
            ),
            Field::new(
                "ranges",
                DataType::List(Arc::new(Field::new(
                    "element",
                    DataType::Struct(range_fields()),
                    false,
                ))),
                false,
            ),
            Field::new("high_bits", DataType::Binary, false),
            Field::new("base_codes", DataType::Binary, false),
            Field::new("high_codes", DataType::Binary, false),
        ],
        HashMap::from([("borsuk.format".to_owned(), FORMAT.to_owned())]),
    )
}

pub(crate) fn encode_v32_code_object(object: &V32CodeObject) -> Result<Vec<u8>> {
    object.validate()?;
    let mut offsets = vec![0_i32];
    let mut starts = Vec::new();
    let mut counts = Vec::new();
    for parent in &object.parents {
        for range in &parent.ranges {
            starts.push(range.logical_start);
            counts.push(range.row_count);
        }
        offsets
            .push(i32::try_from(starts.len()).map_err(|_| invalid("V32 range offset overflows"))?);
    }
    let ranges = StructArray::new(
        range_fields(),
        vec![
            Arc::new(UInt64Array::from(starts)),
            Arc::new(UInt32Array::from(counts)),
        ],
        None,
    );
    let ranges = ListArray::new(
        Arc::new(Field::new(
            "element",
            DataType::Struct(range_fields()),
            false,
        )),
        OffsetBuffer::new(offsets.into()),
        Arc::new(ranges),
        None,
    );
    let centroids = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float16, false)),
        96,
        Arc::new(Float16Array::from(
            object
                .parents
                .iter()
                .flat_map(|p| p.centroid)
                .collect::<Vec<_>>(),
        )),
        None,
    )?;
    let schema = object_schema();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(UInt32Array::from(
                object
                    .parents
                    .iter()
                    .map(|p| p.code_parent_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(centroids),
            Arc::new(ranges),
            Arc::new(BinaryArray::from_iter_values(
                object.parents.iter().map(|p| p.high_bits.as_slice()),
            )),
            Arc::new(BinaryArray::from_iter_values(
                object.parents.iter().map(|p| p.base_codes.as_slice()),
            )),
            Arc::new(BinaryArray::from_iter_values(
                object.parents.iter().map(|p| p.high_codes.as_slice()),
            )),
        ],
    )?;
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    if bytes.len() > MAX_ENCODED {
        return Err(invalid("V32 code object encoded size exceeds cap"));
    }
    Ok(bytes)
}

/// Check raw schema semantics before Arrow's infallible schema conversion.
/// Recursion follows only the fixed expected schema, never untrusted depth.
fn validate_ipc_field(field: arrow_ipc::Field<'_>, expected: &Field) -> Result<()> {
    if field.name() != Some(expected.name().as_str())
        || field.nullable()
        || field.dictionary().is_some()
        || field.custom_metadata().is_some_and(|m| !m.is_empty())
    {
        return Err(invalid("V32 code IPC schema field differs"));
    }
    let children: Vec<&Field> = match expected.data_type() {
        DataType::UInt32 | DataType::UInt64 => {
            let int = field
                .type_as_int()
                .ok_or_else(|| invalid("V32 code IPC integer type differs"))?;
            let width = if expected.data_type() == &DataType::UInt32 {
                32
            } else {
                64
            };
            if int.bitWidth() != width || int.is_signed() {
                return Err(invalid("V32 code IPC integer width differs"));
            }
            vec![]
        }
        DataType::Float16 => {
            if field
                .type_as_floating_point()
                .is_none_or(|f| f.precision() != arrow_ipc::Precision::HALF)
            {
                return Err(invalid("V32 code IPC float type differs"));
            }
            vec![]
        }
        DataType::Binary => {
            if field.type_as_binary().is_none() {
                return Err(invalid("V32 code IPC binary type differs"));
            }
            vec![]
        }
        DataType::FixedSizeList(child, size) => {
            if field
                .type_as_fixed_size_list()
                .is_none_or(|l| l.listSize() != *size)
            {
                return Err(invalid("V32 code IPC fixed list differs"));
            }
            vec![child.as_ref()]
        }
        DataType::List(child) => {
            if field.type_as_list().is_none() {
                return Err(invalid("V32 code IPC list type differs"));
            }
            vec![child.as_ref()]
        }
        DataType::Struct(children) => {
            if field.type_as_struct_().is_none() {
                return Err(invalid("V32 code IPC struct type differs"));
            }
            children.iter().map(AsRef::as_ref).collect()
        }
        _ => return Err(invalid("V32 code IPC unsupported expected type")),
    };
    let actual = field.children();
    if actual.map_or(0, |c| c.len()) != children.len() {
        return Err(invalid("V32 code IPC schema children differ"));
    }
    for (index, expected) in children.iter().enumerate() {
        validate_ipc_field(
            actual
                .ok_or_else(|| invalid("V32 code IPC schema children missing"))?
                .get(index),
            expected,
        )?;
    }
    Ok(())
}

fn validate_ipc_schema(schema: arrow_ipc::Schema<'_>) -> Result<()> {
    if schema.endianness() != arrow_ipc::Endianness::Little
        || schema.features().is_some_and(|f| !f.is_empty())
    {
        return Err(invalid("V32 code IPC schema features differ"));
    }
    let metadata = schema
        .custom_metadata()
        .ok_or_else(|| invalid("V32 code IPC schema metadata missing"))?;
    if metadata.len() != 1
        || metadata.get(0).key() != Some("borsuk.format")
        || metadata.get(0).value() != Some(FORMAT)
    {
        return Err(invalid("V32 code IPC schema metadata differs"));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid("V32 code IPC schema fields missing"))?;
    let expected = object_schema();
    if fields.len() != expected.fields().len() {
        return Err(invalid("V32 code IPC schema field count differs"));
    }
    for (index, field) in expected.fields().iter().enumerate() {
        validate_ipc_field(fields.get(index), field)?;
    }
    Ok(())
}

/// Validate the encoded allocation envelope before FileReader sees a batch.
fn validate_ipc_envelope(bytes: &[u8]) -> Result<()> {
    if bytes.len() < 18
        || bytes.len() > MAX_ENCODED
        || !bytes.starts_with(b"ARROW1")
        || !bytes.ends_with(b"ARROW1")
    {
        return Err(invalid("V32 code IPC magic or length differs"));
    }
    let trailer = bytes.len() - 10;
    let footer_len = u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap()) as usize;
    let footer_start = trailer
        .checked_sub(footer_len)
        .filter(|n| *n >= 8)
        .ok_or_else(|| invalid("V32 code IPC footer extent differs"))?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..trailer])
        .map_err(|_| invalid("V32 code IPC footer differs"))?;
    validate_ipc_schema(
        footer
            .schema()
            .ok_or_else(|| invalid("V32 code IPC schema missing"))?,
    )?;
    if footer.dictionaries().is_some_and(|d| !d.is_empty()) {
        return Err(invalid("V32 code IPC dictionaries forbidden"));
    }
    let blocks = footer
        .recordBatches()
        .ok_or_else(|| invalid("V32 code IPC batches missing"))?;
    if blocks.len() != 1 {
        return Err(invalid("V32 code IPC batch count differs"));
    }
    let block = blocks.get(0);
    let offset =
        usize::try_from(block.offset()).map_err(|_| invalid("V32 code IPC offset differs"))?;
    let meta_len = usize::try_from(block.metaDataLength())
        .map_err(|_| invalid("V32 code IPC metadata length differs"))?;
    let body_len = usize::try_from(block.bodyLength())
        .map_err(|_| invalid("V32 code IPC body length differs"))?;
    let body_start = offset
        .checked_add(meta_len)
        .ok_or_else(|| invalid("V32 code IPC extent overflows"))?;
    let end = body_start
        .checked_add(body_len)
        .ok_or_else(|| invalid("V32 code IPC extent overflows"))?;
    if offset < 8 || meta_len < 8 || end > footer_start {
        return Err(invalid("V32 code IPC extent differs"));
    }
    let metadata = bytes
        .get(offset..body_start)
        .ok_or_else(|| invalid("V32 code IPC metadata extent differs"))?;
    let prefix = if metadata.starts_with(&[255; 4]) {
        8
    } else {
        4
    };
    let message_len = u32::from_le_bytes(metadata[prefix - 4..prefix].try_into().unwrap()) as usize;
    let message_end = prefix
        .checked_add(message_len)
        .filter(|n| *n <= metadata.len())
        .ok_or_else(|| invalid("V32 code IPC message extent differs"))?;
    let message = arrow_ipc::root_as_message(&metadata[prefix..message_end])
        .map_err(|_| invalid("V32 code IPC message differs"))?;
    let record = message
        .header_as_record_batch()
        .ok_or_else(|| invalid("V32 code IPC record differs"))?;
    let rows = usize::try_from(record.length())
        .map_err(|_| invalid("V32 code IPC record count differs"))?;
    if !(1..=MAX_PARENTS).contains(&rows)
        || record.compression().is_some()
        || usize::try_from(message.bodyLength()).ok() != Some(body_len)
    {
        return Err(invalid("V32 code IPC record/compression differs"));
    }
    let nodes = record
        .nodes()
        .ok_or_else(|| invalid("V32 code IPC nodes missing"))?;
    if nodes.len() != 10 {
        return Err(invalid("V32 code IPC nodes count differs"));
    }
    let ranges = usize::try_from(nodes.get(4).length())
        .map_err(|_| invalid("V32 code IPC nodes range length differs"))?;
    if !(rows..=MAX_RANGES).contains(&ranges) {
        return Err(invalid("V32 code IPC nodes ranges exceed cap"));
    }
    let expected_nodes = [
        rows,
        rows,
        rows * 96,
        rows,
        ranges,
        ranges,
        ranges,
        rows,
        rows,
        rows,
    ];
    for (node, expected) in nodes.iter().zip(expected_nodes) {
        if usize::try_from(node.length()).ok() != Some(expected) || node.null_count() != 0 {
            return Err(invalid("V32 code IPC nodes shape differs"));
        }
    }
    let buffers = record
        .buffers()
        .ok_or_else(|| invalid("V32 code IPC buffers missing"))?;
    if buffers.len() != 21 {
        return Err(invalid("V32 code IPC buffers count differs"));
    }
    let body = &bytes[body_start..end];
    let mut slices = Vec::with_capacity(21);
    let mut previous_end = 0;
    for buffer in buffers {
        let start = usize::try_from(buffer.offset())
            .map_err(|_| invalid("V32 code IPC buffer offset differs"))?;
        let len = usize::try_from(buffer.length())
            .map_err(|_| invalid("V32 code IPC buffer length differs"))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| invalid("V32 code IPC buffer overflows"))?;
        if start < previous_end {
            return Err(invalid("V32 code IPC buffers overlap"));
        }
        slices.push(
            body.get(start..end)
                .ok_or_else(|| invalid("V32 code IPC buffer extent differs"))?,
        );
        previous_end = end;
    }
    for (index, count) in [
        (0, rows),
        (2, rows),
        (3, rows * 96),
        (5, rows),
        (7, ranges),
        (8, ranges),
        (10, ranges),
        (12, rows),
        (15, rows),
        (18, rows),
    ] {
        if !slices[index].is_empty() && slices[index].len() != count.div_ceil(8) {
            return Err(invalid("V32 code IPC validity length differs"));
        }
        if !slices[index].is_empty()
            && (0..count).any(|i| slices[index][i / 8] & (1 << (i % 8)) == 0)
        {
            return Err(invalid("V32 code IPC validity contradicts zero null count"));
        }
    }
    for (index, length) in [
        (1, rows * 4),
        (4, rows * 96 * 2),
        (6, (rows + 1) * 4),
        (9, ranges * 8),
        (11, ranges * 4),
        (13, (rows + 1) * 4),
        (16, (rows + 1) * 4),
        (19, (rows + 1) * 4),
    ] {
        if slices[index].len() != length {
            return Err(invalid("V32 code IPC value length differs"));
        }
    }
    if slices[14].len() > MAX_ROWS.div_ceil(8) + MAX_PARENTS - 1
        || slices[17].len() > MAX_ROWS * 24
        || slices[20].len() > MAX_ROWS * 48
    {
        return Err(invalid("V32 code IPC binary payload exceeds cap"));
    }
    for (offsets, terminal) in [
        (6, ranges),
        (13, slices[14].len()),
        (16, slices[17].len()),
        (19, slices[20].len()),
    ] {
        let mut previous = 0;
        for (i, raw) in slices[offsets].as_chunks::<4>().0.iter().enumerate() {
            let n = usize::try_from(i32::from_le_bytes(*raw))
                .map_err(|_| invalid("V32 code IPC child offset negative"))?;
            if (i == 0 && n != 0) || n < previous || n > terminal || (i == rows && n != terminal) {
                return Err(invalid("V32 code IPC child offsets differ"));
            }
            previous = n;
        }
    }
    Ok(())
}

pub(crate) fn decode_v32_code_object(
    bytes: &[u8],
    expected_sha256: &str,
    expected_bytes: usize,
) -> Result<V32CodeObject> {
    if bytes.is_empty()
        || bytes.len() > MAX_ENCODED
        || bytes.len() != expected_bytes
        || expected_sha256.len() != 64
        || !expected_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || format!("{:x}", Sha256::digest(bytes)) != expected_sha256
    {
        return Err(invalid("V32 code object byte authority differs"));
    }
    validate_ipc_envelope(bytes)?;
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &object_schema() {
        return Err(invalid("V32 code object schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V32 code object batch missing"))??;
    if reader.next().is_some() || batch.columns().iter().any(|c| c.null_count() != 0) {
        return Err(invalid("V32 code object batch differs"));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V32 parent column differs"))?;
    let centroids = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V32 centroid column differs"))?;
    let vectors = centroids
        .values()
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| invalid("V32 centroid child differs"))?;
    let ranges = batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| invalid("V32 ranges column differs"))?;
    if vectors.null_count() != 0 || ranges.values().null_count() != 0 {
        return Err(invalid("V32 nested null differs"));
    }
    let binary = |i| {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("V32 binary column differs"))
    };
    let (bits, base, high) = (binary(3)?, binary(4)?, binary(5)?);
    let mut parents = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let values = ranges.value(row);
        let fields = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| invalid("V32 range struct differs"))?;
        let starts = fields
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V32 range starts differ"))?;
        let counts = fields
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V32 range counts differ"))?;
        if starts.null_count() != 0 || counts.null_count() != 0 {
            return Err(invalid("V32 range null differs"));
        }
        parents.push(V32ParentCodes {
            code_parent_ordinal: ids.value(row),
            centroid: std::array::from_fn(|d| vectors.value(row * 96 + d)),
            ranges: (0..fields.len())
                .map(|i| V32CodeRange {
                    logical_start: starts.value(i),
                    row_count: counts.value(i),
                })
                .collect(),
            high_bits: bits.value(row).to_vec(),
            base_codes: base.value(row).to_vec(),
            high_codes: high.value(row).to_vec(),
        });
    }
    let object = V32CodeObject { parents };
    object.validate()?;
    Ok(object)
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow_array::RecordBatch;
    use arrow_ipc::{
        reader::FileReader,
        writer::{FileWriter, IpcWriteOptions},
    };
    use arrow_schema::{Field, Schema};
    use half::f16;
    use sha2::{Digest, Sha256};

    use super::{
        V32CodeObject, V32CodeRange, V32ParentCodes, decode_v32_code_object, encode_v32_code_object,
    };
    use crate::v30_s3_pq::V30PqWidth;

    fn parent() -> V32ParentCodes {
        V32ParentCodes {
            code_parent_ordinal: 0,
            centroid: [f16::ZERO; 96],
            ranges: vec![
                V32CodeRange {
                    logical_start: 10,
                    row_count: 2,
                },
                V32CodeRange {
                    logical_start: 20,
                    row_count: 2,
                },
            ],
            high_bits: vec![0b1010],
            base_codes: [vec![1; 24], vec![3; 24]].concat(),
            high_codes: [vec![2; 48], vec![4; 48]].concat(),
        }
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn v32_code_object_arrow_roundtrip_and_authentication() {
        let mut p = parent();
        p.centroid[0] = f16::from_bits(0x8000); // Negative zero must survive.
        p.centroid[1] = f16::from_bits(1); // Smallest positive subnormal.
        let expected = V32CodeObject { parents: vec![p] };
        let bytes = encode_v32_code_object(&expected).unwrap();
        let actual = decode_v32_code_object(&bytes, &digest(&bytes), bytes.len()).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            actual.parents[0].centroid.map(f16::to_bits),
            expected.parents[0].centroid.map(f16::to_bits)
        );
        assert!(decode_v32_code_object(&bytes, &"a".repeat(64), bytes.len()).is_err());
        assert!(decode_v32_code_object(&bytes, &digest(&bytes), bytes.len() + 1).is_err());
        let mut corrupted = bytes.clone();
        corrupted[8] ^= 1;
        assert!(decode_v32_code_object(&corrupted, &digest(&bytes), bytes.len()).is_err());
        assert!(decode_v32_code_object(&[], &digest(&[]), 0).is_err());
    }

    #[test]
    fn v32_code_object_arrow_maximum_shape() {
        let expected = V32CodeObject {
            parents: (0..32_u32)
                .map(|id| V32ParentCodes {
                    code_parent_ordinal: id,
                    centroid: [f16::ZERO; 96],
                    ranges: (0..4_u64)
                        .map(|r| V32CodeRange {
                            logical_start: u64::from(id) * 1024 + r * 128,
                            row_count: 64,
                        })
                        .collect(),
                    high_bits: vec![255; 32],
                    base_codes: vec![],
                    high_codes: vec![7; 256 * 48],
                })
                .collect(),
        };
        let bytes = encode_v32_code_object(&expected).unwrap();
        assert!(bytes.len() <= 524288, "encoded bytes {}", bytes.len());
        eprintln!("maximum-shape encoded bytes: {}", bytes.len());
        let decoded = decode_v32_code_object(&bytes, &digest(&bytes), bytes.len()).unwrap();
        assert_eq!(decoded, expected);
    }

    #[test]
    #[ignore = "requires independently generated Python Arrow fixture"]
    fn v32_code_object_python_interchange() {
        let directory = std::path::PathBuf::from(
            std::env::var_os("BORSUK_CODE_OBJECT_FIXTURE_DIR").expect("explicit fixture directory"),
        );
        let bytes = std::fs::read(directory.join("python.arrow")).unwrap();
        let object = decode_v32_code_object(&bytes, &digest(&bytes), bytes.len()).unwrap();
        let mut expected = parent();
        expected.code_parent_ordinal = 7;
        expected.ranges[0].logical_start = 99;
        expected.ranges[1].logical_start = 200;
        expected.centroid[0] = f16::from_bits(0x8000);
        expected.centroid[1] = f16::from_bits(1);
        assert_eq!(object.parents.len(), 1);
        assert_eq!(object.parents[0], expected);
        assert_eq!(
            object.parents[0].centroid.map(f16::to_bits),
            expected.centroid.map(f16::to_bits)
        );
        let mut output = parent();
        output.centroid = expected.centroid;
        let bytes = encode_v32_code_object(&V32CodeObject {
            parents: vec![output],
        })
        .unwrap();
        std::fs::write(directory.join("rust.arrow"), bytes).unwrap();
    }

    #[test]
    fn v32_code_object_arrow_schema_and_resource_rejections() {
        let bytes = encode_v32_code_object(&V32CodeObject {
            parents: vec![parent()],
        })
        .unwrap();
        let mut reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let schema = reader.schema();
        assert_eq!(schema.metadata().len(), 1);
        assert_eq!(
            schema.metadata().get("borsuk.format").unwrap(),
            "borsuk-v32-bounded-code-object-v1"
        );
        let write = |batch: &RecordBatch, options: IpcWriteOptions, twice: bool| {
            let mut raw = Vec::new();
            let mut writer =
                FileWriter::try_new_with_options(&mut raw, batch.schema().as_ref(), options)
                    .unwrap();
            writer.write(batch).unwrap();
            if twice {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
            drop(writer);
            raw
        };
        let mut fields = schema.fields().to_vec();
        fields[0] = Arc::new(Field::new(
            "code_parent_ordinal",
            arrow_schema::DataType::UInt32,
            true,
        ));
        let nullable = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            batch.columns().to_vec(),
        )
        .unwrap();
        let centroid = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeListArray>()
            .unwrap();
        let child = Arc::new(Field::new("element", arrow_schema::DataType::Float16, true));
        let nullable_child = arrow_array::FixedSizeListArray::try_new(
            child.clone(),
            96,
            centroid.values().clone(),
            None,
        )
        .unwrap();
        let mut nested_fields = schema.fields().to_vec();
        nested_fields[1] = Arc::new(Field::new(
            "centroid",
            arrow_schema::DataType::FixedSizeList(child, 96),
            false,
        ));
        let mut nested_columns = batch.columns().to_vec();
        nested_columns[1] = Arc::new(nullable_child);
        let nested_nullable = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                nested_fields,
                schema.metadata().clone(),
            )),
            nested_columns,
        )
        .unwrap();
        for raw in [
            write(&nullable, IpcWriteOptions::default(), false),
            write(&nested_nullable, IpcWriteOptions::default(), false),
            write(&batch, IpcWriteOptions::default(), true),
            write(
                &batch,
                IpcWriteOptions::default()
                    .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))
                    .unwrap(),
                false,
            ),
        ] {
            assert!(decode_v32_code_object(&raw, &digest(&raw), raw.len()).is_err());
        }
        for metadata in [
            std::collections::HashMap::new(),
            std::collections::HashMap::from([(
                "borsuk.format".to_owned(),
                "wrong-version".to_owned(),
            )]),
            std::collections::HashMap::from([
                (
                    "borsuk.format".to_owned(),
                    "borsuk-v32-bounded-code-object-v1".to_owned(),
                ),
                ("extra".to_owned(), "value".to_owned()),
            ]),
        ] {
            let altered = RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata)),
                batch.columns().to_vec(),
            )
            .unwrap();
            let raw = write(&altered, IpcWriteOptions::default(), false);
            assert!(decode_v32_code_object(&raw, &digest(&raw), raw.len()).is_err());
        }
        let mut extra_fields = schema.fields().to_vec();
        extra_fields.push(Arc::new(Field::new(
            "extra",
            arrow_schema::DataType::UInt32,
            false,
        )));
        let mut extra_columns = batch.columns().to_vec();
        extra_columns.push(batch.column(0).clone());
        let extra = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                extra_fields,
                schema.metadata().clone(),
            )),
            extra_columns,
        )
        .unwrap();
        let raw = write(&extra, IpcWriteOptions::default(), false);
        assert!(decode_v32_code_object(&raw, &digest(&raw), raw.len()).is_err());
        // A real dictionary file must not be admitted even with a valid digest.
        let dictionary = arrow_array::DictionaryArray::<arrow_array::types::Int32Type>::try_new(
            arrow_array::Int32Array::from(vec![0]),
            Arc::new(arrow_array::StringArray::from(vec!["value"])),
        )
        .unwrap();
        let dictionary = RecordBatch::try_from_iter(vec![(
            "dictionary",
            Arc::new(dictionary) as Arc<dyn arrow_array::Array>,
        )])
        .unwrap();
        let raw = write(&dictionary, IpcWriteOptions::default(), false);
        assert!(decode_v32_code_object(&raw, &digest(&raw), raw.len()).is_err());

        // Byte-authenticated corrupt extents must never reach allocation.
        for mutation in [0, 1, 2] {
            let mut raw = bytes.clone();
            if mutation == 0 {
                raw[0] ^= 1;
            }
            if mutation == 1 {
                let n = raw.len();
                raw[n - 10..n - 6].copy_from_slice(&u32::MAX.to_le_bytes());
            }
            if mutation == 2 {
                raw.truncate(raw.len() - 1);
            }
            assert!(decode_v32_code_object(&raw, &digest(&raw), raw.len()).is_err());
        }
        // Mutate inline FieldNode lengths without changing buffer sizes. The
        // decoder must reject before Arrow materializes the declared children.
        for (node_index, bad_length) in [(2, 3073_i64), (4, 129_i64)] {
            let mut raw = bytes.clone();
            let offset = {
                let footer_len =
                    u32::from_le_bytes(raw[raw.len() - 10..raw.len() - 6].try_into().unwrap())
                        as usize;
                let footer =
                    arrow_ipc::root_as_footer(&raw[raw.len() - 10 - footer_len..raw.len() - 10])
                        .unwrap();
                let block = footer.recordBatches().unwrap().get(0);
                let start = block.offset() as usize;
                let prefix = if raw[start..start + 4] == [255; 4] {
                    8
                } else {
                    4
                };
                let message = arrow_ipc::root_as_message(
                    &raw[start + prefix..start + block.metaDataLength() as usize],
                )
                .unwrap();
                let nodes = message.header_as_record_batch().unwrap().nodes().unwrap();
                nodes.get(node_index).0.as_ptr() as usize - raw.as_ptr() as usize
            };
            raw[offset..offset + 8].copy_from_slice(&bad_length.to_le_bytes());
            let error = decode_v32_code_object(&raw, &digest(&raw), raw.len()).unwrap_err();
            assert!(error.to_string().contains("nodes"), "wrong gate: {error}");
        }
        // Last binary buffer declares bytes beyond the authenticated body.
        let mut raw = bytes.clone();
        let offset = {
            let trailer = raw.len() - 10;
            let start = trailer
                - u32::from_le_bytes(raw[trailer..trailer + 4].try_into().unwrap()) as usize;
            let footer = arrow_ipc::root_as_footer(&raw[start..trailer]).unwrap();
            let block = footer.recordBatches().unwrap().get(0);
            let start = block.offset() as usize;
            let message = arrow_ipc::root_as_message(
                &raw[start + 8..start + block.metaDataLength() as usize],
            )
            .unwrap();
            message
                .header_as_record_batch()
                .unwrap()
                .buffers()
                .unwrap()
                .get(20)
                .0
                .as_ptr() as usize
                - raw.as_ptr() as usize
        };
        raw[offset + 8..offset + 16].copy_from_slice(&i64::MAX.to_le_bytes());
        let error = decode_v32_code_object(&raw, &digest(&raw), raw.len()).unwrap_err();
        assert!(error.to_string().contains("buffer"), "wrong gate: {error}");
    }

    #[test]
    fn v32_code_object_arrow_malformed_footer_is_error_not_panic() {
        let bytes = encode_v32_code_object(&V32CodeObject {
            parents: vec![parent()],
        })
        .unwrap();
        let trailer = bytes.len() - 10;
        let start =
            trailer - u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap()) as usize;
        let footer = arrow_ipc::root_as_footer(&bytes[start..trailer]).unwrap();
        let schema = footer.schema().unwrap();
        let fields = schema.fields().unwrap();
        // Remove optional FlatBuffer slots without corrupting record metadata.
        // Each payload is reauthenticated so it reaches semantic schema checks.
        let slots = [
            (
                "missing schema",
                footer._tab.loc(),
                arrow_ipc::Footer::VT_SCHEMA,
            ),
            (
                "missing fields",
                schema._tab.loc(),
                arrow_ipc::Schema::VT_FIELDS,
            ),
            (
                "missing list child",
                fields.get(1)._tab.loc(),
                arrow_ipc::Field::VT_CHILDREN,
            ),
        ];
        let mut cases = Vec::new();
        for (name, table, slot) in slots {
            let displacement =
                i32::from_le_bytes(bytes[start + table..start + table + 4].try_into().unwrap());
            let vtable = (table as i64 - i64::from(displacement)) as usize;
            let offset = start + vtable + usize::from(slot);
            let mut raw = bytes.clone();
            raw[offset..offset + 2].fill(0);
            cases.push((name, raw));
        }
        let int = fields.get(0).type_as_int().unwrap();
        let offset = start
            + int._tab.loc()
            + usize::from(int._tab.vtable().get(arrow_ipc::Int::VT_BITWIDTH));
        let mut raw = bytes.clone();
        raw[offset..offset + 4].copy_from_slice(&7_i32.to_le_bytes());
        cases.push(("invalid integer width", raw));
        // Valid FlatBuffer schemas with incompatible nested contracts also fail.
        let list = fields.get(1).type_as_fixed_size_list().unwrap();
        let offset = start
            + list._tab.loc()
            + usize::from(
                list._tab
                    .vtable()
                    .get(arrow_ipc::FixedSizeList::VT_LISTSIZE),
            );
        let mut raw = bytes.clone();
        raw[offset..offset + 4].copy_from_slice(&95_i32.to_le_bytes());
        cases.push(("wrong centroid dimension", raw));
        let name = fields.get(1).children().unwrap().get(0).name().unwrap();
        let offset = name.as_ptr() as usize - bytes.as_ptr() as usize;
        let mut raw = bytes.clone();
        raw[offset..offset + 7].copy_from_slice(b"invalid");
        cases.push(("wrong centroid child name", raw));
        let mut failures = Vec::new();
        for (name, raw) in cases {
            assert!(
                arrow_ipc::root_as_footer(&raw[start..trailer]).is_ok(),
                "invalid test FlatBuffer: {name}"
            );
            match std::panic::catch_unwind(|| {
                decode_v32_code_object(&raw, &digest(&raw), raw.len())
            }) {
                Ok(Err(_)) => {}
                Ok(Ok(_)) => failures.push(format!("{name}: accepted")),
                Err(_) => failures.push(format!("{name}: panicked")),
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("; "));
    }

    #[test]
    fn v32_code_object_arrow_buffer_and_validity_contract() {
        let bytes = encode_v32_code_object(&V32CodeObject {
            parents: vec![parent()],
        })
        .unwrap();
        let trailer = bytes.len() - 10;
        let start =
            trailer - u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap()) as usize;
        let footer = arrow_ipc::root_as_footer(&bytes[start..trailer]).unwrap();
        let block = footer.recordBatches().unwrap().get(0);
        let start = block.offset() as usize;
        let body = start + block.metaDataLength() as usize;
        let message = arrow_ipc::root_as_message(&bytes[start + 8..body]).unwrap();
        let record = message.header_as_record_batch().unwrap();
        let buffers = record.buffers().unwrap();
        let nodes = record.nodes().unwrap();
        let mut cases = Vec::new();
        for (name, index, component, value) in [
            ("negative buffer offset", 20, 0, -1_i64),
            ("overlapping buffers", 20, 0, 0),
            ("oversized buffer", 20, 8, i64::MAX),
            ("negative buffer length", 20, 8, -1),
        ] {
            let offset =
                buffers.get(index).0.as_ptr() as usize - bytes.as_ptr() as usize + component;
            let mut raw = bytes.clone();
            raw[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            cases.push((name, raw));
        }
        for (name, index, element, value) in [
            ("negative list offset", 6, 0, -1_i32),
            ("nonzero list start", 6, 0, 1),
            ("list terminal drift", 6, 1, 3),
            ("negative binary offset", 16, 0, -1),
            ("binary terminal drift", 16, 1, 49),
        ] {
            let offset = body + buffers.get(index).offset() as usize + element * 4;
            let mut raw = bytes.clone();
            raw[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            cases.push((name, raw));
        }
        for (name, index) in [("declared nested null", 2), ("declared range null", 5)] {
            let offset = nodes.get(index).0.as_ptr() as usize - bytes.as_ptr() as usize + 8;
            let mut raw = bytes.clone();
            raw[offset..offset + 8].copy_from_slice(&1_i64.to_le_bytes());
            cases.push((name, raw));
        }
        // The writer emits all-valid bitmaps. Clearing a used bit must be
        // rejected even when the authenticated FieldNode still says zero nulls.
        for (name, index) in [
            ("hidden parent null", 0),
            ("hidden centroid null", 3),
            ("hidden range null", 8),
        ] {
            assert!(buffers.get(index).length() > 0);
            let offset = body + buffers.get(index).offset() as usize;
            let mut raw = bytes.clone();
            raw[offset] &= !1;
            cases.push((name, raw));
        }
        let mut failures = Vec::new();
        for (name, raw) in cases {
            match std::panic::catch_unwind(|| {
                decode_v32_code_object(&raw, &digest(&raw), raw.len())
            }) {
                Ok(Err(_)) => {}
                Ok(Ok(_)) => failures.push(format!("{name}: accepted")),
                Err(_) => failures.push(format!("{name}: panicked")),
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("; "));
    }

    #[test]
    fn v32_code_object_cursor_borrows_mixed_gapped_rows() {
        let parent = parent();
        let mut cursor = parent.cursor().unwrap();
        let first = cursor.next().unwrap();
        assert_eq!(first, (10, V30PqWidth::Base24, &[1_u8; 24][..]));
        assert_eq!(first.2.as_ptr(), parent.base_codes.as_ptr());
        let high = cursor.next().unwrap();
        assert_eq!(high, (11, V30PqWidth::High48, &[2_u8; 48][..]));
        assert_eq!(high.2.as_ptr(), parent.high_codes.as_ptr());
        assert_eq!(
            cursor.next().unwrap(),
            (20, V30PqWidth::Base24, &[3_u8; 24][..])
        );
        assert_eq!(
            cursor.next().unwrap(),
            (21, V30PqWidth::High48, &[4_u8; 48][..])
        );
        assert!(cursor.next().is_none());
        assert!(cursor.next().is_none());
    }

    #[test]
    fn v32_code_object_cursor_boundaries_and_rejections() {
        let mut p = parent();
        p.ranges = vec![
            V32CodeRange {
                logical_start: 100,
                row_count: 3,
            },
            V32CodeRange {
                logical_start: 200,
                row_count: 6,
            },
        ];
        p.high_bits = vec![0xAA, 0];
        p.base_codes = [0_u8, 2, 4, 6, 8]
            .into_iter()
            .flat_map(|n| [n; 24])
            .collect();
        p.high_codes = [1_u8, 3, 5, 7].into_iter().flat_map(|n| [n; 48]).collect();
        let expected_logical = [100, 101, 102, 200, 201, 202, 203, 204, 205];
        let rows = p.cursor().unwrap().collect::<Vec<_>>();
        assert_eq!(rows.len(), 9);
        for (i, (logical, width, code)) in rows.iter().enumerate() {
            assert_eq!(*logical, expected_logical[i]);
            assert_eq!(
                *width,
                if i % 2 == 0 {
                    V30PqWidth::Base24
                } else {
                    V30PqWidth::High48
                }
            );
            assert!(code.iter().all(|n| usize::from(*n) == i));
            assert_eq!(code.len(), if i % 2 == 0 { 24 } else { 48 });
        }
        let mut bad = p.clone();
        bad.ranges.clear();
        assert!(bad.cursor().is_err());
        let mut bad = p.clone();
        bad.base_codes.pop();
        assert!(bad.cursor().is_err());
        let mut bad = p.clone();
        bad.high_bits[1] = 0x80;
        assert!(bad.cursor().is_err());
        for high in [false, true] {
            let mut p = parent();
            p.ranges = vec![V32CodeRange {
                logical_start: 100,
                row_count: 8192,
            }];
            p.high_bits = vec![if high { 255 } else { 0 }; 1024];
            p.base_codes = if high { vec![] } else { vec![3; 8192 * 24] };
            p.high_codes = if high { vec![7; 8192 * 48] } else { vec![] };
            let mut cursor = p.cursor().unwrap();
            assert_eq!(cursor.by_ref().take(8191).count(), 8191);
            let last = cursor.next().unwrap();
            assert_eq!(last.0, 8291);
            assert_eq!(
                last.2,
                if high {
                    &[7_u8; 48][..]
                } else {
                    &[3_u8; 24][..]
                }
            );
            assert!(cursor.next().is_none());
        }
    }

    #[test]
    fn v32_code_object_parent_local_addressing() {
        let parent = parent();
        parent.validate().unwrap();
        assert_eq!(parent.logical(0).unwrap(), 10);
        assert_eq!(parent.logical(1).unwrap(), 11);
        assert_eq!(parent.logical(2).unwrap(), 20);
        assert_eq!(parent.logical(3).unwrap(), 21);
        assert_eq!(
            parent.code(0).unwrap(),
            (V30PqWidth::Base24, &[1_u8; 24][..])
        );
        assert_eq!(
            parent.code(1).unwrap(),
            (V30PqWidth::High48, &[2_u8; 48][..])
        );
        assert_eq!(
            parent.code(2).unwrap(),
            (V30PqWidth::Base24, &[3_u8; 24][..])
        );
        assert_eq!(
            parent.code(3).unwrap(),
            (V30PqWidth::High48, &[4_u8; 48][..])
        );
        assert!(parent.code(4).is_err());
        assert!(parent.logical(4).is_err());
        V32CodeObject {
            parents: vec![parent],
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn v32_code_object_invariant_rejections() {
        let baseline = parent();
        baseline.validate().unwrap();
        type Mutation = (&'static str, Box<dyn Fn(&mut V32ParentCodes)>);
        let mutations: Vec<Mutation> = vec![
            ("empty ranges", Box::new(|p| p.ranges.clear())),
            ("zero range", Box::new(|p| p.ranges[0].row_count = 0)),
            (
                "overflow",
                Box::new(|p| p.ranges[1].logical_start = u64::MAX),
            ),
            ("overlap", Box::new(|p| p.ranges[1].logical_start = 11)),
            ("order", Box::new(|p| p.ranges.swap(0, 1))),
            ("nonfinite", Box::new(|p| p.centroid[0] = f16::NAN)),
            ("padding", Box::new(|p| p.high_bits[0] |= 0b1000_0000)),
            ("short bitmap", Box::new(|p| p.high_bits.clear())),
            ("extra bitmap", Box::new(|p| p.high_bits.push(0))),
            (
                "short base",
                Box::new(|p| {
                    p.base_codes.pop();
                }),
            ),
            ("extra base", Box::new(|p| p.base_codes.push(0))),
            (
                "short high",
                Box::new(|p| {
                    p.high_codes.pop();
                }),
            ),
            ("extra high", Box::new(|p| p.high_codes.push(0))),
        ];
        for (name, mutate) in mutations {
            let mut bad = baseline.clone();
            mutate(&mut bad);
            assert!(bad.validate().is_err(), "accepted {name}");
        }
        assert!(V32CodeObject { parents: vec![] }.validate().is_err());
        assert!(
            V32CodeObject {
                parents: vec![baseline.clone(), baseline.clone()]
            }
            .validate()
            .is_err()
        );
        let mut overlap = baseline.clone();
        overlap.code_parent_ordinal = 1;
        assert!(
            V32CodeObject {
                parents: vec![baseline, overlap]
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn v32_code_object_exact_population_caps() {
        fn all_base(id: u32, ranges: Vec<V32CodeRange>) -> V32ParentCodes {
            let rows: usize = ranges.iter().map(|r| r.row_count as usize).sum();
            V32ParentCodes {
                code_parent_ordinal: id,
                centroid: [f16::ZERO; 96],
                ranges,
                high_bits: vec![0; rows.div_ceil(8)],
                base_codes: vec![0; rows * 24],
                high_codes: vec![],
            }
        }
        let maximum = V32CodeObject {
            parents: (0..32)
                .map(|id| {
                    all_base(
                        id,
                        (0..4)
                            .map(|range| V32CodeRange {
                                logical_start: u64::from(id) * 4096 + range * 128,
                                row_count: 64,
                            })
                            .collect(),
                    )
                })
                .collect(),
        };
        maximum.validate().unwrap(); // 32 parents,128 ranges,8192 rows.

        let maximum_parent = all_base(
            0,
            vec![V32CodeRange {
                logical_start: 0,
                row_count: 8192,
            }],
        );
        maximum_parent.validate().unwrap();
        assert_eq!(maximum_parent.logical(8191).unwrap(), 8191);
        assert_eq!(maximum_parent.code(8191).unwrap().1.len(), 24);
        assert!(
            all_base(
                0,
                vec![V32CodeRange {
                    logical_start: 0,
                    row_count: 8193
                }]
            )
            .validate()
            .is_err()
        );
        assert!(
            V32CodeObject {
                parents: vec![
                    maximum_parent,
                    all_base(
                        1,
                        vec![V32CodeRange {
                            logical_start: 9000,
                            row_count: 1
                        }]
                    )
                ]
            }
            .validate()
            .is_err()
        );
        assert!(
            V32CodeObject {
                parents: (0..33)
                    .map(|id| all_base(
                        id,
                        vec![V32CodeRange {
                            logical_start: u64::from(id),
                            row_count: 1
                        }]
                    ))
                    .collect()
            }
            .validate()
            .is_err()
        );
        assert!(
            all_base(
                0,
                (0..129)
                    .map(|id| V32CodeRange {
                        logical_start: id * 2,
                        row_count: 1,
                    })
                    .collect()
            )
            .validate()
            .is_err()
        );
        let mut reversed = maximum;
        reversed.parents.swap(0, 1);
        assert!(reversed.validate().is_err());
    }
}
