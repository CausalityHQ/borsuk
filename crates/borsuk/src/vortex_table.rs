use std::collections::BTreeMap;
use std::fmt::Display;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use futures_util::{FutureExt, future::BoxFuture};
use vortex::VortexSessionDefault;
use vortex::array::arrays::ChunkedArray;
use vortex::array::buffer::BufferHandle;
use vortex::array::expr::{Expression, select};
use vortex::array::stream::ArrayStreamExt;
use vortex::array::{IntoArray, VortexSessionExecute};
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::{Alignment, Buffer, ByteBuffer, ByteBufferMut};
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex::io::VortexReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use crate::{BorsukError, PhysicalLayoutRef, Result, storage::Storage};

#[derive(Clone)]
pub(crate) struct StorageVortexReadAt {
    storage: Storage,
    path: String,
    uri: Arc<str>,
    size: u64,
    layout: PhysicalLayoutRef,
    verified_chunks: Arc<Mutex<BTreeMap<usize, Arc<Vec<u8>>>>>,
    chunk_locks: Arc<Mutex<BTreeMap<usize, Arc<Mutex<()>>>>>,
}

impl StorageVortexReadAt {
    pub(crate) fn new(
        storage: Storage,
        path: impl Into<String>,
        size: u64,
        layout: PhysicalLayoutRef,
    ) -> Self {
        let path = path.into();
        Self {
            storage,
            uri: Arc::from(path.as_str()),
            path,
            size,
            layout,
            verified_chunks: Arc::new(Mutex::new(BTreeMap::new())),
            chunk_locks: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn read_verified_range(&self, offset: u64, end: u64) -> Result<Vec<u8>> {
        if offset == end {
            return Ok(Vec::new());
        }
        let chunk_bytes = u64::try_from(self.layout.integrity_chunk_bytes).map_err(|_| {
            BorsukError::InvalidStorage("Vortex integrity chunk width exceeds u64".to_string())
        })?;
        if chunk_bytes == 0 || self.layout.integrity_checksums.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "Vortex object reference has no range integrity".to_string(),
            ));
        }
        let expected_chunks = self.size.div_ceil(chunk_bytes);
        if usize::try_from(expected_chunks).ok() != Some(self.layout.integrity_checksums.len()) {
            return Err(BorsukError::InvalidStorage(
                "Vortex object size disagrees with its range integrity".to_string(),
            ));
        }
        let first_chunk = usize::try_from(offset / chunk_bytes).map_err(|_| {
            BorsukError::InvalidStorage("Vortex range chunk index exceeds usize".to_string())
        })?;
        let last_chunk = usize::try_from(end.saturating_sub(1) / chunk_bytes).map_err(|_| {
            BorsukError::InvalidStorage("Vortex range chunk index exceeds usize".to_string())
        })?;
        let requested_bytes = usize::try_from(end.saturating_sub(offset)).map_err(|_| {
            BorsukError::InvalidStorage("Vortex requested range exceeds usize".to_string())
        })?;
        let mut selected = Vec::with_capacity(requested_bytes);
        for chunk_index in first_chunk..=last_chunk {
            let cached = self
                .verified_chunks
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&chunk_index)
                .cloned();
            let chunk = match cached {
                Some(chunk) => chunk,
                None => {
                    // Vortex can issue several concurrent reads into one
                    // integrity chunk. Serialize only contenders for that
                    // chunk, then recheck the verified cache; different chunks
                    // retain full I/O concurrency.
                    let chunk_lock = self
                        .chunk_locks
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .entry(chunk_index)
                        .or_insert_with(|| Arc::new(Mutex::new(())))
                        .clone();
                    let _chunk_guard = chunk_lock.lock().unwrap_or_else(|error| error.into_inner());
                    if let Some(chunk) = self
                        .verified_chunks
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .get(&chunk_index)
                        .cloned()
                    {
                        chunk
                    } else {
                        let chunk_index_u64 = u64::try_from(chunk_index).map_err(|_| {
                            BorsukError::InvalidStorage(
                                "Vortex integrity chunk index exceeds u64".to_string(),
                            )
                        })?;
                        let chunk_start =
                            chunk_index_u64.checked_mul(chunk_bytes).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "Vortex integrity chunk range overflows".to_string(),
                                )
                            })?;
                        let chunk_end = chunk_start.saturating_add(chunk_bytes).min(self.size);
                        let chunk_range = chunk_start..chunk_end;
                        let mut bytes = self.storage.read_range(&self.path, chunk_range.clone())?;
                        if let Err(error) =
                            self.layout
                                .verify_integrity_chunk(&self.path, chunk_index, &bytes)
                        {
                            if !matches!(error, BorsukError::ChecksumMismatch { .. }) {
                                return Err(error);
                            }
                            self.storage
                                .evict_cached_range(&self.path, chunk_range.clone())?;
                            bytes = self.storage.read_range(&self.path, chunk_range)?;
                            self.layout
                                .verify_integrity_chunk(&self.path, chunk_index, &bytes)?;
                        }
                        let bytes = Arc::new(bytes);
                        self.verified_chunks
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .insert(chunk_index, Arc::clone(&bytes));
                        bytes
                    }
                }
            };
            let chunk_index_u64 = u64::try_from(chunk_index).map_err(|_| {
                BorsukError::InvalidStorage("Vortex integrity chunk index exceeds u64".to_string())
            })?;
            let chunk_start = chunk_index_u64 * chunk_bytes;
            let take_start = usize::try_from(offset.max(chunk_start).saturating_sub(chunk_start))
                .map_err(|_| {
                BorsukError::InvalidStorage("Vortex range start exceeds usize".to_string())
            })?;
            let take_end = end
                .min(chunk_start.saturating_add(chunk.len() as u64))
                .saturating_sub(chunk_start);
            let take_end = usize::try_from(take_end).map_err(|_| {
                BorsukError::InvalidStorage("Vortex range end exceeds usize".to_string())
            })?;
            selected.extend_from_slice(chunk.get(take_start..take_end).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "Vortex verified chunk does not cover the requested range".to_string(),
                )
            })?);
        }
        Ok(selected)
    }
}

impl VortexReadAt for StorageVortexReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        Some(&self.uri)
    }

    fn concurrency(&self) -> usize {
        crate::configured_io_threads().min(32)
    }

    fn size(&self) -> BoxFuture<'static, vortex::error::VortexResult<u64>> {
        let size = self.size;
        async move { Ok(size) }.boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, vortex::error::VortexResult<BufferHandle>> {
        let reader = self.clone();
        let size = self.size;
        async move {
            let length_u64 = u64::try_from(length).map_err(|error| {
                vortex::error::VortexError::from(io::Error::new(io::ErrorKind::InvalidInput, error))
            })?;
            let end = offset.checked_add(length_u64).ok_or_else(|| {
                vortex::error::VortexError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Vortex object-store range overflows",
                ))
            })?;
            if end > size {
                return Err(vortex::error::VortexError::from(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Vortex range {offset}..{end} exceeds {size} bytes"),
                )));
            }
            let bytes =
                tokio::task::spawn_blocking(move || reader.read_verified_range(offset, end))
                    .await
                    .map_err(|error| {
                        vortex::error::VortexError::from(io::Error::other(format!(
                            "Vortex range task failed: {error}"
                        )))
                    })?
                    .map_err(|error| {
                        vortex::error::VortexError::from(io::Error::other(error.to_string()))
                    })?;
            Ok(BufferHandle::new_host(
                ByteBuffer::from(bytes).aligned(alignment),
            ))
        }
        .boxed()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum VortexLayout {
    Default,
    Compact,
}

#[derive(Default)]
pub(crate) struct VortexScanOptions {
    projection: Option<Vec<String>>,
    filter: Option<Expression>,
    row_range: Option<Range<u64>>,
    row_indices: Option<Vec<u64>>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl VortexScanOptions {
    pub(crate) fn with_projection<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.projection = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    pub(crate) fn with_filter(mut self, filter: Expression) -> Self {
        self.filter = Some(filter);
        self
    }

    pub(crate) fn with_row_range(mut self, row_range: Range<u64>) -> Self {
        self.row_range = Some(row_range);
        self
    }

    pub(crate) fn with_row_indices<I>(mut self, row_indices: I) -> Self
    where
        I: IntoIterator<Item = u64>,
    {
        self.row_indices = Some(row_indices.into_iter().collect());
        self
    }
}

pub(crate) async fn write_vortex_table(
    batches: &[RecordBatch],
    layout: VortexLayout,
) -> Result<Vec<u8>> {
    let Some(first) = batches.first() else {
        return Err(BorsukError::InvalidStorage(
            "Vortex table requires at least one RecordBatch".to_string(),
        ));
    };
    let schema = first.schema();
    if batches.iter().any(|batch| batch.schema() != schema) {
        return Err(BorsukError::InvalidStorage(
            "Vortex table RecordBatch schemas must match exactly".to_string(),
        ));
    }

    let session = VortexSession::default().with_tokio();
    let chunks = batches
        .iter()
        .cloned()
        .map(|batch| {
            session
                .arrow()
                .from_arrow_record_batch(batch, schema.as_ref())
                .map_err(vortex_error)
        })
        .collect::<Result<Vec<_>>>()?;
    let dtype = chunks[0].dtype().clone();
    let array = ChunkedArray::try_new(chunks, dtype)
        .map_err(vortex_error)?
        .into_array();
    let writer = match layout {
        VortexLayout::Default => session.write_options(),
        VortexLayout::Compact => session.write_options().with_strategy(
            WriteStrategyBuilder::default()
                .with_btrblocks_builder(BtrBlocksCompressorBuilder::default().with_compact())
                .build(),
        ),
    };
    let mut output = ByteBufferMut::empty();
    writer
        .write(&mut output, array.to_array_stream())
        .await
        .map_err(vortex_error)?;
    Ok(output.to_vec())
}

pub(crate) async fn read_vortex_table(
    bytes: Vec<u8>,
    options: VortexScanOptions,
) -> Result<RecordBatch> {
    read_vortex_buffer(ByteBuffer::from(bytes), options).await
}

/// Run the header and row projections over one shared immutable source buffer.
///
/// Normal segments store one packed header value in row zero. Reading that row
/// separately lets the serving path materialize the constants once while the
/// full-row scan projects only row-varying columns. `ByteBuffer` clones share
/// the owned source allocation, so this does not duplicate the compressed
/// segment payload.
pub(crate) async fn read_vortex_table_pair(
    bytes: Vec<u8>,
    first: VortexScanOptions,
    second: VortexScanOptions,
) -> Result<(RecordBatch, RecordBatch)> {
    let buffer = ByteBuffer::from(bytes);
    let first_batch = read_vortex_buffer(buffer.clone(), first).await?;
    let second_batch = read_vortex_buffer(buffer, second).await?;
    Ok((first_batch, second_batch))
}

pub(crate) async fn read_vortex_storage_pair(
    reader: StorageVortexReadAt,
    first: VortexScanOptions,
    second: VortexScanOptions,
) -> Result<(RecordBatch, RecordBatch)> {
    let session = VortexSession::default().with_tokio();
    let file = session
        .open_options()
        .open_read(reader)
        .await
        .map_err(vortex_error)?;
    let first_batch = scan_vortex_file(&session, file.clone(), first).await?;
    let second_batch = scan_vortex_file(&session, file, second).await?;
    Ok((first_batch, second_batch))
}

async fn read_vortex_buffer(buffer: ByteBuffer, options: VortexScanOptions) -> Result<RecordBatch> {
    let session = VortexSession::default().with_tokio();
    let file = session
        .open_options()
        .open_buffer(buffer)
        .map_err(vortex_error)?;
    scan_vortex_file(&session, file, options).await
}

async fn scan_vortex_file(
    session: &VortexSession,
    file: vortex::file::VortexFile,
    options: VortexScanOptions,
) -> Result<RecordBatch> {
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(columns) = options.projection {
        let source_schema = session
            .arrow()
            .to_arrow_schema(&scan.dtype().map_err(vortex_error)?)
            .map_err(vortex_error)?;
        let columns = columns
            .into_iter()
            .filter(|column| source_schema.index_of(column).is_ok())
            .map(Arc::<str>::from)
            .collect::<Vec<_>>();
        scan = scan.with_projection(select(columns, vortex::expr::root()));
    }
    if let Some(filter) = options.filter {
        scan = scan.with_filter(filter);
    }
    if let Some(row_range) = options.row_range {
        scan = scan.with_row_range(row_range);
    }
    if let Some(row_indices) = options.row_indices {
        scan = scan.with_row_indices(Buffer::from_iter(row_indices));
    }

    let schema = materialized_arrow_schema(
        &session
            .arrow()
            .to_arrow_schema(&scan.dtype().map_err(vortex_error)?)
            .map_err(vortex_error)?,
    );
    let array = scan
        .into_array_stream()
        .map_err(vortex_error)?
        .read_all()
        .await
        .map_err(vortex_error)?;
    let target = Field::new("", DataType::Struct(schema.fields().clone()), false);
    let mut context = session.create_execution_ctx();
    let arrow = session
        .arrow()
        .execute_arrow(array, Some(&target), &mut context)
        .map_err(vortex_error)?;
    Ok(RecordBatch::from(arrow.as_struct()))
}

fn materialized_arrow_schema(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| Arc::new(materialized_arrow_field(field)))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

fn materialized_arrow_field(field: &FieldRef) -> Field {
    Field::new(
        field.name(),
        materialized_arrow_type(field.data_type()),
        field.is_nullable(),
    )
    .with_metadata(field.metadata().clone())
}

fn materialized_arrow_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::ListView(field) => DataType::List(Arc::new(materialized_arrow_field(field))),
        DataType::LargeListView(field) => {
            DataType::LargeList(Arc::new(materialized_arrow_field(field)))
        }
        DataType::List(field) => DataType::List(Arc::new(materialized_arrow_field(field))),
        DataType::LargeList(field) => {
            DataType::LargeList(Arc::new(materialized_arrow_field(field)))
        }
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(materialized_arrow_field(field)), *size)
        }
        DataType::Struct(fields) => DataType::Struct(Fields::from_iter(
            fields
                .iter()
                .map(|field| Arc::new(materialized_arrow_field(field))),
        )),
        other => other.clone(),
    }
}

fn vortex_error(error: impl Display) -> BorsukError {
    BorsukError::InvalidStorage(format!("Vortex table error: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use arrow_array::types::{Float32Type, UInt8Type, UInt32Type};
    use arrow_array::{
        Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array,
        Float32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use object_store::{ObjectStore, memory::InMemory};
    use vortex::expr::{eq, get_item, lit, root};

    use super::{
        StorageVortexReadAt, VortexLayout, VortexScanOptions, read_vortex_storage_pair,
        read_vortex_table, read_vortex_table_pair, write_vortex_table,
    };
    use crate::{PhysicalLayoutRef, storage::Storage};

    fn standard_batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "row_id",
                Arc::new(UInt64Array::from_iter_values(0..8)) as Arc<dyn Array>,
            ),
            (
                "tenant",
                Arc::new(UInt32Array::from_iter_values([7, 8, 7, 9, 7, 8, 9, 7])) as Arc<dyn Array>,
            ),
            (
                "score",
                Arc::new(Float32Array::from_iter_values([
                    0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5,
                ])) as Arc<dyn Array>,
            ),
            (
                "active",
                Arc::new(BooleanArray::from(vec![
                    true, false, true, true, false, true, false, true,
                ])) as Arc<dyn Array>,
            ),
        ])
        .unwrap()
    }

    async fn encoded_standard_batch() -> Vec<u8> {
        write_vortex_table(&[standard_batch()], VortexLayout::Default)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn standard_record_batch_round_trips_in_both_layouts() {
        let expected = standard_batch();
        for layout in [VortexLayout::Default, VortexLayout::Compact] {
            let bytes = write_vortex_table(std::slice::from_ref(&expected), layout)
                .await
                .unwrap();
            let actual = read_vortex_table(bytes, VortexScanOptions::default())
                .await
                .unwrap();
            assert_eq!(actual, expected);
        }
    }

    /// Research-only size decomposition for an exact WAL object captured by a
    /// frozen qualification campaign. The input path and its checksum belong
    /// in the campaign notes; keeping the probe ignored prevents external
    /// fixture availability from affecting the normal test suite.
    #[tokio::test]
    #[ignore = "requires BORSUK_VORTEX_LAYOUT_PROBE_OBJECT"]
    async fn captured_wal_object_reports_layout_and_column_sizes() {
        let path = std::env::var("BORSUK_VORTEX_LAYOUT_PROBE_OBJECT")
            .expect("set BORSUK_VORTEX_LAYOUT_PROBE_OBJECT");
        let captured = std::fs::read(&path).unwrap();
        let batch = read_vortex_table(captured.clone(), VortexScanOptions::default())
            .await
            .unwrap();

        println!(
            "scope,name,rows,columns,bytes\ncaptured,full,{},{},{}",
            batch.num_rows(),
            batch.num_columns(),
            captured.len()
        );
        for layout in [VortexLayout::Default, VortexLayout::Compact] {
            let full = write_vortex_table(std::slice::from_ref(&batch), layout)
                .await
                .unwrap();
            println!(
                "{layout:?},full,{},{},{}",
                batch.num_rows(),
                batch.num_columns(),
                full.len()
            );
            for (column, field) in batch.schema().fields().iter().enumerate() {
                let projected = batch.project(&[column]).unwrap();
                let bytes = write_vortex_table(&[projected], layout).await.unwrap();
                println!(
                    "{layout:?},{},{},{},{}",
                    field.name(),
                    batch.num_rows(),
                    1,
                    bytes.len()
                );
            }
        }

        if let Ok(vector_column) = batch.schema().index_of("vector")
            && let Some(vectors) = batch
                .column(vector_column)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
            && vectors.value_type() == DataType::Float32
        {
            let encoded = (0..vectors.len())
                .map(|row| {
                    if vectors.is_null(row) {
                        return None;
                    }
                    let values = vectors.value(row);
                    let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
                    Some(
                        values
                            .values()
                            .iter()
                            .flat_map(|value| value.to_le_bytes())
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>();
            let mut fields = batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            fields[vector_column] = Field::new("vector", DataType::Binary, true);
            let mut columns = batch.columns().to_vec();
            columns[vector_column] = Arc::new(BinaryArray::from_iter(
                encoded.iter().map(|value| value.as_deref()),
            ));
            let binary_vector_batch =
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let bytes = write_vortex_table(&[binary_vector_batch], VortexLayout::Compact)
                .await
                .unwrap();
            println!(
                "CompactBinaryVector,full,{},{},{}",
                batch.num_rows(),
                batch.num_columns(),
                bytes.len()
            );
        }
    }

    #[tokio::test]
    async fn projection_returns_only_requested_columns() {
        let bytes = encoded_standard_batch().await;
        let actual = read_vortex_table(
            bytes,
            VortexScanOptions::default().with_projection(["score", "row_id"]),
        )
        .await
        .unwrap();
        let expected = standard_batch().project(&[2, 0]).unwrap();

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn projection_skips_optional_columns_absent_from_the_table() {
        let bytes = encoded_standard_batch().await;
        let actual = read_vortex_table(
            bytes,
            VortexScanOptions::default().with_projection(["missing_optional", "score"]),
        )
        .await
        .unwrap();

        assert_eq!(actual, standard_batch().project(&[2]).unwrap());
    }

    #[tokio::test]
    async fn paired_projection_reuses_one_owned_source_buffer() {
        let bytes = encoded_standard_batch().await;
        let (header, rows) = read_vortex_table_pair(
            bytes,
            VortexScanOptions::default()
                .with_projection(["tenant"])
                .with_row_range(0..1),
            VortexScanOptions::default().with_projection(["score", "row_id"]),
        )
        .await
        .unwrap();

        assert_eq!(header, standard_batch().project(&[1]).unwrap().slice(0, 1));
        assert_eq!(rows, standard_batch().project(&[2, 0]).unwrap());
    }

    #[tokio::test]
    async fn row_range_returns_contiguous_rows() {
        let bytes = encoded_standard_batch().await;
        let actual = read_vortex_table(bytes, VortexScanOptions::default().with_row_range(2..6))
            .await
            .unwrap();

        assert_eq!(actual, standard_batch().slice(2, 4));
    }

    #[tokio::test]
    async fn point_lookup_uses_native_row_indices() {
        let bytes = encoded_standard_batch().await;
        let actual = read_vortex_table(bytes, VortexScanOptions::default().with_row_indices([6]))
            .await
            .unwrap();

        assert_eq!(actual, standard_batch().slice(6, 1));
    }

    #[test]
    fn object_store_reader_projects_and_takes_without_fetching_full_object() {
        let rows = 4_096_u64;
        let payloads = (0..rows)
            .map(|row| {
                (0..1_024_u64)
                    .map(|byte| {
                        row.wrapping_mul(131)
                            .wrapping_add(byte.wrapping_mul(17))
                            .wrapping_add(row.rotate_left((byte % 63) as u32))
                            as u8
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter([
            (
                "row_id",
                Arc::new(UInt64Array::from_iter_values(0..rows)) as Arc<dyn Array>,
            ),
            (
                "payload",
                Arc::new(BinaryArray::from_iter_values(
                    payloads.iter().map(Vec::as_slice),
                )) as Arc<dyn Array>,
            ),
        ])
        .unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let bytes = runtime
            .block_on(write_vortex_table(&[batch], VortexLayout::Default))
            .unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage =
            Storage::from_object_store("memory:///vortex-ranges".to_string(), store).unwrap();
        storage
            .write_bytes("segments/ranged.vortex", &bytes)
            .unwrap();
        let before = storage.cache_read_counts();
        let layout = PhysicalLayoutRef::resolve(
            &crate::PhysicalLayoutPolicy::production_baseline().with_role_format(
                crate::PhysicalObjectRole::NormalSegment,
                crate::PhysicalFormat::Vortex,
            ),
            crate::PhysicalObjectRole::NormalSegment,
            crate::PhysicalLayoutContext {
                rows: usize::try_from(rows).unwrap(),
                ..crate::PhysicalLayoutContext::default()
            },
        )
        .unwrap()
        .with_integrity(&bytes);
        let reader = StorageVortexReadAt::new(
            storage.clone(),
            "segments/ranged.vortex",
            bytes.len() as u64,
            layout,
        );

        let (range, points) = runtime
            .block_on(read_vortex_storage_pair(
                reader,
                VortexScanOptions::default()
                    .with_projection(["row_id"])
                    .with_row_range(100..110),
                VortexScanOptions::default()
                    .with_projection(["row_id"])
                    .with_row_indices([3, 2_047, 4_095]),
            ))
            .unwrap();

        assert_eq!(
            range
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &(100..110).collect::<Vec<_>>()
        );
        assert_eq!(
            points
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &[3, 2_047, 4_095]
        );
        let fetched = storage.cache_read_counts().delta(&before).backing_bytes;
        assert!(fetched > 0);
        assert!(
            fetched < bytes.len() as u64,
            "range-aware reader fetched {fetched} of {} bytes",
            bytes.len()
        );
    }

    #[test]
    fn concurrent_ranges_singleflight_the_same_integrity_chunk() {
        let original = (0..crate::RANGE_INTEGRITY_CHUNK_BYTES)
            .map(|index| index.wrapping_mul(131).wrapping_add(17) as u8)
            .collect::<Vec<_>>();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage =
            Storage::from_object_store("memory:///vortex-singleflight".to_string(), store).unwrap();
        let path = "segments/singleflight.vortex";
        storage.write_bytes(path, &original).unwrap();
        let layout = PhysicalLayoutRef::resolve(
            &crate::PhysicalLayoutPolicy::production_baseline().with_role_format(
                crate::PhysicalObjectRole::NormalSegment,
                crate::PhysicalFormat::Vortex,
            ),
            crate::PhysicalObjectRole::NormalSegment,
            crate::PhysicalLayoutContext {
                rows: 1,
                ..crate::PhysicalLayoutContext::default()
            },
        )
        .unwrap()
        .with_integrity(&original);
        let reader = Arc::new(StorageVortexReadAt::new(
            storage.clone(),
            path,
            u64::try_from(original.len()).unwrap(),
            layout,
        ));
        let before = storage.cache_read_counts();
        let callers = 32;
        let barrier = Arc::new(Barrier::new(callers));
        let handles = (0..callers)
            .map(|caller| {
                let reader = Arc::clone(&reader);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let offset = u64::try_from(caller * 16).unwrap();
                    reader.read_verified_range(offset, offset + 64).unwrap()
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            assert_eq!(handle.join().unwrap().len(), 64);
        }

        let reads = storage.cache_read_counts().delta(&before);
        assert_eq!(reads.backing_reads, 1, "{reads:?}");
        assert_eq!(
            reads.backing_bytes,
            crate::RANGE_INTEGRITY_CHUNK_BYTES as u64
        );
    }

    #[test]
    fn object_store_reader_rejects_a_corrupted_integrity_chunk() {
        let original = vec![17_u8; crate::RANGE_INTEGRITY_CHUNK_BYTES + 31];
        let mut corrupted = original.clone();
        corrupted[23] ^= 0xff;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage =
            Storage::from_object_store("memory:///vortex-corruption".to_string(), store).unwrap();
        storage
            .write_bytes("segments/corrupt.vortex", &original)
            .unwrap();
        let layout = PhysicalLayoutRef::resolve(
            &crate::PhysicalLayoutPolicy::production_baseline().with_role_format(
                crate::PhysicalObjectRole::NormalSegment,
                crate::PhysicalFormat::Vortex,
            ),
            crate::PhysicalObjectRole::NormalSegment,
            crate::PhysicalLayoutContext {
                rows: 1,
                ..crate::PhysicalLayoutContext::default()
            },
        )
        .unwrap()
        .with_integrity(&original);
        storage
            .write_bytes("segments/corrupt.vortex", &corrupted)
            .unwrap();
        let reader = StorageVortexReadAt::new(
            storage,
            "segments/corrupt.vortex",
            u64::try_from(original.len()).unwrap(),
            layout,
        );

        let error = reader.read_verified_range(0, 64).unwrap_err();
        assert_eq!(error.code(), "checksum_mismatch");
        assert!(
            error
                .to_string()
                .contains("segments/corrupt.vortex#chunk-0"),
            "{error}"
        );
    }

    #[test]
    fn object_store_reader_repairs_a_corrupted_range_cache_entry() {
        let original = (0..crate::RANGE_INTEGRITY_CHUNK_BYTES + 31)
            .map(|index| index.wrapping_mul(131).wrapping_add(17) as u8)
            .collect::<Vec<_>>();
        let directory = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let uri = directory.path().to_string_lossy().into_owned();
        let path = "segments/cached.vortex";
        let writer = Storage::from_uri(&uri).unwrap();
        writer.write_bytes(path, &original).unwrap();
        let storage = Storage::from_uri_with_cache(&uri, Some(cache.path().to_path_buf())).unwrap();
        storage
            .read_range(path, 0..crate::RANGE_INTEGRITY_CHUNK_BYTES as u64)
            .unwrap();
        let cache_path = cache.path().join(format!(
            ".borsuk-ranges/{}/0-{}",
            blake3::hash(path.as_bytes()).to_hex(),
            crate::RANGE_INTEGRITY_CHUNK_BYTES
        ));
        std::fs::write(cache_path, vec![0_u8; crate::RANGE_INTEGRITY_CHUNK_BYTES]).unwrap();
        let layout = PhysicalLayoutRef::resolve(
            &crate::PhysicalLayoutPolicy::production_baseline().with_role_format(
                crate::PhysicalObjectRole::NormalSegment,
                crate::PhysicalFormat::Vortex,
            ),
            crate::PhysicalObjectRole::NormalSegment,
            crate::PhysicalLayoutContext {
                rows: 1,
                ..crate::PhysicalLayoutContext::default()
            },
        )
        .unwrap()
        .with_integrity(&original);
        let reader = StorageVortexReadAt::new(
            storage,
            path,
            u64::try_from(original.len()).unwrap(),
            layout,
        );

        assert_eq!(reader.read_verified_range(0, 64).unwrap(), original[..64]);
    }

    #[tokio::test]
    async fn filter_is_applied_by_the_vortex_scan() {
        let bytes = encoded_standard_batch().await;
        let actual = read_vortex_table(
            bytes,
            VortexScanOptions::default().with_filter(eq(get_item("tenant", root()), lit(7_u32))),
        )
        .await
        .unwrap();
        let expected = standard_batch()
            .project(&[0])
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .enumerate()
            .filter_map(|(index, value)| matches!(index, 0 | 2 | 4 | 7).then_some(value.unwrap()))
            .collect::<Vec<_>>();
        let actual_ids = actual
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values();

        assert_eq!(actual.num_rows(), 4);
        assert_eq!(actual_ids, expected.as_slice());
    }

    #[tokio::test]
    async fn exact_supported_required_arrow_types_round_trip() {
        let batch = RecordBatch::try_from_iter([
            (
                "uint8",
                Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)])) as Arc<dyn Array>,
            ),
            (
                "uint16",
                Arc::new(UInt16Array::from(vec![Some(10), None, Some(30)])) as Arc<dyn Array>,
            ),
            (
                "uint32",
                Arc::new(UInt32Array::from(vec![Some(100), None, Some(300)])) as Arc<dyn Array>,
            ),
            (
                "uint64",
                Arc::new(UInt64Array::from(vec![Some(1000), None, Some(3000)])) as Arc<dyn Array>,
            ),
            (
                "int64",
                Arc::new(Int64Array::from(vec![Some(-1), None, Some(3)])) as Arc<dyn Array>,
            ),
            (
                "float16",
                Arc::new(Float16Array::from(vec![
                    Some(f16::from_f32(1.25)),
                    None,
                    Some(f16::from_f32(3.5)),
                ])) as Arc<dyn Array>,
            ),
            (
                "float32",
                Arc::new(Float32Array::from(vec![Some(1.25), None, Some(3.5)])) as Arc<dyn Array>,
            ),
            (
                "boolean",
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as Arc<dyn Array>,
            ),
            (
                "fixed_list_f32",
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        [
                            Some(vec![Some(1.0), Some(2.0)]),
                            None,
                            Some(vec![Some(5.0), Some(6.0)]),
                        ],
                        2,
                    ),
                ) as Arc<dyn Array>,
            ),
            (
                "fixed_list_u8",
                Arc::new(FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(
                    [
                        Some(vec![Some(1), Some(2)]),
                        None,
                        Some(vec![Some(5), Some(6)]),
                    ],
                    2,
                )) as Arc<dyn Array>,
            ),
            (
                "list_u32",
                Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>([
                    Some(vec![Some(1), Some(2)]),
                    None,
                    Some(vec![Some(5), Some(6), Some(7)]),
                ])) as Arc<dyn Array>,
            ),
            (
                "list_f32",
                Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>([
                    Some(vec![Some(1.0), Some(2.0)]),
                    None,
                    Some(vec![Some(5.0)]),
                ])) as Arc<dyn Array>,
            ),
        ])
        .unwrap();

        for layout in [VortexLayout::Default, VortexLayout::Compact] {
            let bytes = write_vortex_table(std::slice::from_ref(&batch), layout)
                .await
                .unwrap();
            let actual = read_vortex_table(bytes, VortexScanOptions::default())
                .await
                .unwrap();
            assert_eq!(actual.schema(), batch.schema());
            assert_eq!(actual, batch);
        }
    }

    #[tokio::test]
    async fn utf8_and_binary_are_materialized_to_the_original_arrow_types() {
        let batch = RecordBatch::try_from_iter([
            (
                "label",
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")]))
                    as Arc<dyn Array>,
            ),
            (
                "payload",
                Arc::new(BinaryArray::from(vec![
                    Some(b"one".as_slice()),
                    None,
                    Some(b"three".as_slice()),
                ])) as Arc<dyn Array>,
            ),
        ])
        .unwrap();

        for layout in [VortexLayout::Default, VortexLayout::Compact] {
            let bytes = write_vortex_table(std::slice::from_ref(&batch), layout)
                .await
                .unwrap();
            let actual = read_vortex_table(bytes, VortexScanOptions::default())
                .await
                .unwrap();
            assert_eq!(actual.schema(), batch.schema());
            assert_eq!(actual, batch);
        }
    }

    #[tokio::test]
    async fn fixed_size_binary_is_reported_as_an_upstream_blocker() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "packed",
            DataType::FixedSizeBinary(4),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    [b"abcd".as_slice(), b"efgh".as_slice()].into_iter(),
                )
                .unwrap(),
            )],
        )
        .unwrap();

        for layout in [VortexLayout::Default, VortexLayout::Compact] {
            let error = write_vortex_table(std::slice::from_ref(&batch), layout)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("FixedSizeBinary"),
                "unexpected error: {error}"
            );
        }
    }
}
