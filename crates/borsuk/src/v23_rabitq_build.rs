use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array,
    Float32Array, RecordBatch, UInt16Array, UInt32Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v23_incidence_tree::{V23IncidenceTree, assign_one_leaf, normalize_v23_incidence_vector},
    v23_rabitq::V23RaBitQObjectIdentity,
    v23_rabitq_arrow::{V23RaBitQGeometry, encode_v23_rabitq_geometry},
    v23_rabitq_quantizer::{build_v23_rabitq_rotation, encode_v23_rabitq_residual},
};

const MAXIMUM_RUN_BYTES: usize = 256 * 1024 * 1024;
const OUTPUT_BATCH_ROWS: usize = 32_768;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23RaBitQSourceRow {
    pub(crate) canonical_record_id: Vec<u8>,
    pub(crate) vector: [f32; 96],
    pub(crate) page_ordinal: u32,
    pub(crate) is_primary: bool,
}

pub(crate) struct V23RaBitQBuildRequest<'a, I>
where
    I: Iterator<Item = Result<V23RaBitQSourceRow>>,
{
    pub(crate) tree: &'a V23IncidenceTree,
    pub(crate) source_rows: I,
    pub(crate) expected_source_occurrences: u64,
    pub(crate) expected_unique_rows: u64,
    pub(crate) rotation_seed: [u8; 32],
    pub(crate) scratch_directory: &'a Path,
    pub(crate) output_directory: &'a Path,
    pub(crate) output_uri_prefix: &'a str,
    pub(crate) maximum_sort_run_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23RaBitQBuiltArtifacts {
    pub(crate) source_rows: u64,
    pub(crate) f16_control_rows: u64,
    pub(crate) sort_runs: u32,
    pub(crate) final_progress_sha256: Option<String>,
    pub(crate) row_codes_sha256: String,
    pub(crate) output_directory: PathBuf,
    pub(crate) outputs: Vec<V23RaBitQObjectIdentity>,
}

impl V23RaBitQBuiltArtifacts {
    #[cfg(test)]
    fn role_bytes(&self) -> Vec<Vec<u8>> {
        self.outputs
            .iter()
            .filter(|identity| identity.role != "construction-receipt")
            .map(|identity| {
                let name = identity.uri.rsplit('/').next().unwrap();
                fs::read(self.output_directory.join(name)).unwrap()
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct SortRow {
    leaf_ordinal: u16,
    canonical_record_id: Vec<u8>,
    vector: [f32; 96],
    primary_page: u32,
    replica_page: Option<u32>,
}

impl SortRow {
    fn key(&self) -> (u16, &[u8]) {
        (self.leaf_ordinal, &self.canonical_record_id)
    }

    fn estimated_bytes(&self) -> usize {
        2 + 4 + 4 + 96 * 4 + self.canonical_record_id.len()
    }
}

#[derive(Debug)]
struct HeapRow {
    run: usize,
    row: SortRow,
}

#[derive(Debug)]
struct IdHeapRow {
    run: usize,
    row: SortRow,
}

impl PartialEq for IdHeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.row.canonical_record_id == other.row.canonical_record_id && self.run == other.run
    }
}

impl Eq for IdHeapRow {}

impl PartialOrd for IdHeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IdHeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .canonical_record_id
            .cmp(&self.row.canonical_record_id)
            .then_with(|| other.row.leaf_ordinal.cmp(&self.row.leaf_ordinal))
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialEq for HeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.row.key() == other.row.key() && self.run == other.run
    }
}

impl Eq for HeapRow {}

impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .key()
            .cmp(&self.row.key())
            .then_with(|| other.run.cmp(&self.run))
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn io<T>(path: &Path, value: std::io::Result<T>) -> Result<T> {
    value.map_err(|source| BorsukError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn run_schema() -> Schema {
    Schema::new(vec![
        Field::new("leaf_ordinal", DataType::UInt16, false),
        Field::new("canonical_record_id", DataType::Binary, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            ),
            false,
        ),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

fn row_schema() -> Schema {
    Schema::new(vec![
        Field::new("sign_code", DataType::FixedSizeBinary(12), false),
        Field::new("residual_norm", DataType::Float32, false),
        Field::new("alignment", DataType::Float32, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

fn f16_schema() -> Schema {
    Schema::new(vec![Field::new(
        "row",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float16, false)),
            96,
        ),
        false,
    )])
}

fn ipc_options() -> Result<IpcWriteOptions> {
    Ok(IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?)
}

fn fixed_f32(rows: &[[f32; 96]]) -> Result<FixedSizeListArray> {
    Ok(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        96,
        Arc::new(Float32Array::from_iter_values(
            rows.iter().flatten().copied(),
        )),
        None,
    )?)
}

fn write_run(path: &Path, rows: &mut [SortRow], maximum_bytes: usize, by_leaf: bool) -> Result<()> {
    if by_leaf {
        rows.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
    } else {
        rows.sort_unstable_by(|left, right| {
            left.canonical_record_id
                .cmp(&right.canonical_record_id)
                .then_with(|| left.leaf_ordinal.cmp(&right.leaf_ordinal))
        });
    }
    let file = io(path, File::create(path))?;
    let mut writer = FileWriter::try_new_with_options(file, &run_schema(), ipc_options()?)?;
    for chunk in rows.chunks(OUTPUT_BATCH_ROWS) {
        let vectors = chunk.iter().map(|row| row.vector).collect::<Vec<_>>();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from_iter_values(
                chunk.iter().map(|row| row.leaf_ordinal),
            )),
            Arc::new(BinaryArray::from_iter_values(
                chunk.iter().map(|row| row.canonical_record_id.as_slice()),
            )),
            Arc::new(fixed_f32(&vectors)?),
            Arc::new(UInt32Array::from_iter_values(
                chunk.iter().map(|row| row.primary_page),
            )),
            Arc::new(UInt32Array::from_iter_values(
                chunk.iter().map(|row| row.replica_page.unwrap_or(u32::MAX)),
            )),
        ];
        writer.write(&RecordBatch::try_new(Arc::new(run_schema()), columns)?)?;
    }
    writer.finish()?;
    let encoded_bytes = io(path, fs::metadata(path))?.len();
    if encoded_bytes > maximum_bytes as u64 {
        return Err(invalid("V23 RaBitQ sort run exceeds byte cap"));
    }
    Ok(())
}

struct RunReader {
    reader: FileReader<BufReader<File>>,
    batch: Option<RecordBatch>,
    ordinal: usize,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self> {
        let reader = FileReader::try_new(BufReader::new(io(path, File::open(path))?), None)?;
        if reader.schema().as_ref() != &run_schema() {
            return Err(invalid("V23 RaBitQ sort-run schema differs"));
        }
        let mut value = Self {
            reader,
            batch: None,
            ordinal: 0,
        };
        value.advance_batch()?;
        Ok(value)
    }

    fn advance_batch(&mut self) -> Result<()> {
        self.batch = self.reader.next().transpose()?;
        self.ordinal = 0;
        Ok(())
    }

    fn next_row(&mut self) -> Result<Option<SortRow>> {
        loop {
            let Some(batch) = self.batch.as_ref() else {
                return Ok(None);
            };
            if self.ordinal == batch.num_rows() {
                self.advance_batch()?;
                continue;
            }
            let row = self.ordinal;
            self.ordinal += 1;
            if batch.columns().iter().any(|column| column.is_null(row)) {
                return Err(invalid("V23 RaBitQ sort-run null differs"));
            }
            let leaves = batch.columns()[0]
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| invalid("V23 RaBitQ sort-run leaf differs"))?;
            let ids = batch.columns()[1]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("V23 RaBitQ sort-run ID differs"))?;
            let vectors = batch.columns()[2]
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| invalid("V23 RaBitQ sort-run vector differs"))?;
            let vector_values = vectors
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V23 RaBitQ sort-run vector child differs"))?;
            let primary = batch.columns()[3]
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid("V23 RaBitQ sort-run primary differs"))?;
            let replica = batch.columns()[4]
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid("V23 RaBitQ sort-run replica differs"))?;
            let start = row * 96;
            let vector: [f32; 96] = vector_values.values()[start..start + 96]
                .try_into()
                .unwrap();
            return Ok(Some(SortRow {
                leaf_ordinal: leaves.value(row),
                canonical_record_id: ids.value(row).to_vec(),
                vector,
                primary_page: primary.value(row),
                replica_page: (replica.value(row) != u32::MAX).then(|| replica.value(row)),
            }));
        }
    }
}

#[derive(Default)]
struct OutputRows {
    codes: Vec<[u8; 12]>,
    norms: Vec<f32>,
    alignments: Vec<f32>,
    primary: Vec<u32>,
    replica: Vec<u32>,
    exact: Vec<[f16; 96]>,
}

impl OutputRows {
    fn len(&self) -> usize {
        self.codes.len()
    }

    fn clear(&mut self) {
        self.codes.clear();
        self.norms.clear();
        self.alignments.clear();
        self.primary.clear();
        self.replica.clear();
        self.exact.clear();
    }
}

fn write_output_batch(
    rows: &OutputRows,
    row_writer: &mut FileWriter<File>,
    exact_writer: &mut FileWriter<File>,
) -> Result<()> {
    if rows.len() == 0 {
        return Ok(());
    }
    let row_columns: Vec<ArrayRef> = vec![
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            rows.codes.iter().map(<[_; 12]>::as_slice),
        )?),
        Arc::new(Float32Array::from(rows.norms.clone())),
        Arc::new(Float32Array::from(rows.alignments.clone())),
        Arc::new(UInt32Array::from(rows.primary.clone())),
        Arc::new(UInt32Array::from(rows.replica.clone())),
    ];
    row_writer.write(&RecordBatch::try_new(Arc::new(row_schema()), row_columns)?)?;
    let exact = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float16, false)),
        96,
        Arc::new(Float16Array::from_iter_values(
            rows.exact.iter().flatten().copied(),
        )),
        None,
    )?;
    exact_writer.write(&RecordBatch::try_new(
        Arc::new(f16_schema()),
        vec![Arc::new(exact)],
    )?)?;
    Ok(())
}

fn digest_file(path: &Path) -> Result<(String, u64)> {
    let mut file = io(path, File::open(path))?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = io(path, file.read(&mut buffer))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| invalid("V23 RaBitQ artifact length overflows"))?;
    }
    Ok((format!("{:x}", hasher.finalize()), bytes))
}

fn identity(role: &str, path: &Path, uri_prefix: &str) -> Result<V23RaBitQObjectIdentity> {
    let (sha256, encoded_bytes) = digest_file(path)?;
    Ok(V23RaBitQObjectIdentity {
        role: role.to_string(),
        uri: format!(
            "{}{}",
            uri_prefix,
            path.file_name().and_then(|name| name.to_str()).unwrap()
        ),
        sha256,
        blake3: None,
        encoded_bytes,
    })
}

#[derive(Serialize)]
struct Progress<'a> {
    schema: &'a str,
    completed_rows: u64,
    expected_rows: u64,
    sort_runs: u32,
    terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BuildReceipt {
    pub(crate) schema: String,
    pub(crate) source_rows: u64,
    pub(crate) sort_runs: u32,
    pub(crate) final_progress_sha256: String,
    pub(crate) outputs: Vec<V23RaBitQObjectIdentity>,
}

pub(crate) fn read_v23_rabitq_build_receipt(bytes: &[u8]) -> Result<BuildReceipt> {
    let receipt: BuildReceipt = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V23 RaBitQ build receipt JSON differs: {error}")))?;
    if canonical_bytes(&receipt)? != bytes
        || receipt.schema != "borsuk-v23-rabitq-build-receipt-v1"
        || receipt.source_rows == 0
        || receipt.sort_runs == 0
        || receipt.outputs.len() != 5
        || receipt.final_progress_sha256.len() != 64
        || !receipt
            .final_progress_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("V23 RaBitQ build receipt authority differs"));
    }
    Ok(receipt)
}

fn progress_bytes(value: &Progress<'_>) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|error| invalid(&format!("V23 RaBitQ progress JSON failed: {error}")))?;
    let value = crate::v23_incidence::canonical_json_value(value);
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|error| invalid(&format!("V23 RaBitQ progress JSON failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|error| invalid(&format!("V23 RaBitQ canonical JSON failed: {error}")))?;
    let value = crate::v23_incidence::canonical_json_value(value);
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|error| invalid(&format!("V23 RaBitQ canonical JSON failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn write_progress(path: &Path, value: &Progress<'_>) -> Result<String> {
    let bytes = progress_bytes(value)?;
    io(path, fs::write(path, &bytes))?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

fn validate_paths(request_scratch: &Path, request_output: &Path) -> Result<()> {
    let scratch_is_empty = io(request_scratch, fs::read_dir(request_scratch))?
        .next()
        .is_none();
    let output_is_empty = io(request_output, fs::read_dir(request_output))?
        .next()
        .is_none();
    if !request_scratch.is_absolute()
        || !request_output.is_absolute()
        || request_scratch == request_output
        || !request_scratch.is_dir()
        || !request_output.is_dir()
        || !scratch_is_empty
        || !output_is_empty
    {
        return Err(invalid("V23 RaBitQ build paths differ"));
    }
    Ok(())
}

fn push_unique_row(
    row: SortRow,
    buffer: &mut Vec<SortRow>,
    buffer_bytes: &mut usize,
    paths: &mut Vec<PathBuf>,
    scratch: &Path,
    run_target_bytes: usize,
    maximum_sort_run_bytes: usize,
) -> Result<()> {
    if row.primary_page == u32::MAX || row.replica_page == Some(row.primary_page) {
        return Err(invalid("V23 RaBitQ source occurrence authority differs"));
    }
    let row_bytes = row.estimated_bytes();
    if !buffer.is_empty()
        && buffer_bytes
            .checked_add(row_bytes)
            .is_none_or(|value| value > run_target_bytes)
    {
        let path = scratch.join(format!("rabitq-leaf-run-{:08}.arrow", paths.len()));
        write_run(&path, buffer, maximum_sort_run_bytes, true)?;
        paths.push(path);
        buffer.clear();
        *buffer_bytes = 0;
    }
    *buffer_bytes += row_bytes;
    buffer.push(row);
    Ok(())
}

pub(crate) fn build_v23_rabitq_artifacts<I>(
    request: V23RaBitQBuildRequest<'_, I>,
) -> Result<V23RaBitQBuiltArtifacts>
where
    I: Iterator<Item = Result<V23RaBitQSourceRow>>,
{
    validate_paths(request.scratch_directory, request.output_directory)?;
    if request.expected_unique_rows == 0
        || request.expected_source_occurrences < request.expected_unique_rows
        || request.expected_source_occurrences > request.expected_unique_rows.saturating_mul(2)
        || request.maximum_sort_run_bytes == 0
        || request.maximum_sort_run_bytes > MAXIMUM_RUN_BYTES
        || !request.output_uri_prefix.starts_with("s3://")
        || !request.output_uri_prefix.ends_with('/')
        || request.output_uri_prefix.contains("//../")
        || request.tree.leaves.is_empty()
        || request.tree.leaves.len() > u16::MAX as usize + 1
    {
        return Err(invalid("V23 RaBitQ build request differs"));
    }
    let progress_path = request.output_directory.join("progress.json");
    let run_target_bytes = request.maximum_sort_run_bytes / 2;
    if run_target_bytes == 0 {
        return Err(invalid("V23 RaBitQ sort-run target differs"));
    }
    let mut run_paths = Vec::new();
    let mut buffer = Vec::<SortRow>::new();
    let mut buffer_bytes = 0usize;
    let mut source_occurrences = 0u64;
    for item in request.source_rows {
        let row = item?;
        if row.canonical_record_id.is_empty()
            || row.canonical_record_id.len() > u16::MAX as usize
            || row.vector.iter().any(|value| !value.is_finite())
            || row.page_ordinal == u32::MAX
        {
            return Err(invalid("V23 RaBitQ source row authority differs"));
        }
        let digest = Sha256::digest(&row.canonical_record_id);
        let source_ordinal = u64::from_le_bytes(digest[..8].try_into().unwrap());
        let leaf_ordinal = assign_one_leaf(request.tree, &row.vector, source_ordinal)?;
        let row = SortRow {
            leaf_ordinal,
            canonical_record_id: row.canonical_record_id,
            vector: row.vector,
            primary_page: if row.is_primary {
                row.page_ordinal
            } else {
                u32::MAX
            },
            replica_page: (!row.is_primary).then_some(row.page_ordinal),
        };
        let row_bytes = row.estimated_bytes();
        if row_bytes > run_target_bytes {
            return Err(invalid("V23 RaBitQ sort row exceeds run cap"));
        }
        if !buffer.is_empty()
            && buffer_bytes
                .checked_add(row_bytes)
                .is_none_or(|value| value > run_target_bytes)
        {
            let path = request
                .scratch_directory
                .join(format!("rabitq-id-run-{:08}.arrow", run_paths.len()));
            write_run(&path, &mut buffer, request.maximum_sort_run_bytes, false)?;
            run_paths.push(path);
            buffer.clear();
            buffer_bytes = 0;
            write_progress(
                &progress_path,
                &Progress {
                    schema: "borsuk-v23-rabitq-progress-v1",
                    completed_rows: source_occurrences,
                    expected_rows: request.expected_unique_rows,
                    sort_runs: u32::try_from(run_paths.len()).unwrap(),
                    terminal: false,
                },
            )?;
        }
        buffer_bytes += row_bytes;
        buffer.push(row);
        source_occurrences = source_occurrences
            .checked_add(1)
            .ok_or_else(|| invalid("V23 RaBitQ source row count overflows"))?;
        if source_occurrences > request.expected_source_occurrences {
            return Err(invalid("V23 RaBitQ source row count differs"));
        }
    }
    if source_occurrences != request.expected_source_occurrences {
        return Err(invalid("V23 RaBitQ source row count differs"));
    }
    if !buffer.is_empty() {
        let path = request
            .scratch_directory
            .join(format!("rabitq-id-run-{:08}.arrow", run_paths.len()));
        write_run(&path, &mut buffer, request.maximum_sort_run_bytes, false)?;
        run_paths.push(path);
    }

    // First merge by canonical record identity. This catches duplicate or
    // conflicting primary rows even when their vectors route to different
    // leaves. Unique rows are then external-sorted by the serving order.
    let mut id_readers = run_paths
        .iter()
        .map(|path| RunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut id_heap = BinaryHeap::new();
    for (run, reader) in id_readers.iter_mut().enumerate() {
        if let Some(row) = reader.next_row()? {
            id_heap.push(IdHeapRow { run, row });
        }
    }
    let mut leaf_run_paths = Vec::new();
    let mut leaf_buffer = Vec::new();
    let mut leaf_buffer_bytes = 0usize;
    let mut pending: Option<SortRow> = None;
    let mut unique_rows = 0u64;
    while let Some(IdHeapRow { run, row }) = id_heap.pop() {
        match pending.as_mut() {
            Some(current) if current.canonical_record_id == row.canonical_record_id => {
                if current.leaf_ordinal != row.leaf_ordinal
                    || current
                        .vector
                        .iter()
                        .zip(row.vector)
                        .any(|(left, right)| left.to_bits() != right.to_bits())
                {
                    return Err(invalid("V23 RaBitQ source occurrence vector differs"));
                }
                if row.primary_page != u32::MAX {
                    if current.primary_page != u32::MAX {
                        return Err(invalid("V23 RaBitQ duplicate primary row"));
                    }
                    current.primary_page = row.primary_page;
                }
                if let Some(replica) = row.replica_page {
                    if current.replica_page.is_some() {
                        return Err(invalid("V23 RaBitQ duplicate replica row"));
                    }
                    current.replica_page = Some(replica);
                }
            }
            Some(_) => {
                let complete = pending.replace(row).unwrap();
                push_unique_row(
                    complete,
                    &mut leaf_buffer,
                    &mut leaf_buffer_bytes,
                    &mut leaf_run_paths,
                    request.scratch_directory,
                    run_target_bytes,
                    request.maximum_sort_run_bytes,
                )?;
                unique_rows += 1;
            }
            None => pending = Some(row),
        }
        if let Some(next) = id_readers[run].next_row()? {
            id_heap.push(IdHeapRow { run, row: next });
        }
    }
    if let Some(complete) = pending {
        push_unique_row(
            complete,
            &mut leaf_buffer,
            &mut leaf_buffer_bytes,
            &mut leaf_run_paths,
            request.scratch_directory,
            run_target_bytes,
            request.maximum_sort_run_bytes,
        )?;
        unique_rows += 1;
    }
    if unique_rows != request.expected_unique_rows {
        return Err(invalid("V23 RaBitQ unique source row count differs"));
    }
    if !leaf_buffer.is_empty() {
        let path = request
            .scratch_directory
            .join(format!("rabitq-leaf-run-{:08}.arrow", leaf_run_paths.len()));
        write_run(
            &path,
            &mut leaf_buffer,
            request.maximum_sort_run_bytes,
            true,
        )?;
        leaf_run_paths.push(path);
    }
    for path in &run_paths {
        io(path, fs::remove_file(path))?;
    }
    let total_sort_runs = run_paths
        .len()
        .checked_add(leaf_run_paths.len())
        .ok_or_else(|| invalid("V23 RaBitQ sort-run count overflows"))?;

    let rotation = build_v23_rabitq_rotation(request.rotation_seed)?;
    let row_path = request.output_directory.join("row-codes.arrow");
    let exact_path = request.output_directory.join("f16-control.arrow");
    let mut row_writer = FileWriter::try_new_with_options(
        io(&row_path, File::create(&row_path))?,
        &row_schema(),
        ipc_options()?,
    )?;
    let mut exact_writer = FileWriter::try_new_with_options(
        io(&exact_path, File::create(&exact_path))?,
        &f16_schema(),
        ipc_options()?,
    )?;
    let mut readers = leaf_run_paths
        .iter()
        .map(|path| RunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = reader.next_row()? {
            heap.push(HeapRow { run, row });
        }
    }
    let mut output_rows = OutputRows::default();
    let mut leaf_offsets = vec![0u64; request.tree.leaves.len() + 1];
    let mut current_leaf = 0usize;
    let mut merged_rows = 0u64;
    while let Some(HeapRow { run, row }) = heap.pop() {
        let leaf = usize::from(row.leaf_ordinal);
        while current_leaf < leaf {
            leaf_offsets[current_leaf + 1] = merged_rows;
            current_leaf += 1;
        }
        let normalized = normalize_v23_incidence_vector(&row.vector)?;
        let centroid = request.tree.leaves[leaf].centroid.map(f16::to_f32);
        let residual = std::array::from_fn(|dimension| normalized[dimension] - centroid[dimension]);
        let code = encode_v23_rabitq_residual(&residual, &rotation)?;
        output_rows.codes.push(code.sign_code);
        output_rows.norms.push(code.residual_norm);
        output_rows.alignments.push(code.alignment);
        output_rows.primary.push(row.primary_page);
        output_rows
            .replica
            .push(row.replica_page.unwrap_or(u32::MAX));
        output_rows.exact.push(normalized.map(f16::from_f32));
        merged_rows += 1;
        if output_rows.len() == OUTPUT_BATCH_ROWS {
            write_output_batch(&output_rows, &mut row_writer, &mut exact_writer)?;
            output_rows.clear();
        }
        if let Some(next) = readers[run].next_row()? {
            heap.push(HeapRow { run, row: next });
        }
    }
    write_output_batch(&output_rows, &mut row_writer, &mut exact_writer)?;
    row_writer.finish()?;
    exact_writer.finish()?;
    if merged_rows != request.expected_unique_rows {
        return Err(invalid("V23 RaBitQ merged row count differs"));
    }
    while current_leaf < request.tree.leaves.len() {
        leaf_offsets[current_leaf + 1] = merged_rows;
        current_leaf += 1;
    }

    let geometry = V23RaBitQGeometry {
        leaf_offsets,
        centroids: request
            .tree
            .leaves
            .iter()
            .map(|leaf| leaf.centroid)
            .collect(),
        rotation,
    };
    let geometry_bytes = encode_v23_rabitq_geometry(&geometry)?;
    let offset_path = request.output_directory.join("leaf-offsets.arrow");
    let centroid_path = request.output_directory.join("centroids.arrow");
    let rotation_path = request.output_directory.join("rotation.arrow");
    io(
        &offset_path,
        fs::write(&offset_path, geometry_bytes.leaf_offsets),
    )?;
    io(
        &centroid_path,
        fs::write(&centroid_path, geometry_bytes.centroids),
    )?;
    io(
        &rotation_path,
        fs::write(&rotation_path, geometry_bytes.rotation),
    )?;

    let final_progress_sha256 = write_progress(
        &progress_path,
        &Progress {
            schema: "borsuk-v23-rabitq-progress-v1",
            completed_rows: unique_rows,
            expected_rows: request.expected_unique_rows,
            sort_runs: u32::try_from(total_sort_runs)
                .map_err(|_| invalid("V23 RaBitQ sort-run count overflows"))?,
            terminal: true,
        },
    )?;
    for path in &leaf_run_paths {
        io(path, fs::remove_file(path))?;
    }
    let roles = [
        ("row-codes", &row_path),
        ("leaf-offsets", &offset_path),
        ("centroids", &centroid_path),
        ("rotation", &rotation_path),
        ("f16-control", &exact_path),
    ];
    let mut outputs = roles
        .into_iter()
        .map(|(role, path)| identity(role, path, request.output_uri_prefix))
        .collect::<Result<Vec<_>>>()?;
    let receipt_path = request.output_directory.join("construction-receipt.json");
    let receipt = BuildReceipt {
        schema: "borsuk-v23-rabitq-build-receipt-v1".to_string(),
        source_rows: unique_rows,
        sort_runs: u32::try_from(total_sort_runs).unwrap(),
        final_progress_sha256: final_progress_sha256.clone(),
        outputs: outputs.clone(),
    };
    io(
        &receipt_path,
        fs::write(&receipt_path, canonical_bytes(&receipt)?),
    )?;
    outputs.push(identity(
        "construction-receipt",
        &receipt_path,
        request.output_uri_prefix,
    )?);
    let row_codes_sha256 = outputs[0].sha256.clone();
    let built = V23RaBitQBuiltArtifacts {
        source_rows: unique_rows,
        f16_control_rows: merged_rows,
        sort_runs: u32::try_from(total_sort_runs).unwrap(),
        final_progress_sha256: Some(final_progress_sha256),
        row_codes_sha256,
        output_directory: request.output_directory.to_path_buf(),
        outputs,
    };
    validate_v23_rabitq_built_artifacts(&built)?;
    Ok(built)
}

pub(crate) fn validate_v23_rabitq_built_artifacts(built: &V23RaBitQBuiltArtifacts) -> Result<()> {
    let expected_roles = [
        "row-codes",
        "leaf-offsets",
        "centroids",
        "rotation",
        "f16-control",
        "construction-receipt",
    ];
    if built.source_rows == 0
        || built.f16_control_rows != built.source_rows
        || built.sort_runs == 0
        || built.outputs.len() != expected_roles.len()
        || built.row_codes_sha256 != built.outputs[0].sha256
    {
        return Err(invalid("V23 RaBitQ built artifact authority differs"));
    }
    for (output, role) in built.outputs.iter().zip(expected_roles) {
        let name = output
            .uri
            .rsplit('/')
            .next()
            .ok_or_else(|| invalid("V23 RaBitQ output URI differs"))?;
        let path = built.output_directory.join(name);
        let (sha256, encoded_bytes) = digest_file(&path)?;
        if output.role != role
            || output.sha256 != sha256
            || output.encoded_bytes != encoded_bytes
            || output.blake3.is_some()
        {
            return Err(invalid("V23 RaBitQ built artifact digest differs"));
        }
    }
    let progress_path = built.output_directory.join("progress.json");
    let progress_bytes = io(&progress_path, fs::read(&progress_path))?;
    if built.final_progress_sha256.as_deref()
        != Some(format!("{:x}", Sha256::digest(&progress_bytes)).as_str())
    {
        return Err(invalid("V23 RaBitQ final progress digest differs"));
    }
    let receipt_path = built.output_directory.join("construction-receipt.json");
    let receipt_bytes = io(&receipt_path, fs::read(&receipt_path))?;
    let receipt = read_v23_rabitq_build_receipt(&receipt_bytes)?;
    if receipt.schema != "borsuk-v23-rabitq-build-receipt-v1"
        || receipt.source_rows != built.source_rows
        || receipt.sort_runs != built.sort_runs
        || Some(receipt.final_progress_sha256.as_str()) != built.final_progress_sha256.as_deref()
        || receipt.outputs != built.outputs[..5]
    {
        return Err(invalid("V23 RaBitQ build receipt binding differs"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, fs, path::Path, rc::Rc};

    use tempfile::tempdir;

    use super::{
        V23RaBitQBuildRequest, V23RaBitQSourceRow, build_v23_rabitq_artifacts,
        validate_v23_rabitq_built_artifacts,
    };
    use crate::v23_incidence_tree::{
        V23IncidenceTrainingShape, V23TrainingRow, train_incidence_tree_test_shape,
    };
    use crate::v23_rabitq_arrow::{read_v23_rabitq_f16_control, read_v23_rabitq_row_planes};

    fn vector(ordinal: usize) -> [f32; 96] {
        std::array::from_fn(|dimension| {
            (((ordinal + 1) * (dimension + 3) % 211) as f32 + 1.0) / 212.0
        })
    }

    fn tree(rows: usize) -> crate::v23_incidence_tree::V23IncidenceTree {
        let training = (0..rows.max(32))
            .map(|ordinal| V23TrainingRow {
                source_ordinal: ordinal as u64,
                vector: vector(ordinal),
            })
            .collect::<Vec<_>>();
        train_incidence_tree_test_shape(
            &training,
            V23IncidenceTrainingShape {
                dimensions: 96,
                reservoir_rows: 32,
                depth: 3,
                lloyd_iterations: 4,
            },
            1,
            16,
        )
        .unwrap()
    }

    fn source_row(ordinal: usize) -> V23RaBitQSourceRow {
        V23RaBitQSourceRow {
            canonical_record_id: format!("record-{ordinal:08}").into_bytes(),
            vector: vector(ordinal),
            page_ordinal: (ordinal % 16) as u32,
            is_primary: true,
        }
    }

    fn request<'a, I>(
        tree: &'a crate::v23_incidence_tree::V23IncidenceTree,
        rows: I,
        expected_rows: u64,
        scratch: &'a Path,
        output: &'a Path,
        maximum_sort_run_bytes: usize,
    ) -> V23RaBitQBuildRequest<'a, I>
    where
        I: Iterator<Item = crate::Result<V23RaBitQSourceRow>>,
    {
        V23RaBitQBuildRequest {
            tree,
            source_rows: rows,
            expected_source_occurrences: expected_rows,
            expected_unique_rows: expected_rows,
            rotation_seed: [0x42; 32],
            scratch_directory: scratch,
            output_directory: output,
            output_uri_prefix: "s3://borsuk-v23-rabitq/test-construction/",
            maximum_sort_run_bytes,
        }
    }

    fn build(rows: Vec<V23RaBitQSourceRow>, maximum_sort_run_bytes: usize) -> Vec<Vec<u8>> {
        let tree = tree(rows.len());
        let scratch = tempdir().unwrap();
        let output = tempdir().unwrap();
        let built = build_v23_rabitq_artifacts(request(
            &tree,
            rows.into_iter().map(Ok),
            24,
            scratch.path(),
            output.path(),
            maximum_sort_run_bytes,
        ))
        .unwrap();
        validate_v23_rabitq_built_artifacts(&built).unwrap();
        built.role_bytes()
    }

    #[test]
    fn v23_rabitq_build_stream_is_one_pass_and_rejects_source_authority_drift() {
        struct OnceRows {
            rows: std::vec::IntoIter<V23RaBitQSourceRow>,
            calls: Rc<Cell<usize>>,
        }
        impl Iterator for OnceRows {
            type Item = crate::Result<V23RaBitQSourceRow>;

            fn next(&mut self) -> Option<Self::Item> {
                self.calls.set(self.calls.get() + 1);
                self.rows.next().map(Ok)
            }
        }

        let rows = (0..24).map(source_row).collect::<Vec<_>>();
        let calls = Rc::new(Cell::new(0));
        let tree = tree(rows.len());
        let scratch = tempdir().unwrap();
        let output = tempdir().unwrap();
        let built = build_v23_rabitq_artifacts(request(
            &tree,
            OnceRows {
                rows: rows.clone().into_iter(),
                calls: Rc::clone(&calls),
            },
            24,
            scratch.path(),
            output.path(),
            8_192,
        ))
        .unwrap();
        assert_eq!(built.source_rows, 24);
        assert_eq!(calls.get(), 25);

        let variants = [
            {
                let mut value = rows.clone();
                value.push(value[0].clone());
                value
            },
            {
                let mut value = rows.clone();
                let mut conflicting = value[0].clone();
                conflicting.vector = vector(10_001);
                conflicting.page_ordinal = 15;
                value.push(conflicting);
                value
            },
            {
                let mut value = rows.clone();
                value[2].canonical_record_id.clear();
                value
            },
            {
                let mut value = rows.clone();
                value[3].vector[0] = f32::NAN;
                value
            },
            {
                let mut value = rows.clone();
                let mut replica = value[4].clone();
                replica.is_primary = false;
                value.push(replica);
                value
            },
            rows[..23].to_vec(),
        ];
        for invalid in variants {
            let scratch = tempdir().unwrap();
            let output = tempdir().unwrap();
            assert!(
                build_v23_rabitq_artifacts(request(
                    &tree,
                    invalid.into_iter().map(Ok),
                    24,
                    scratch.path(),
                    output.path(),
                    8_192,
                ))
                .is_err()
            );
        }
    }

    #[test]
    fn v23_rabitq_build_external_runs_merge_deterministically_in_leaf_record_order() {
        let rows = (0..24).map(source_row).collect::<Vec<_>>();
        let mut reversed = rows.clone();
        reversed.reverse();
        let one_row_runs = build(reversed, 8_192);
        let one_run = build(rows, 256 * 1024 * 1024);
        assert_eq!(one_row_runs, one_run);
    }

    #[test]
    fn v23_rabitq_build_progress_digest_and_f16_control_are_terminally_bound() {
        let rows = (0..24).map(source_row).collect::<Vec<_>>();
        let tree = tree(rows.len());
        let scratch = tempdir().unwrap();
        let output = tempdir().unwrap();
        fs::write(output.path().join("progress.json"), b"interrupted\n").unwrap();
        assert!(
            build_v23_rabitq_artifacts(request(
                &tree,
                rows.clone().into_iter().map(Ok),
                24,
                scratch.path(),
                output.path(),
                8_192,
            ))
            .is_err()
        );

        let output = tempdir().unwrap();
        let built = build_v23_rabitq_artifacts(request(
            &tree,
            rows.into_iter().map(Ok),
            24,
            scratch.path(),
            output.path(),
            8_192,
        ))
        .unwrap();
        assert_eq!(built.f16_control_rows, built.source_rows);
        assert!(built.sort_runs > 1);
        assert!(built.final_progress_sha256.is_some());
        validate_v23_rabitq_built_artifacts(&built).unwrap();
        let row_bytes = fs::read(output.path().join("row-codes.arrow")).unwrap();
        let decoded = read_v23_rabitq_row_planes(&row_bytes, &built.outputs[0]).unwrap();
        assert_eq!(decoded.sign_codes.len(), 24);
        let exact_bytes = fs::read(output.path().join("f16-control.arrow")).unwrap();
        let exact = read_v23_rabitq_f16_control(&exact_bytes, &built.outputs[4], 24).unwrap();
        assert_eq!(exact.len(), 24);

        let mut changed = built.clone();
        changed.row_codes_sha256.replace_range(0..1, "0");
        assert!(validate_v23_rabitq_built_artifacts(&changed).is_err());
    }

    #[test]
    fn v23_rabitq_build_joins_one_primary_and_replica_occurrence_in_either_order() {
        let primary = V23RaBitQSourceRow {
            canonical_record_id: b"record-occurrence".to_vec(),
            vector: vector(7),
            page_ordinal: 3,
            is_primary: true,
        };
        let replica = V23RaBitQSourceRow {
            canonical_record_id: primary.canonical_record_id.clone(),
            vector: primary.vector,
            page_ordinal: 11,
            is_primary: false,
        };
        for rows in [
            vec![primary.clone(), replica.clone()],
            vec![replica.clone(), primary.clone()],
        ] {
            let tree = tree(32);
            let scratch = tempdir().unwrap();
            let output = tempdir().unwrap();
            let mut request = request(
                &tree,
                rows.into_iter().map(Ok),
                1,
                scratch.path(),
                output.path(),
                8_192,
            );
            request.expected_source_occurrences = 2;
            let built = build_v23_rabitq_artifacts(request).unwrap();
            let bytes = fs::read(output.path().join("row-codes.arrow")).unwrap();
            let decoded = read_v23_rabitq_row_planes(&bytes, &built.outputs[0]).unwrap();
            assert_eq!(decoded.primary_pages, vec![3]);
            assert_eq!(decoded.replica_pages, vec![11]);
        }
    }
}
