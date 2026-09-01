use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, UInt64Array,
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use parquet::arrow::ArrowWriter;
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v23_balanced_pages::V23BalancedIdentity,
    v23_balanced_pages_arrow::{V23PageRow, V23RowPage, V23SupercellRow, v23_row_page_schema},
    v23_incidence_tree::normalize_v23_incidence_vector,
};

const DIMENSIONS: i32 = 96;
const MAX_PARTITIONS: usize = 256;
const OUTPUT_BATCH_ROWS: usize = 65_536;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23RoutedRow {
    pub(crate) supercell_ordinal: u32,
    pub(crate) source_ordinal: u64,
    pub(crate) vector: [f32; 96],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23PageBuildShape {
    pub(crate) supercells: u32,
    pub(crate) primary_rows_per_page: u16,
    pub(crate) run_rows: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23PrimaryPageBuild {
    pub(crate) supercells: Vec<V23SupercellRow>,
    pub(crate) pages: Vec<V23PageRow>,
    pub(crate) row_pages: V23BalancedIdentity,
    pub(crate) source_rows: u64,
    pub(crate) maximum_resident_rows: u64,
    pub(crate) scratch_bytes_peak: u64,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("V23 balanced page build {message}"))
}

fn io_error(path: &Path, source: std::io::Error) -> BorsukError {
    BorsukError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn routed_schema() -> Schema {
    Schema::new(vec![
        Field::new("supercell_ordinal", DataType::UInt32, false),
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                DIMENSIONS,
            ),
            false,
        ),
    ])
}

fn assignment_batch(rows: &[V23RowPage]) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(v23_row_page_schema()),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.source_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.primary_page),
            )),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.replica_page),
            )),
        ],
    )
    .map_err(Into::into)
}

fn routed_batch(rows: &[V23RoutedRow]) -> Result<RecordBatch> {
    let values = Arc::new(Float32Array::from_iter_values(
        rows.iter().flat_map(|row| row.vector),
    ));
    let vectors: ArrayRef = Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        DIMENSIONS,
        values,
        None,
    )?);
    Ok(RecordBatch::try_new(
        Arc::new(routed_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.supercell_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.source_ordinal),
            )),
            vectors,
        ],
    )?)
}

fn write_ipc(path: &Path, schema: &Schema, batches: &[RecordBatch]) -> Result<u64> {
    let file = File::create(path).map_err(|error| io_error(path, error))?;
    let mut writer = FileWriter::try_new(file, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| io_error(path, error))
}

fn read_routed_ipc(path: &Path) -> Result<Vec<V23RoutedRow>> {
    let file = File::open(path).map_err(|error| io_error(path, error))?;
    let mut reader = FileReader::try_new(file, None)?;
    if reader.schema().as_ref() != &routed_schema() {
        return Err(invalid("scratch routed schema differs"));
    }
    let mut rows = Vec::new();
    for batch in &mut reader {
        let batch = batch?;
        let supercells = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("scratch supercell column differs"))?;
        let ordinals = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("scratch ordinal column differs"))?;
        let vectors = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("scratch vector column differs"))?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("scratch vector child differs"))?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("scratch nullability differs"));
        }
        for row in 0..batch.num_rows() {
            let start = row * 96;
            let vector = values.values()[start..start + 96]
                .try_into()
                .map_err(|_| invalid("scratch vector width differs"))?;
            rows.push(V23RoutedRow {
                supercell_ordinal: supercells.value(row),
                source_ordinal: ordinals.value(row),
                vector,
            });
        }
    }
    Ok(rows)
}

fn partition_for(supercell: u32, shape: V23PageBuildShape, partitions: usize) -> usize {
    (usize::try_from(supercell).unwrap() * partitions) / usize::try_from(shape.supercells).unwrap()
}

struct ScratchEvidence {
    current_bytes: u64,
    peak_bytes: u64,
}

impl ScratchEvidence {
    fn add(&mut self, bytes: u64) -> Result<()> {
        self.current_bytes = self
            .current_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("scratch bytes overflow"))?;
        self.peak_bytes = self.peak_bytes.max(self.current_bytes);
        Ok(())
    }

    fn remove(&mut self, path: &Path) -> Result<()> {
        let bytes = fs::metadata(path)
            .map_err(|error| io_error(path, error))?
            .len();
        fs::remove_file(path).map_err(|error| io_error(path, error))?;
        self.current_bytes = self
            .current_bytes
            .checked_sub(bytes)
            .ok_or_else(|| invalid("scratch byte accounting underflows"))?;
        Ok(())
    }
}

fn flush_routed_run(
    rows: &mut Vec<V23RoutedRow>,
    run: usize,
    shape: V23PageBuildShape,
    partitions: usize,
    scratch: &Path,
    paths: &mut [Vec<PathBuf>],
    evidence: &mut ScratchEvidence,
) -> Result<()> {
    let mut by_partition = vec![Vec::new(); partitions];
    for row in rows.drain(..) {
        let partition = partition_for(row.supercell_ordinal, shape, partitions);
        by_partition[partition].push(row);
    }
    for (partition, values) in by_partition.iter_mut().enumerate() {
        if values.is_empty() {
            continue;
        }
        values.sort_unstable_by_key(|row| (row.supercell_ordinal, row.source_ordinal));
        let path = scratch.join(format!("routed-p{partition:03}-r{run:08}.arrow"));
        let bytes = write_ipc(&path, &routed_schema(), &[routed_batch(values)?])?;
        evidence.add(bytes)?;
        paths[partition].push(path);
    }
    Ok(())
}

fn dot(left: &[f32; 96], right: &[f32; 96]) -> f32 {
    let mut lanes = [0.0_f32; 8];
    for (lane, accumulator) in lanes.iter_mut().enumerate() {
        for step in 0..12 {
            let dimension = lane * 12 + step;
            *accumulator = left[dimension].mul_add(right[dimension], *accumulator);
        }
    }
    lanes.into_iter().sum()
}

fn normalized_centroid(rows: &[V23RoutedRow]) -> Result<[f32; 96]> {
    if rows.is_empty() {
        return Err(invalid("centroid population is empty"));
    }
    let mut sum = [0.0_f64; 96];
    for row in rows {
        for (target, value) in sum.iter_mut().zip(row.vector) {
            *target += f64::from(value);
        }
    }
    let mean = sum.map(|value| (value / rows.len() as f64) as f32);
    normalize_v23_incidence_vector(&mean)
}

fn stored_centroid_and_radius(rows: &[V23RoutedRow]) -> Result<([f16; 96], f32)> {
    let centroid = normalized_centroid(rows)?.map(f16::from_f32);
    let decoded = centroid.map(f16::to_f32);
    let squared_norm = dot(&decoded, &decoded);
    if !squared_norm.is_finite() || squared_norm <= 0.0 {
        return Err(invalid("stored centroid norm differs"));
    }
    let inverse_norm = squared_norm.sqrt().recip();
    let mut radius = 0.0_f32;
    for row in rows {
        let distance = 1.0 - dot(&row.vector, &decoded) * inverse_norm;
        if !distance.is_finite() {
            return Err(invalid("stored centroid distance is non-finite"));
        }
        radius = radius.max(distance.max(0.0));
    }
    Ok((centroid, radius))
}

fn split_rows(
    mut rows: Vec<V23RoutedRow>,
    left_size: usize,
) -> Result<(Vec<V23RoutedRow>, Vec<V23RoutedRow>)> {
    if left_size == 0 || left_size >= rows.len() {
        return Err(invalid("page split cardinality differs"));
    }
    rows.sort_unstable_by_key(|row| row.source_ordinal);
    let mut left_centroid = rows[0].vector;
    let farthest = rows
        .iter()
        .enumerate()
        .min_by(|left, right| {
            dot(&left_centroid, &left.1.vector)
                .total_cmp(&dot(&left_centroid, &right.1.vector))
                .then_with(|| left.1.source_ordinal.cmp(&right.1.source_ordinal))
        })
        .map(|entry| entry.0)
        .ok_or_else(|| invalid("page farthest seed is missing"))?;
    let mut right_centroid = rows[farthest].vector;
    for _ in 0..4 {
        rows.sort_unstable_by(|left, right| {
            (dot(&left.vector, &right_centroid) - dot(&left.vector, &left_centroid))
                .total_cmp(
                    &(dot(&right.vector, &right_centroid) - dot(&right.vector, &left_centroid)),
                )
                .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
        });
        left_centroid = normalized_centroid(&rows[..left_size])?;
        right_centroid = normalized_centroid(&rows[left_size..])?;
    }
    rows.sort_unstable_by(|left, right| {
        (dot(&left.vector, &right_centroid) - dot(&left.vector, &left_centroid))
            .total_cmp(&(dot(&right.vector, &right_centroid) - dot(&right.vector, &left_centroid)))
            .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
    });
    let right = rows.split_off(left_size);
    Ok((rows, right))
}

fn partition_pages(rows: Vec<V23RoutedRow>, page_count: usize) -> Result<Vec<Vec<V23RoutedRow>>> {
    if page_count == 1 {
        return Ok(vec![rows]);
    }
    let base = rows.len() / page_count;
    let remainder = rows.len() % page_count;
    let left_pages = page_count / 2;
    let left_size = left_pages * base + remainder.min(left_pages);
    let (left, right) = split_rows(rows, left_size)?;
    let mut pages = partition_pages(left, left_pages)?;
    pages.extend(partition_pages(right, page_count - left_pages)?);
    Ok(pages)
}

fn identity_for_path(path: &Path, uri: &str) -> Result<V23BalancedIdentity> {
    let file = File::open(path).map_err(|error| io_error(path, error))?;
    let encoded_bytes = file
        .metadata()
        .map_err(|error| io_error(path, error))?
        .len();
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| io_error(path, error))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(V23BalancedIdentity {
        role: "row-pages-primary-parquet".to_owned(),
        uri: uri.to_owned(),
        digest_algorithm: "sha256".to_owned(),
        digest: format!("{:x}", digest.finalize()),
        encoded_bytes,
    })
}

struct AssignmentCursor {
    reader: FileReader<File>,
    batch: Option<RecordBatch>,
    row: usize,
}

impl AssignmentCursor {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| io_error(path, error))?;
        let mut reader = FileReader::try_new(file, None)?;
        if reader.schema().as_ref() != &v23_row_page_schema() {
            return Err(invalid("assignment scratch schema differs"));
        }
        let batch = reader.next().transpose()?;
        Ok(Self {
            reader,
            batch,
            row: 0,
        })
    }

    fn current(&self) -> Result<Option<V23RowPage>> {
        let Some(batch) = &self.batch else {
            return Ok(None);
        };
        let source = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("assignment source column differs"))?;
        let primary = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("assignment primary column differs"))?;
        let replica = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("assignment replica column differs"))?;
        Ok(Some(V23RowPage {
            source_ordinal: source.value(self.row),
            primary_page: primary.value(self.row),
            replica_page: replica.value(self.row),
        }))
    }

    fn advance(&mut self) -> Result<()> {
        let Some(batch) = &self.batch else {
            return Ok(());
        };
        self.row += 1;
        if self.row == batch.num_rows() {
            self.batch = self.reader.next().transpose()?;
            self.row = 0;
        }
        Ok(())
    }
}

fn merge_assignments(paths: &[PathBuf], output: &Path) -> Result<u64> {
    let mut cursors = paths
        .iter()
        .map(|path| AssignmentCursor::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, cursor) in cursors.iter().enumerate() {
        if let Some(row) = cursor.current()? {
            heap.push(Reverse((row.source_ordinal, index)));
        }
    }
    let file = File::create(output).map_err(|error| io_error(output, error))?;
    let mut writer = ArrowWriter::try_new(file, Arc::new(v23_row_page_schema()), None)?;
    let mut buffer = Vec::with_capacity(OUTPUT_BATCH_ROWS);
    let mut emitted_rows = 0_u64;
    while let Some(Reverse((ordinal, cursor_index))) = heap.pop() {
        let row = cursors[cursor_index]
            .current()?
            .ok_or_else(|| invalid("assignment merge cursor is empty"))?;
        if row.source_ordinal != ordinal || ordinal != emitted_rows {
            return Err(invalid("assignment source order differs"));
        }
        emitted_rows = emitted_rows
            .checked_add(1)
            .ok_or_else(|| invalid("assignment output count overflows"))?;
        buffer.push(row);
        cursors[cursor_index].advance()?;
        if let Some(next) = cursors[cursor_index].current()? {
            heap.push(Reverse((next.source_ordinal, cursor_index)));
        }
        if buffer.len() == OUTPUT_BATCH_ROWS {
            writer.write(&assignment_batch(&buffer)?)?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        writer.write(&assignment_batch(&buffer)?)?;
    }
    writer.close()?;
    Ok(emitted_rows)
}

pub(crate) fn build_v23_primary_pages(
    rows: impl IntoIterator<Item = Result<V23RoutedRow>>,
    shape: V23PageBuildShape,
    workers: usize,
    scratch: &Path,
    output: &Path,
    output_uri: &str,
) -> Result<V23PrimaryPageBuild> {
    if shape.supercells == 0
        || shape.primary_rows_per_page == 0
        || shape.primary_rows_per_page > 384
        || shape.run_rows == 0
        || !(1..=64).contains(&workers)
        || !scratch.is_dir()
        || scratch
            .read_dir()
            .map_err(|error| io_error(scratch, error))?
            .next()
            .is_some()
        || output.exists()
        || !output_uri.starts_with("s3://")
    {
        return Err(invalid("construction boundary differs"));
    }
    let partitions = usize::try_from(shape.supercells)
        .unwrap()
        .min(MAX_PARTITIONS);
    let mut paths = vec![Vec::new(); partitions];
    let mut evidence = ScratchEvidence {
        current_bytes: 0,
        peak_bytes: 0,
    };
    let mut buffer = Vec::with_capacity(shape.run_rows);
    let mut source_rows = 0_u64;
    let mut run = 0_usize;
    let result = (|| {
        for row in rows {
            let mut row = row?;
            if row.supercell_ordinal >= shape.supercells {
                return Err(invalid("routed supercell is out of range"));
            }
            row.vector = normalize_v23_incidence_vector(&row.vector)?;
            source_rows = source_rows
                .checked_add(1)
                .ok_or_else(|| invalid("source rows overflow"))?;
            buffer.push(row);
            if buffer.len() == shape.run_rows {
                flush_routed_run(
                    &mut buffer,
                    run,
                    shape,
                    partitions,
                    scratch,
                    &mut paths,
                    &mut evidence,
                )?;
                run += 1;
            }
        }
        if !buffer.is_empty() {
            flush_routed_run(
                &mut buffer,
                run,
                shape,
                partitions,
                scratch,
                &mut paths,
                &mut evidence,
            )?;
        }
        if source_rows == 0 {
            return Err(invalid("source is empty"));
        }

        let mut supercells = Vec::new();
        let mut pages = Vec::new();
        let mut assignment_paths = Vec::new();
        let mut maximum_partition_rows = 0_usize;
        let mut expected_supercell = 0_u32;
        for (partition, run_paths) in paths.iter().enumerate() {
            let mut partition_rows = Vec::new();
            for path in run_paths {
                partition_rows.extend(read_routed_ipc(path)?);
                evidence.remove(path)?;
            }
            maximum_partition_rows = maximum_partition_rows.max(partition_rows.len());
            partition_rows.sort_unstable_by_key(|row| (row.supercell_ordinal, row.source_ordinal));
            let assignment_path = scratch.join(format!("assignments-p{partition:03}.arrow"));
            let assignment_file = File::create(&assignment_path)
                .map_err(|error| io_error(&assignment_path, error))?;
            let mut assignment_writer =
                FileWriter::try_new(assignment_file, &v23_row_page_schema())?;
            let mut offset = 0_usize;
            while offset < partition_rows.len() {
                let supercell = partition_rows[offset].supercell_ordinal;
                if supercell != expected_supercell {
                    return Err(invalid("supercell population is missing"));
                }
                let end = partition_rows[offset..]
                    .partition_point(|row| row.supercell_ordinal == supercell)
                    + offset;
                let group = partition_rows[offset..end].to_vec();
                if group
                    .windows(2)
                    .any(|pair| pair[0].source_ordinal >= pair[1].source_ordinal)
                {
                    return Err(invalid("source ordinal duplicates"));
                }
                let page_count = group
                    .len()
                    .div_ceil(usize::from(shape.primary_rows_per_page));
                let first_page = u32::try_from(pages.len())
                    .map_err(|_| invalid("first page ordinal overflows"))?;
                let (supercell_centroid, supercell_radius) = stored_centroid_and_radius(&group)?;
                supercells.push(V23SupercellRow {
                    supercell_ordinal: supercell,
                    centroid: supercell_centroid,
                    cosine_radius: supercell_radius,
                    primary_rows: u64::try_from(group.len())
                        .map_err(|_| invalid("supercell population overflows"))?,
                    first_page,
                    page_count: u32::try_from(page_count)
                        .map_err(|_| invalid("supercell page count overflows"))?,
                });
                let groups = partition_pages(group, page_count)?;
                let mut assignments = Vec::with_capacity(end - offset);
                for members in groups {
                    let page_ordinal = u32::try_from(pages.len())
                        .map_err(|_| invalid("page ordinal overflows"))?;
                    let (centroid, cosine_radius) = stored_centroid_and_radius(&members)?;
                    pages.push(V23PageRow {
                        page_ordinal,
                        supercell_ordinal: supercell,
                        primary_rows: u16::try_from(members.len())
                            .map_err(|_| invalid("page population overflows"))?,
                        replica_rows: 0,
                        centroid,
                        cosine_radius,
                    });
                    assignments.extend(members.into_iter().map(|row| V23RowPage {
                        source_ordinal: row.source_ordinal,
                        primary_page: page_ordinal,
                        replica_page: u32::MAX,
                    }));
                }
                assignments.sort_unstable_by_key(|row| row.source_ordinal);
                assignment_writer.write(&assignment_batch(&assignments)?)?;
                expected_supercell += 1;
                offset = end;
            }
            assignment_writer.finish()?;
            let bytes = fs::metadata(&assignment_path)
                .map_err(|error| io_error(&assignment_path, error))?
                .len();
            evidence.add(bytes)?;
            assignment_paths.push(assignment_path);
        }
        if expected_supercell != shape.supercells {
            return Err(invalid("supercell count differs"));
        }
        let emitted_rows = merge_assignments(&assignment_paths, output)?;
        if emitted_rows != source_rows {
            return Err(invalid("assignment output count differs"));
        }
        for path in &assignment_paths {
            evidence.remove(path)?;
        }
        if evidence.current_bytes != 0 {
            return Err(invalid("scratch bytes remain after merge"));
        }
        let row_pages = identity_for_path(output, output_uri)?;
        Ok(V23PrimaryPageBuild {
            supercells,
            pages,
            row_pages,
            source_rows,
            maximum_resident_rows: u64::try_from(
                maximum_partition_rows
                    .saturating_mul(2)
                    .saturating_add(shape.run_rows),
            )
            .map_err(|_| invalid("resident rows overflow"))?,
            scratch_bytes_peak: evidence.peak_bytes,
        })
    })();
    if result.is_err() {
        for entry in scratch
            .read_dir()
            .map_err(|error| io_error(scratch, error))?
        {
            let path = entry.map_err(|error| io_error(scratch, error))?.path();
            if path.is_file() {
                fs::remove_file(&path).map_err(|error| io_error(&path, error))?;
            }
        }
        if output.is_file() {
            fs::remove_file(output).map_err(|error| io_error(output, error))?;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use crate::v23_balanced_pages_arrow::read_v23_row_pages;

    use super::{V23PageBuildShape, V23RoutedRow, build_v23_primary_pages};

    fn vector(source_ordinal: u64, supercell: u32) -> [f32; 96] {
        let mut vector = [0.0_f32; 96];
        vector[usize::try_from(supercell).unwrap()] = 1.0;
        vector[8 + usize::try_from(source_ordinal % 8).unwrap()] = 0.25;
        vector
    }

    fn routed_rows() -> Vec<V23RoutedRow> {
        let mut rows = (0_u64..20)
            .map(|source_ordinal| {
                let supercell_ordinal = u32::try_from(source_ordinal % 2).unwrap();
                V23RoutedRow {
                    supercell_ordinal,
                    source_ordinal,
                    vector: vector(source_ordinal, supercell_ordinal),
                }
            })
            .collect::<Vec<_>>();
        rows.reverse();
        rows
    }

    fn shape() -> V23PageBuildShape {
        V23PageBuildShape {
            supercells: 2,
            primary_rows_per_page: 4,
            run_rows: 3,
        }
    }

    #[test]
    fn v23_balanced_build_pages_are_complete_balanced_and_bounded() {
        let root = tempfile::tempdir().unwrap();
        let scratch = root.path().join("scratch");
        let output = root.path().join("row-pages.parquet");
        std::fs::create_dir(&scratch).unwrap();
        let built = build_v23_primary_pages(
            routed_rows().into_iter().map(Ok),
            shape(),
            1,
            &scratch,
            &output,
            "s3://borsuk-v23-eu-west-1/frozen/row-pages-parquet",
        )
        .unwrap();

        assert_eq!(built.source_rows, 20);
        assert_eq!(built.supercells.len(), 2);
        assert_eq!(
            built
                .supercells
                .iter()
                .map(|supercell| (
                    supercell.supercell_ordinal,
                    supercell.primary_rows,
                    supercell.first_page,
                    supercell.page_count,
                ))
                .collect::<Vec<_>>(),
            [(0, 10, 0, 3), (1, 10, 3, 3)]
        );
        assert!(built.supercells.iter().all(|supercell| {
            supercell.cosine_radius.is_finite() && supercell.cosine_radius > 0.0
        }));
        assert_eq!(built.pages.len(), 6);
        assert_eq!(
            built
                .pages
                .iter()
                .map(|page| (page.supercell_ordinal, page.primary_rows))
                .collect::<Vec<_>>(),
            [(0, 4), (0, 3), (0, 3), (1, 4), (1, 3), (1, 3)]
        );
        assert!(built.maximum_resident_rows <= 23);
        assert!(scratch.read_dir().unwrap().next().is_none());
        let assignments = read_v23_row_pages(
            &output,
            &built.row_pages,
            "row-pages-primary-parquet",
            u32::try_from(built.pages.len()).unwrap(),
        )
        .unwrap();
        assert_eq!(assignments.len(), 20);
        assert_eq!(
            assignments
                .iter()
                .map(|row| row.source_ordinal)
                .collect::<Vec<_>>(),
            (0_u64..20).collect::<Vec<_>>()
        );
        assert!(assignments.iter().all(|row| row.replica_page == u32::MAX));
        assert_eq!(built.row_pages.role, "row-pages-primary-parquet");
    }

    #[test]
    fn v23_balanced_build_is_worker_deterministic_and_cleans_failure_runs() {
        let left_root = tempfile::tempdir().unwrap();
        let right_root = tempfile::tempdir().unwrap();
        let left_scratch = left_root.path().join("scratch");
        let right_scratch = right_root.path().join("scratch");
        std::fs::create_dir(&left_scratch).unwrap();
        std::fs::create_dir(&right_scratch).unwrap();
        let left = build_v23_primary_pages(
            routed_rows().into_iter().map(Ok),
            shape(),
            1,
            &left_scratch,
            &left_root.path().join("rows.parquet"),
            "s3://borsuk-v23-eu-west-1/frozen/row-pages-parquet",
        )
        .unwrap();
        let right = build_v23_primary_pages(
            routed_rows().into_iter().map(Ok),
            shape(),
            4,
            &right_scratch,
            &right_root.path().join("rows.parquet"),
            "s3://borsuk-v23-eu-west-1/frozen/row-pages-parquet",
        )
        .unwrap();
        assert_eq!(left.pages, right.pages);
        assert_eq!(left.supercells, right.supercells);
        assert_eq!(left.row_pages.digest, right.row_pages.digest);

        let mut duplicate = routed_rows();
        duplicate.push(duplicate[0].clone());
        let failed_scratch = left_root.path().join("failed-scratch");
        std::fs::create_dir(&failed_scratch).unwrap();
        assert!(
            build_v23_primary_pages(
                duplicate.into_iter().map(Ok),
                shape(),
                2,
                &failed_scratch,
                &left_root.path().join("failed.parquet"),
                "s3://borsuk-v23-eu-west-1/frozen/row-pages-parquet",
            )
            .is_err()
        );
        assert!(failed_scratch.read_dir().unwrap().next().is_none());

        let mut gap = routed_rows();
        gap[0].source_ordinal = 21;
        let gap_scratch = right_root.path().join("gap-scratch");
        std::fs::create_dir(&gap_scratch).unwrap();
        assert!(
            build_v23_primary_pages(
                gap.into_iter().map(Ok),
                shape(),
                2,
                &gap_scratch,
                &right_root.path().join("gap.parquet"),
                "s3://borsuk-v23-eu-west-1/frozen/row-pages-primary-parquet",
            )
            .is_err()
        );
        assert!(gap_scratch.read_dir().unwrap().next().is_none());
    }
}
