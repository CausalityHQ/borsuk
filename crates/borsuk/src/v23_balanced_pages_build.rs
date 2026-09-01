use std::{
    cmp::{Ordering, Reverse},
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
    v23_balanced_pages::{V23BalancedArmConfig, V23BalancedIdentity},
    v23_balanced_pages_arrow::{
        V23PageRow, V23RowPage, V23SupercellRow, open_v23_row_pages, v23_row_page_schema,
        validate_v23_balanced_page_geometry,
    },
    v23_incidence_tree::normalize_v23_incidence_vector,
};

const DIMENSIONS: i32 = 96;
const MAX_PARTITIONS: usize = 256;
const CANDIDATE_MERGE_FAN_IN: usize = 64;
const OUTPUT_BATCH_ROWS: usize = 65_536;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23RoutedRow {
    pub(crate) supercell_ordinal: u32,
    pub(crate) runner_up_supercell_ordinal: u32,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23ReplicaArmOutput {
    pub(crate) config: V23BalancedArmConfig,
    pub(crate) row_pages_path: PathBuf,
    pub(crate) row_pages_uri: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct V23ReplicaBuildInputs<'a> {
    pub(crate) primary_path: &'a Path,
    pub(crate) primary_identity: &'a V23BalancedIdentity,
    pub(crate) supercells: &'a [V23SupercellRow],
    pub(crate) pages: &'a [V23PageRow],
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23ReplicaArmBuild {
    pub(crate) config: V23BalancedArmConfig,
    pub(crate) pages: Vec<V23PageRow>,
    pub(crate) row_pages: V23BalancedIdentity,
    pub(crate) replica_rows: u64,
    pub(crate) selection_bytes: u64,
    pub(crate) scratch_bytes_peak: u64,
}

#[derive(Debug, Clone, Copy)]
struct ReplicaCandidate {
    ratio: f32,
    source_ordinal: u64,
    primary_page: u32,
    replica_page: u32,
}

impl PartialEq for ReplicaCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.ratio.to_bits() == other.ratio.to_bits()
            && self.source_ordinal == other.source_ordinal
            && self.primary_page == other.primary_page
            && self.replica_page == other.replica_page
    }
}

impl Eq for ReplicaCandidate {}

impl PartialOrd for ReplicaCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReplicaCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ratio
            .total_cmp(&other.ratio)
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
            .then_with(|| self.primary_page.cmp(&other.primary_page))
            .then_with(|| self.replica_page.cmp(&other.replica_page))
    }
}

struct V23ReplicaSelection {
    decisions: Vec<[u32; 4]>,
    targets: [u64; 3],
    selected_counts: [u64; 3],
    page_counts: Vec<Vec<u16>>,
    page_caps: [u16; 3],
}

type V23ReplicaSelectionResult = (Vec<[u32; 4]>, [u64; 3], Vec<Vec<u16>>);

impl V23ReplicaSelection {
    fn new(source_rows: u64, page_count: usize, outputs: &[V23ReplicaArmOutput]) -> Result<Self> {
        if !replica_arm_outputs_are_exact(outputs) || source_rows == 0 || page_count == 0 {
            return Err(invalid("replica selection boundary differs"));
        }
        let source_count =
            usize::try_from(source_rows).map_err(|_| invalid("replica source count overflows"))?;
        let mut decisions = Vec::new();
        decisions
            .try_reserve_exact(source_count)
            .map_err(|_| invalid("replica selection memory unavailable"))?;
        decisions.resize(source_count, [u32::MAX; 4]);
        let mut targets = [0_u64; 3];
        for arm in 0..3 {
            targets[arm] = source_rows
                .checked_mul(outputs[arm].config.amplification_ppm - 1_000_000)
                .ok_or_else(|| invalid("replica target count overflows"))?
                / 1_000_000;
        }
        let mut page_counts = Vec::new();
        page_counts
            .try_reserve_exact(3)
            .map_err(|_| invalid("replica page count memory unavailable"))?;
        for _ in 0..3 {
            let mut counts = Vec::new();
            counts
                .try_reserve_exact(page_count)
                .map_err(|_| invalid("replica page count memory unavailable"))?;
            counts.resize(page_count, 0);
            page_counts.push(counts);
        }
        Ok(Self {
            decisions,
            targets,
            selected_counts: [0; 3],
            page_counts,
            page_caps: std::array::from_fn(|arm| outputs[arm].config.replicas_per_page),
        })
    }

    fn consider(&mut self, candidate: ReplicaCandidate) -> Result<()> {
        let source = usize::try_from(candidate.source_ordinal)
            .map_err(|_| invalid("candidate source overflows"))?;
        let replica_page = usize::try_from(candidate.replica_page)
            .map_err(|_| invalid("candidate page overflows"))?;
        let decision = self
            .decisions
            .get_mut(source)
            .ok_or_else(|| invalid("candidate source is out of range"))?;
        if replica_page >= self.page_counts[0].len()
            || (decision[3] != u32::MAX && decision[3] != candidate.primary_page)
        {
            return Err(invalid("candidate authority is out of range"));
        }
        decision[3] = candidate.primary_page;
        for (arm, replica) in decision.iter_mut().take(3).enumerate() {
            if self.selected_counts[arm] < self.targets[arm]
                && self.page_counts[arm][replica_page] < self.page_caps[arm]
            {
                *replica = candidate.replica_page;
                self.page_counts[arm][replica_page] += 1;
                self.selected_counts[arm] += 1;
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<V23ReplicaSelectionResult> {
        if self
            .decisions
            .iter()
            .any(|decision| decision[3] == u32::MAX)
        {
            return Err(invalid("replica primary decisions are incomplete"));
        }
        Ok((self.decisions, self.selected_counts, self.page_counts))
    }
}

fn validated_cosine_distance(dot_product: f32) -> Result<f32> {
    let distance = 1.0 - dot_product;
    if !distance.is_finite() || distance < -(16.0 * f32::EPSILON) {
        return Err(invalid("replica page distance differs"));
    }
    Ok(distance.max(0.0))
}

fn update_routed_replay_digest(digest: &mut Sha256, row: &V23RoutedRow) -> Result<()> {
    digest.update(row.source_ordinal.to_le_bytes());
    digest.update(row.supercell_ordinal.to_le_bytes());
    digest.update(row.runner_up_supercell_ordinal.to_le_bytes());
    for value in normalize_v23_incidence_vector(&row.vector)? {
        digest.update(value.to_bits().to_le_bytes());
    }
    Ok(())
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
        Field::new("runner_up_supercell_ordinal", DataType::UInt32, false),
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

fn candidate_schema() -> Schema {
    Schema::new(vec![
        Field::new("ratio", DataType::Float32, false),
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

fn candidate_batch(rows: &[ReplicaCandidate]) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(candidate_schema()),
        vec![
            Arc::new(Float32Array::from_iter_values(
                rows.iter().map(|row| row.ratio),
            )),
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
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.runner_up_supercell_ordinal),
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

fn flush_candidate_run(
    candidates: &mut Vec<ReplicaCandidate>,
    run: usize,
    scratch: &Path,
    paths: &mut Vec<PathBuf>,
    evidence: &mut ScratchEvidence,
) -> Result<()> {
    candidates.sort_unstable();
    let path = scratch.join(format!("replica-candidates-r{run:08}.arrow"));
    let bytes = write_ipc(&path, &candidate_schema(), &[candidate_batch(candidates)?])?;
    evidence.add(bytes)?;
    paths.push(path);
    candidates.clear();
    Ok(())
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
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("scratch ordinal column differs"))?;
        let vectors = batch
            .column(3)
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
                runner_up_supercell_ordinal: batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| invalid("scratch runner-up column differs"))?
                    .value(row),
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

fn identity_for_path(path: &Path, uri: &str, role: &str) -> Result<V23BalancedIdentity> {
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
        role: role.to_owned(),
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
        if batch.as_ref().is_some_and(|batch| batch.num_rows() == 0) {
            return Err(invalid("assignment scratch batch is empty"));
        }
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
            if self
                .batch
                .as_ref()
                .is_some_and(|batch| batch.num_rows() == 0)
            {
                return Err(invalid("assignment scratch batch is empty"));
            }
            self.row = 0;
        }
        Ok(())
    }
}

struct CandidateCursor {
    reader: FileReader<File>,
    batch: Option<RecordBatch>,
    row: usize,
}

impl CandidateCursor {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| io_error(path, error))?;
        let mut reader = FileReader::try_new(file, None)?;
        if reader.schema().as_ref() != &candidate_schema() {
            return Err(invalid("candidate scratch schema differs"));
        }
        let batch = reader.next().transpose()?;
        if batch.as_ref().is_some_and(|batch| batch.num_rows() == 0) {
            return Err(invalid("candidate scratch batch is empty"));
        }
        Ok(Self {
            reader,
            batch,
            row: 0,
        })
    }

    fn current(&self) -> Result<Option<ReplicaCandidate>> {
        let Some(batch) = &self.batch else {
            return Ok(None);
        };
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("candidate scratch nullability differs"));
        }
        let ratio = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("candidate ratio column differs"))?
            .value(self.row);
        let source_ordinal = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("candidate source column differs"))?
            .value(self.row);
        let primary_page = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("candidate primary column differs"))?
            .value(self.row);
        let replica_page = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("candidate replica column differs"))?
            .value(self.row);
        if !ratio.is_finite() || ratio < 0.0 || primary_page == replica_page {
            return Err(invalid("candidate row differs"));
        }
        Ok(Some(ReplicaCandidate {
            ratio,
            source_ordinal,
            primary_page,
            replica_page,
        }))
    }

    fn advance(&mut self) -> Result<()> {
        let Some(batch) = &self.batch else {
            return Ok(());
        };
        self.row += 1;
        if self.row == batch.num_rows() {
            self.batch = self.reader.next().transpose()?;
            if self
                .batch
                .as_ref()
                .is_some_and(|batch| batch.num_rows() == 0)
            {
                return Err(invalid("candidate scratch batch is empty"));
            }
            self.row = 0;
        }
        Ok(())
    }
}

fn merge_candidate_group(paths: &[PathBuf], output: &Path) -> Result<u64> {
    if paths.is_empty() || paths.len() > CANDIDATE_MERGE_FAN_IN {
        return Err(invalid("candidate merge fan-in differs"));
    }
    let mut cursors = paths
        .iter()
        .map(|path| CandidateCursor::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, cursor) in cursors.iter().enumerate() {
        if let Some(candidate) = cursor.current()? {
            heap.push(Reverse((candidate, index)));
        }
    }
    let file = File::create(output).map_err(|error| io_error(output, error))?;
    let mut writer = FileWriter::try_new(file, &candidate_schema())?;
    let mut buffer = Vec::with_capacity(OUTPUT_BATCH_ROWS);
    while let Some(Reverse((candidate, cursor_index))) = heap.pop() {
        buffer.push(candidate);
        cursors[cursor_index].advance()?;
        if let Some(next) = cursors[cursor_index].current()? {
            heap.push(Reverse((next, cursor_index)));
        }
        if buffer.len() == OUTPUT_BATCH_ROWS {
            writer.write(&candidate_batch(&buffer)?)?;
            buffer.clear();
        }
    }
    if !buffer.is_empty() {
        writer.write(&candidate_batch(&buffer)?)?;
    }
    writer.finish()?;
    fs::metadata(output)
        .map(|metadata| metadata.len())
        .map_err(|error| io_error(output, error))
}

fn collapse_candidate_runs(
    paths: &mut Vec<PathBuf>,
    scratch: &Path,
    evidence: &mut ScratchEvidence,
) -> Result<()> {
    let mut pass = 0_usize;
    while paths.len() > CANDIDATE_MERGE_FAN_IN {
        let mut merged: Vec<PathBuf> = Vec::new();
        for (group, chunk) in paths.chunks(CANDIDATE_MERGE_FAN_IN).enumerate() {
            let output = scratch.join(format!("replica-merge-p{pass:03}-g{group:08}.arrow"));
            let bytes = match merge_candidate_group(chunk, &output) {
                Ok(bytes) => bytes,
                Err(error) => {
                    if output.is_file() {
                        fs::remove_file(&output).map_err(|source| io_error(&output, source))?;
                    }
                    for path in &merged {
                        if path.is_file() {
                            fs::remove_file(path).map_err(|source| io_error(path, source))?;
                        }
                    }
                    return Err(error);
                }
            };
            if let Err(error) = evidence.add(bytes) {
                if output.is_file() {
                    fs::remove_file(&output).map_err(|source| io_error(&output, source))?;
                }
                for path in &merged {
                    if path.is_file() {
                        fs::remove_file(path).map_err(|source| io_error(path, source))?;
                    }
                }
                return Err(error);
            }
            merged.push(output);
        }
        for path in paths.iter() {
            if let Err(error) = evidence.remove(path) {
                for merged_path in &merged {
                    if merged_path.is_file() {
                        fs::remove_file(merged_path)
                            .map_err(|source| io_error(merged_path, source))?;
                    }
                }
                return Err(error);
            }
        }
        *paths = merged;
        pass += 1;
    }
    Ok(())
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

fn replica_arm_outputs_are_exact(outputs: &[V23ReplicaArmOutput]) -> bool {
    outputs.len() == 3
        && outputs
            .iter()
            .map(|output| {
                (
                    output.config.name.as_str(),
                    output.config.amplification_ppm,
                    output.config.replicas_per_page,
                )
            })
            .eq([
                ("amp-1125", 1_125_000, 48),
                ("amp-1250", 1_250_000, 96),
                ("amp-1500", 1_500_000, 192),
            ])
}

fn replica_candidate(
    row: &V23RoutedRow,
    primary: V23RowPage,
    supercells: &[V23SupercellRow],
    pages: &[V23PageRow],
    page_centroids: &[[f32; 96]],
) -> Result<ReplicaCandidate> {
    let vector = normalize_v23_incidence_vector(&row.vector)?;
    let mut ranked = Vec::new();
    for supercell_ordinal in [row.supercell_ordinal, row.runner_up_supercell_ordinal] {
        let supercell = supercells
            .get(usize::try_from(supercell_ordinal).unwrap())
            .filter(|value| value.supercell_ordinal == supercell_ordinal)
            .ok_or_else(|| invalid("replica supercell authority differs"))?;
        let end = supercell
            .first_page
            .checked_add(supercell.page_count)
            .ok_or_else(|| invalid("replica supercell page range overflows"))?;
        for page_ordinal in supercell.first_page..end {
            let page = pages
                .get(usize::try_from(page_ordinal).unwrap())
                .filter(|value| {
                    value.page_ordinal == page_ordinal
                        && value.supercell_ordinal == supercell_ordinal
                })
                .ok_or_else(|| invalid("replica page authority differs"))?;
            let distance = validated_cosine_distance(dot(
                &vector,
                page_centroids
                    .get(usize::try_from(page.page_ordinal).unwrap())
                    .ok_or_else(|| invalid("replica page centroid is missing"))?,
            ))?;
            ranked.push((distance, page.page_ordinal));
        }
    }
    ranked.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let primary_distance = ranked
        .iter()
        .find(|entry| entry.1 == primary.primary_page)
        .map(|entry| entry.0)
        .ok_or_else(|| invalid("replica primary page authority differs"))?;
    if pages[usize::try_from(primary.primary_page).unwrap()].supercell_ordinal
        != row.supercell_ordinal
    {
        return Err(invalid("replica primary supercell authority differs"));
    }
    let alternate = ranked
        .iter()
        .copied()
        .find(|entry| entry.1 != primary.primary_page)
        .ok_or_else(|| invalid("replica alternate page is missing"))?;
    let ratio = alternate.0 / primary_distance.max(f32::MIN_POSITIVE);
    if !ratio.is_finite() || ratio < 0.0 {
        return Err(invalid("replica margin ratio differs"));
    }
    Ok(ReplicaCandidate {
        ratio,
        source_ordinal: row.source_ordinal,
        primary_page: primary.primary_page,
        replica_page: alternate.1,
    })
}

pub(crate) fn build_v23_replica_arms<F, I>(
    mut rows: F,
    inputs: V23ReplicaBuildInputs<'_>,
    outputs: &[V23ReplicaArmOutput],
    scratch: &Path,
    run_rows: usize,
) -> Result<Vec<V23ReplicaArmBuild>>
where
    F: FnMut() -> Result<I>,
    I: IntoIterator<Item = Result<V23RoutedRow>>,
{
    if !replica_arm_outputs_are_exact(outputs)
        || inputs.supercells.is_empty()
        || inputs.pages.is_empty()
        || run_rows == 0
        || !scratch.is_dir()
        || scratch
            .read_dir()
            .map_err(|error| io_error(scratch, error))?
            .next()
            .is_some()
        || outputs.iter().any(|output| {
            output.row_pages_path.exists()
                || !output.row_pages_uri.starts_with("s3://")
                || output.row_pages_uri.ends_with('/')
        })
        || outputs.iter().enumerate().any(|(index, output)| {
            outputs[..index].iter().any(|earlier| {
                earlier.row_pages_path == output.row_pages_path
                    || earlier.row_pages_uri == output.row_pages_uri
            })
        })
    {
        return Err(invalid("replica construction boundary differs"));
    }
    let page_count =
        u32::try_from(inputs.pages.len()).map_err(|_| invalid("page count overflows"))?;
    let supercell_count =
        u32::try_from(inputs.supercells.len()).map_err(|_| invalid("supercell count overflows"))?;
    validate_v23_balanced_page_geometry(inputs.supercells, inputs.pages, 0)?;
    let mut page_centroids = Vec::new();
    page_centroids
        .try_reserve_exact(inputs.pages.len())
        .map_err(|_| invalid("primary centroid memory unavailable"))?;
    for page in inputs.pages {
        page_centroids.push(normalize_v23_incidence_vector(
            &page.centroid.map(f16::to_f32),
        )?);
    }
    let mut primary_stream = open_v23_row_pages(
        inputs.primary_path,
        inputs.primary_identity,
        "row-pages-primary-parquet",
        page_count,
    )?;
    let mut evidence = ScratchEvidence {
        current_bytes: 0,
        peak_bytes: 0,
    };
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(run_rows)
        .map_err(|_| invalid("replica candidate memory unavailable"))?;
    let mut candidate_paths = Vec::new();
    let mut primary_counts = Vec::new();
    primary_counts
        .try_reserve_exact(inputs.pages.len())
        .map_err(|_| invalid("primary page count memory unavailable"))?;
    primary_counts.resize(inputs.pages.len(), 0_u64);
    let mut source_rows = 0_u64;
    let mut run = 0_usize;
    let result = (|| {
        let mut routed_digest = Sha256::new();
        for row in rows()? {
            let row = row?;
            if row.source_ordinal != source_rows
                || row.supercell_ordinal >= supercell_count
                || row.runner_up_supercell_ordinal >= supercell_count
                || row.runner_up_supercell_ordinal == row.supercell_ordinal
            {
                return Err(invalid("replica routed row authority differs"));
            }
            update_routed_replay_digest(&mut routed_digest, &row)?;
            let primary = primary_stream
                .next()
                .transpose()?
                .ok_or_else(|| invalid("primary assignments end early"))?;
            if primary.source_ordinal != row.source_ordinal || primary.replica_page != u32::MAX {
                return Err(invalid("primary assignment authority differs"));
            }
            let primary_count = primary_counts
                .get_mut(usize::try_from(primary.primary_page).unwrap())
                .ok_or_else(|| invalid("primary assignment page is out of range"))?;
            *primary_count = primary_count
                .checked_add(1)
                .ok_or_else(|| invalid("primary page count overflows"))?;
            candidates.push(replica_candidate(
                &row,
                primary,
                inputs.supercells,
                inputs.pages,
                &page_centroids,
            )?);
            source_rows = source_rows
                .checked_add(1)
                .ok_or_else(|| invalid("replica source count overflows"))?;
            if candidates.len() == run_rows {
                flush_candidate_run(
                    &mut candidates,
                    run,
                    scratch,
                    &mut candidate_paths,
                    &mut evidence,
                )?;
                run += 1;
            }
        }
        if primary_stream.next().transpose()?.is_some() || source_rows == 0 {
            return Err(invalid("primary assignment count differs"));
        }
        if primary_counts
            .iter()
            .zip(inputs.pages)
            .any(|(observed, page)| *observed != u64::from(page.primary_rows))
        {
            return Err(invalid("primary page count reconciliation differs"));
        }
        let routed_digest: [u8; 32] = routed_digest.finalize().into();
        if !candidates.is_empty() {
            flush_candidate_run(
                &mut candidates,
                run,
                scratch,
                &mut candidate_paths,
                &mut evidence,
            )?;
        }
        collapse_candidate_runs(&mut candidate_paths, scratch, &mut evidence)?;
        drop(candidates);

        let selection_bytes = source_rows
            .checked_mul(u64::try_from(std::mem::size_of::<[u32; 4]>()).unwrap())
            .ok_or_else(|| invalid("replica selection bytes overflow"))?;
        let mut selection = V23ReplicaSelection::new(source_rows, inputs.pages.len(), outputs)?;
        let mut cursors = candidate_paths
            .iter()
            .map(|path| CandidateCursor::open(path))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (index, cursor) in cursors.iter().enumerate() {
            if let Some(candidate) = cursor.current()? {
                heap.push(Reverse((candidate, index)));
            }
        }
        while let Some(Reverse((candidate, cursor_index))) = heap.pop() {
            selection.consider(candidate)?;
            cursors[cursor_index].advance()?;
            if let Some(next) = cursors[cursor_index].current()? {
                heap.push(Reverse((next, cursor_index)));
            }
        }
        let (selected, selected_counts, page_counts) = selection.finish()?;
        for path in &candidate_paths {
            evidence.remove(path)?;
        }
        if evidence.current_bytes != 0 {
            return Err(invalid("replica scratch remains"));
        }

        let mut sums: [Vec<[f64; 96]>; 3] = std::array::from_fn(|_| Vec::new());
        for arm_sums in &mut sums {
            arm_sums
                .try_reserve_exact(inputs.pages.len())
                .map_err(|_| invalid("replica centroid sum memory unavailable"))?;
            arm_sums.resize(inputs.pages.len(), [0.0; 96]);
        }
        let mut replayed = 0_u64;
        let mut replay_digest = Sha256::new();
        for row in rows()? {
            let row = row?;
            if row.source_ordinal != replayed {
                return Err(invalid("replica centroid replay order differs"));
            }
            update_routed_replay_digest(&mut replay_digest, &row)?;
            let decision = selected
                .get(usize::try_from(row.source_ordinal).unwrap())
                .ok_or_else(|| invalid("replica centroid decision is missing"))?;
            let primary = usize::try_from(decision[3]).unwrap();
            if inputs
                .pages
                .get(primary)
                .filter(|page| page.supercell_ordinal == row.supercell_ordinal)
                .is_none()
            {
                return Err(invalid("replica centroid primary authority differs"));
            }
            let vector = normalize_v23_incidence_vector(&row.vector)?;
            for arm in 0..3 {
                for page in [decision[3], decision[arm]] {
                    if page == u32::MAX {
                        continue;
                    }
                    let sum = sums[arm]
                        .get_mut(usize::try_from(page).unwrap())
                        .ok_or_else(|| invalid("replica centroid page is out of range"))?;
                    for (target, value) in sum.iter_mut().zip(vector) {
                        *target += f64::from(value);
                    }
                }
            }
            replayed += 1;
        }
        if replayed != source_rows || <[u8; 32]>::from(replay_digest.finalize()) != routed_digest {
            return Err(invalid("replica centroid replay count differs"));
        }

        let mut arm_pages: [Vec<V23PageRow>; 3] = std::array::from_fn(|_| Vec::new());
        for arm in 0..3 {
            arm_pages[arm]
                .try_reserve_exact(inputs.pages.len())
                .map_err(|_| invalid("replica page table memory unavailable"))?;
            for (page, count) in inputs.pages.iter().cloned().zip(&page_counts[arm]) {
                let mut page = page;
                page.replica_rows = *count;
                arm_pages[arm].push(page);
            }
        }
        for arm in 0..3 {
            for (ordinal, page) in arm_pages[arm].iter_mut().enumerate() {
                let occurrences = u64::from(page.primary_rows) + u64::from(page.replica_rows);
                let mean = sums[arm][ordinal].map(|value| (value / occurrences as f64) as f32);
                page.centroid = normalize_v23_incidence_vector(&mean)?.map(f16::from_f32);
                page.cosine_radius = 0.0;
            }
        }
        drop(sums);
        let mut arm_centroids: [Vec<[f32; 96]>; 3] = std::array::from_fn(|_| Vec::new());
        for arm in 0..3 {
            arm_centroids[arm]
                .try_reserve_exact(inputs.pages.len())
                .map_err(|_| invalid("replica centroid memory unavailable"))?;
            for page in &arm_pages[arm] {
                arm_centroids[arm].push(normalize_v23_incidence_vector(
                    &page.centroid.map(f16::to_f32),
                )?);
            }
        }
        let mut writers = outputs
            .iter()
            .map(|output| {
                let file = File::create(&output.row_pages_path)
                    .map_err(|error| io_error(&output.row_pages_path, error))?;
                ArrowWriter::try_new(file, Arc::new(v23_row_page_schema()), None)
                    .map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut buffers: [Vec<V23RowPage>; 3] = std::array::from_fn(|_| Vec::new());
        for buffer in &mut buffers {
            buffer
                .try_reserve_exact(OUTPUT_BATCH_ROWS)
                .map_err(|_| invalid("replica output buffer memory unavailable"))?;
        }
        replayed = 0;
        let mut replay_digest = Sha256::new();
        for row in rows()? {
            let row = row?;
            if row.source_ordinal != replayed {
                return Err(invalid("replica radius replay order differs"));
            }
            update_routed_replay_digest(&mut replay_digest, &row)?;
            let decision = selected
                .get(usize::try_from(row.source_ordinal).unwrap())
                .ok_or_else(|| invalid("replica radius decision is missing"))?;
            let vector = normalize_v23_incidence_vector(&row.vector)?;
            for arm in 0..3 {
                for page in [decision[3], decision[arm]] {
                    if page == u32::MAX {
                        continue;
                    }
                    let page_index = usize::try_from(page).unwrap();
                    let distance =
                        validated_cosine_distance(dot(&vector, &arm_centroids[arm][page_index]))?;
                    arm_pages[arm][page_index].cosine_radius =
                        arm_pages[arm][page_index].cosine_radius.max(distance);
                }
                buffers[arm].push(V23RowPage {
                    source_ordinal: row.source_ordinal,
                    primary_page: decision[3],
                    replica_page: decision[arm],
                });
                if buffers[arm].len() == OUTPUT_BATCH_ROWS {
                    writers[arm].write(&assignment_batch(&buffers[arm])?)?;
                    buffers[arm].clear();
                }
            }
            replayed += 1;
        }
        if replayed != source_rows || <[u8; 32]>::from(replay_digest.finalize()) != routed_digest {
            return Err(invalid("replica radius replay count differs"));
        }
        for arm in 0..3 {
            if !buffers[arm].is_empty() {
                writers[arm].write(&assignment_batch(&buffers[arm])?)?;
            }
        }
        for writer in writers {
            writer.close()?;
        }

        let mut builds = Vec::with_capacity(3);
        for arm in 0..3 {
            let row_pages = identity_for_path(
                &outputs[arm].row_pages_path,
                &outputs[arm].row_pages_uri,
                &format!("row-pages-{}-parquet", outputs[arm].config.name),
            )?;
            builds.push(V23ReplicaArmBuild {
                config: outputs[arm].config.clone(),
                pages: std::mem::take(&mut arm_pages[arm]),
                row_pages,
                replica_rows: selected_counts[arm],
                selection_bytes,
                scratch_bytes_peak: evidence.peak_bytes,
            });
        }
        Ok(builds)
    })();
    if result.is_err() {
        for entry in fs::read_dir(scratch).map_err(|error| io_error(scratch, error))? {
            let path = entry.map_err(|error| io_error(scratch, error))?.path();
            if !path.is_file() {
                return Err(invalid("replica scratch contains a non-file entry"));
            }
            fs::remove_file(&path).map_err(|error| io_error(&path, error))?;
        }
        for output in outputs {
            if output.row_pages_path.is_file() {
                fs::remove_file(&output.row_pages_path)
                    .map_err(|error| io_error(&output.row_pages_path, error))?;
            }
        }
    }
    result
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
            if row.supercell_ordinal >= shape.supercells
                || row.runner_up_supercell_ordinal >= shape.supercells
                || row.runner_up_supercell_ordinal == row.supercell_ordinal
            {
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
        let row_pages = identity_for_path(output, output_uri, "row-pages-primary-parquet")?;
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
    use crate::{
        v23_balanced_pages::V23BalancedArmConfig,
        v23_balanced_pages_arrow::{read_v23_row_pages, reconcile_v23_balanced_arm},
    };

    use super::{
        CandidateCursor, ReplicaCandidate, ScratchEvidence, V23PageBuildShape, V23ReplicaArmOutput,
        V23ReplicaBuildInputs, V23ReplicaSelection, V23RoutedRow, build_v23_primary_pages,
        build_v23_replica_arms, candidate_batch, candidate_schema, collapse_candidate_runs,
        validated_cosine_distance, write_ipc,
    };

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
                    runner_up_supercell_ordinal: 1 - supercell_ordinal,
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

    #[test]
    fn v23_balanced_build_replica_arms_apply_exact_global_and_page_caps() {
        let root = tempfile::tempdir().unwrap();
        let primary_scratch = root.path().join("primary-scratch");
        let replica_scratch = root.path().join("replica-scratch");
        let primary_path = root.path().join("row-pages-primary.parquet");
        std::fs::create_dir(&primary_scratch).unwrap();
        std::fs::create_dir(&replica_scratch).unwrap();
        let primary = build_v23_primary_pages(
            routed_rows().into_iter().map(Ok),
            shape(),
            2,
            &primary_scratch,
            &primary_path,
            "s3://borsuk-v23-eu-west-1/frozen/row-pages-primary-parquet",
        )
        .unwrap();
        let outputs = [
            ("amp-1125", 1_125_000, 48_u16),
            ("amp-1250", 1_250_000, 96_u16),
            ("amp-1500", 1_500_000, 192_u16),
        ]
        .map(
            |(name, amplification_ppm, replicas_per_page)| V23ReplicaArmOutput {
                config: V23BalancedArmConfig {
                    name: name.to_owned(),
                    amplification_ppm,
                    replicas_per_page,
                },
                row_pages_path: root.path().join(format!("row-pages-{name}.parquet")),
                row_pages_uri: format!("s3://borsuk-v23-eu-west-1/frozen/row-pages-{name}.parquet"),
            },
        );

        let mut replica_rows = routed_rows();
        replica_rows.sort_unstable_by_key(|row| row.source_ordinal);
        let arms = build_v23_replica_arms(
            || Ok(replica_rows.clone().into_iter().map(Ok)),
            V23ReplicaBuildInputs {
                primary_path: &primary_path,
                primary_identity: &primary.row_pages,
                supercells: &primary.supercells,
                pages: &primary.pages,
            },
            &outputs,
            &replica_scratch,
            3,
        )
        .unwrap();
        assert_eq!(
            arms.iter().map(|arm| arm.replica_rows).collect::<Vec<_>>(),
            [2, 5, 10]
        );
        for (arm, output) in arms.iter().zip(&outputs) {
            assert_eq!(arm.selection_bytes, 320);
            assert!(arm.scratch_bytes_peak > 0);
            assert_eq!(
                arm.row_pages.role,
                format!("row-pages-{}-parquet", arm.config.name)
            );
            let assignments = read_v23_row_pages(
                &output.row_pages_path,
                &arm.row_pages,
                &arm.row_pages.role,
                u32::try_from(arm.pages.len()).unwrap(),
            )
            .unwrap();
            assert_eq!(assignments.len(), 20);
            assert_eq!(
                assignments
                    .iter()
                    .filter(|row| row.replica_page != u32::MAX)
                    .count() as u64,
                arm.replica_rows
            );
            reconcile_v23_balanced_arm(
                &primary.supercells,
                &arm.pages,
                &assignments,
                arm.config.replicas_per_page,
            )
            .unwrap();
        }
        assert!(
            arms[2]
                .pages
                .iter()
                .zip(&primary.pages)
                .any(|(arm, primary)| {
                    arm.replica_rows > 0
                        && (arm.centroid != primary.centroid
                            || arm.cosine_radius.to_bits() != primary.cosine_radius.to_bits())
                })
        );
        assert!(replica_scratch.read_dir().unwrap().next().is_none());
    }

    #[test]
    fn v23_balanced_build_replica_caps_allow_geometric_shortfall() {
        let outputs = [
            ("amp-1125", 1_125_000, 48_u16),
            ("amp-1250", 1_250_000, 96_u16),
            ("amp-1500", 1_500_000, 192_u16),
        ]
        .map(
            |(name, amplification_ppm, replicas_per_page)| V23ReplicaArmOutput {
                config: V23BalancedArmConfig {
                    name: name.to_owned(),
                    amplification_ppm,
                    replicas_per_page,
                },
                row_pages_path: std::path::PathBuf::from(format!("{name}.parquet")),
                row_pages_uri: format!("s3://bucket/{name}.parquet"),
            },
        );
        let mut selection = V23ReplicaSelection::new(400, 2, &outputs).unwrap();
        for source_ordinal in 0_u64..400 {
            selection
                .consider(ReplicaCandidate {
                    ratio: source_ordinal as f32,
                    source_ordinal,
                    primary_page: 0,
                    replica_page: 1,
                })
                .unwrap();
        }
        let (_, counts, page_counts) = selection.finish().unwrap();
        assert_eq!(counts, [48, 96, 192]);
        assert_eq!(page_counts[0][1], 48);
        assert_eq!(page_counts[1][1], 96);
        assert_eq!(page_counts[2][1], 192);
    }

    #[test]
    fn v23_balanced_build_replica_rejects_mutated_replay_and_cleans_outputs() {
        let root = tempfile::tempdir().unwrap();
        let primary_scratch = root.path().join("primary-scratch");
        let replica_scratch = root.path().join("replica-scratch");
        let primary_path = root.path().join("row-pages-primary.parquet");
        std::fs::create_dir(&primary_scratch).unwrap();
        std::fs::create_dir(&replica_scratch).unwrap();
        let primary = build_v23_primary_pages(
            routed_rows().into_iter().map(Ok),
            shape(),
            2,
            &primary_scratch,
            &primary_path,
            "s3://borsuk-v23-eu-west-1/frozen/row-pages-primary-parquet",
        )
        .unwrap();
        let outputs = [
            ("amp-1125", 1_125_000, 48_u16),
            ("amp-1250", 1_250_000, 96_u16),
            ("amp-1500", 1_500_000, 192_u16),
        ]
        .map(
            |(name, amplification_ppm, replicas_per_page)| V23ReplicaArmOutput {
                config: V23BalancedArmConfig {
                    name: name.to_owned(),
                    amplification_ppm,
                    replicas_per_page,
                },
                row_pages_path: root.path().join(format!("row-pages-{name}.parquet")),
                row_pages_uri: format!("s3://borsuk-v23-eu-west-1/frozen/row-pages-{name}.parquet"),
            },
        );
        let mut replay = 0;
        let result = build_v23_replica_arms(
            || {
                replay += 1;
                let mut rows = routed_rows();
                rows.sort_unstable_by_key(|row| row.source_ordinal);
                if replay == 2 {
                    rows[0].vector.swap(0, 8);
                }
                Ok(rows.into_iter().map(Ok))
            },
            V23ReplicaBuildInputs {
                primary_path: &primary_path,
                primary_identity: &primary.row_pages,
                supercells: &primary.supercells,
                pages: &primary.pages,
            },
            &outputs,
            &replica_scratch,
            3,
        );
        assert!(result.is_err());
        assert!(replica_scratch.read_dir().unwrap().next().is_none());
        assert!(outputs.iter().all(|output| !output.row_pages_path.exists()));
    }

    #[test]
    fn v23_balanced_build_replica_reconciles_primary_page_counts() {
        let root = tempfile::tempdir().unwrap();
        let primary_scratch = root.path().join("primary-scratch");
        let replica_scratch = root.path().join("replica-scratch");
        let primary_path = root.path().join("row-pages-primary.parquet");
        std::fs::create_dir(&primary_scratch).unwrap();
        std::fs::create_dir(&replica_scratch).unwrap();
        let primary = build_v23_primary_pages(
            routed_rows().into_iter().map(Ok),
            shape(),
            2,
            &primary_scratch,
            &primary_path,
            "s3://borsuk-v23-eu-west-1/frozen/row-pages-primary-parquet",
        )
        .unwrap();
        let mut pages = primary.pages.clone();
        pages[0].primary_rows -= 1;
        pages[1].primary_rows += 1;
        let outputs = [
            ("amp-1125", 1_125_000, 48_u16),
            ("amp-1250", 1_250_000, 96_u16),
            ("amp-1500", 1_500_000, 192_u16),
        ]
        .map(
            |(name, amplification_ppm, replicas_per_page)| V23ReplicaArmOutput {
                config: V23BalancedArmConfig {
                    name: name.to_owned(),
                    amplification_ppm,
                    replicas_per_page,
                },
                row_pages_path: root.path().join(format!("row-pages-{name}.parquet")),
                row_pages_uri: format!("s3://borsuk-v23-eu-west-1/frozen/row-pages-{name}.parquet"),
            },
        );
        let mut replica_rows = routed_rows();
        replica_rows.sort_unstable_by_key(|row| row.source_ordinal);
        let result = build_v23_replica_arms(
            || Ok(replica_rows.clone().into_iter().map(Ok)),
            V23ReplicaBuildInputs {
                primary_path: &primary_path,
                primary_identity: &primary.row_pages,
                supercells: &primary.supercells,
                pages: &pages,
            },
            &outputs,
            &replica_scratch,
            3,
        );
        assert!(result.is_err());
        assert!(replica_scratch.read_dir().unwrap().next().is_none());
        assert!(outputs.iter().all(|output| !output.row_pages_path.exists()));
    }

    #[test]
    fn v23_balanced_build_cosine_distance_clamps_only_roundoff() {
        assert_eq!(validated_cosine_distance(1.0 + f32::EPSILON).unwrap(), 0.0);
        assert_eq!(
            validated_cosine_distance(1.0 + 12.0 * f32::EPSILON).unwrap(),
            0.0
        );
        assert!(validated_cosine_distance(1.001).is_err());
        assert!(validated_cosine_distance(f32::NAN).is_err());
    }

    #[test]
    fn v23_balanced_build_candidate_merge_has_bounded_fan_in() {
        let root = tempfile::tempdir().unwrap();
        let mut paths = Vec::new();
        let mut evidence = ScratchEvidence {
            current_bytes: 0,
            peak_bytes: 0,
        };
        for source_ordinal in (0_u64..65).rev() {
            let path = root
                .path()
                .join(format!("candidate-{source_ordinal:03}.arrow"));
            let candidate = ReplicaCandidate {
                ratio: source_ordinal as f32,
                source_ordinal,
                primary_page: 0,
                replica_page: 1,
            };
            let bytes = write_ipc(
                &path,
                &candidate_schema(),
                &[candidate_batch(&[candidate]).unwrap()],
            )
            .unwrap();
            evidence.add(bytes).unwrap();
            paths.push(path);
        }
        collapse_candidate_runs(&mut paths, root.path(), &mut evidence).unwrap();
        assert_eq!(paths.len(), 2);
        let mut rows = Vec::new();
        for path in &paths {
            let mut cursor = CandidateCursor::open(path).unwrap();
            while let Some(candidate) = cursor.current().unwrap() {
                rows.push(candidate);
                cursor.advance().unwrap();
            }
            evidence.remove(path).unwrap();
        }
        rows.sort_unstable();
        assert_eq!(
            rows.iter()
                .map(|row| row.source_ordinal)
                .collect::<Vec<_>>(),
            (0_u64..65).collect::<Vec<_>>()
        );
        assert_eq!(evidence.current_bytes, 0);
        assert!(root.path().read_dir().unwrap().next().is_none());
    }
}
