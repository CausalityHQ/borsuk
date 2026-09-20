use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
    io::Cursor,
    ops::Range,
    sync::{Arc, RwLock},
};

use arrow_array::{Array, FixedSizeListArray, Int64Array, UInt8Array, UInt64Array};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use sha2::{Digest, Sha256};

use crate::{
    error::{BorsukError, Result},
    native_ann_format::NativeRouterArtifacts,
    native_ann_router::{NativeRouteLimits, route_native_query},
    segment_cache::AdmissionGate,
    storage::Storage,
};

const PAGE_ROWS: u64 = 256;
const MAX_PAGE_BODY_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum NativeRowState {
    Live = 0,
    Tombstone = 1,
}

impl NativeRowState {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Live),
            1 => Ok(Self::Tombstone),
            _ => Err(invalid("native ANN row state differs")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativePageRef {
    pub(crate) page: u32,
    pub(crate) object: String,
    pub(crate) range: Range<u64>,
    pub(crate) sha256: String,
    pub(crate) rows: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeMutationEntry {
    pub(crate) id: i64,
    pub(crate) sequence: u64,
    pub(crate) state: NativeRowState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeResidentRow {
    pub(crate) id: i64,
    pub(crate) sequence: u64,
    pub(crate) state: NativeRowState,
    pub(crate) code: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct NativeSnapshotInputs {
    pub(crate) generation: u64,
    pub(crate) dimensions: u32,
    pub(crate) router: NativeRouterArtifacts,
    pub(crate) pages: Vec<NativePageRef>,
    pub(crate) mutation_entries: Vec<NativeMutationEntry>,
    pub(crate) delta_rows: Vec<NativeResidentRow>,
    pub(crate) sq8_low: Vec<f32>,
    pub(crate) sq8_step: Vec<f32>,
    pub(crate) route_limits: NativeRouteLimits,
    pub(crate) range_concurrency: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeSearchHit {
    pub(crate) id: i64,
    pub(crate) sequence: u64,
    pub(crate) distance: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeSearchOutcome {
    pub(crate) generation: u64,
    pub(crate) hits: Vec<NativeSearchHit>,
    pub(crate) pages_read: usize,
    pub(crate) physical_gets: u64,
    pub(crate) bytes_read: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeStoredRow {
    id: i64,
    sequence: u64,
    state: NativeRowState,
    code: Vec<u8>,
}

#[derive(Clone)]
pub(crate) struct NativeAnnSnapshot {
    storage: Storage,
    generation: u64,
    dimensions: usize,
    router: NativeRouterArtifacts,
    pages: Vec<NativePageRef>,
    mutations: BTreeMap<i64, NativeMutationEntry>,
    delta_rows: Vec<NativeResidentRow>,
    sq8_low: Vec<f32>,
    sq8_step: Vec<f32>,
    route_limits: NativeRouteLimits,
    range_concurrency: usize,
    range_gate: Arc<AdmissionGate>,
}

pub(crate) struct NativeAnnHandle {
    snapshot: RwLock<Arc<NativeAnnSnapshot>>,
}

#[derive(Clone, Copy, Debug)]
struct SearchCandidate {
    distance: f32,
    id: i64,
    sequence: u64,
}

impl PartialEq for SearchCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance.total_cmp(&other.distance).is_eq()
            && self.id == other.id
            && self.sequence == other.sequence
    }
}

impl Eq for SearchCandidate {}

impl PartialOrd for SearchCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SearchCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.id.cmp(&other.id))
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

#[derive(Debug)]
struct PhysicalPageRead {
    object: String,
    range: Range<u64>,
    slices: Vec<(usize, Range<usize>)>,
}

fn invalid(message: impl Into<String>) -> BorsukError {
    BorsukError::InvalidStorage(message.into())
}

fn native_page_schema(dimensions: i32) -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("state", DataType::UInt8, false),
        Field::new(
            "code",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::UInt8, false)),
                dimensions,
            ),
            false,
        ),
    ])
}

pub(crate) fn decode_native_page(
    bytes: &[u8],
    dimensions: u32,
    expected_rows: u32,
) -> Result<Vec<NativeStoredRow>> {
    let dimensions_usize = usize::try_from(dimensions)
        .map_err(|_| invalid("native ANN page dimensions exceed usize"))?;
    let dimensions_i32 =
        i32::try_from(dimensions).map_err(|_| invalid("native ANN page dimensions exceed i32"))?;
    if dimensions == 0 || expected_rows == 0 || u64::from(expected_rows) > PAGE_ROWS {
        return Err(invalid("native ANN page shape differs"));
    }
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &native_page_schema(dimensions_i32) {
        return Err(invalid("native ANN page physical schema differs"));
    }
    let mut rows = Vec::with_capacity(expected_rows as usize);
    for batch in &mut reader {
        let batch = batch?;
        if batch.num_columns() != 4 || batch.columns().iter().any(|array| array.null_count() != 0) {
            return Err(invalid("native ANN page batch shape differs"));
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("native ANN page id column differs"))?;
        let sequences = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("native ANN page sequence column differs"))?;
        let states = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("native ANN page state column differs"))?;
        let codes = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("native ANN page code column differs"))?;
        if codes.values().null_count() != 0 {
            return Err(invalid("native ANN page code contains nulls"));
        }
        for row in 0..batch.num_rows() {
            let code = codes.value(row);
            let code = code
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| invalid("native ANN page code child differs"))?;
            if code.len() != dimensions_usize || code.null_count() != 0 {
                return Err(invalid("native ANN page code width differs"));
            }
            let sequence = sequences.value(row);
            if sequence == 0 {
                return Err(invalid("native ANN page sequence must be nonzero"));
            }
            rows.push(NativeStoredRow {
                id: ids.value(row),
                sequence,
                state: NativeRowState::from_u8(states.value(row))?,
                code: code.values().to_vec(),
            });
        }
    }
    if rows.len() != expected_rows as usize
        || rows
            .windows(2)
            .any(|pair| (pair[0].id, pair[0].sequence) >= (pair[1].id, pair[1].sequence))
    {
        return Err(invalid("native ANN page row order or count differs"));
    }
    Ok(rows)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

impl NativeAnnSnapshot {
    pub(crate) fn open(storage: Storage, inputs: NativeSnapshotInputs) -> Result<Self> {
        let dimensions = usize::try_from(inputs.dimensions)
            .map_err(|_| invalid("native ANN snapshot dimensions exceed usize"))?;
        if inputs.generation == 0
            || inputs.dimensions == 0
            || inputs.router.dimensions != inputs.dimensions
            || inputs.pages.len() != inputs.router.page_count as usize
            || inputs.sq8_low.len() != dimensions
            || inputs.sq8_step.len() != dimensions
            || inputs.sq8_low.iter().any(|value| !value.is_finite())
            || inputs
                .sq8_step
                .iter()
                .any(|value| !value.is_finite() || *value <= 0.0)
            || !(1..=16).contains(&inputs.range_concurrency)
            || inputs.route_limits.max_body_bytes > MAX_PAGE_BODY_BYTES
        {
            return Err(invalid("native ANN snapshot shape differs"));
        }
        let mut expected_start = BTreeMap::<&str, u64>::new();
        let mut page_rows = 0_u64;
        for (ordinal, page) in inputs.pages.iter().enumerate() {
            let expected_rows = if ordinal + 1 == inputs.pages.len() {
                inputs.router.physical_rows - page_rows
            } else {
                PAGE_ROWS
            };
            if page.page as usize != ordinal
                || page.object.is_empty()
                || page.range.start >= page.range.end
                || !valid_sha256(&page.sha256)
                || u64::from(page.rows) != expected_rows
                || expected_start
                    .get(page.object.as_str())
                    .is_some_and(|end| page.range.start < *end)
            {
                return Err(invalid("native ANN page reference differs"));
            }
            expected_start.insert(page.object.as_str(), page.range.end);
            page_rows = page_rows
                .checked_add(u64::from(page.rows))
                .ok_or_else(|| invalid("native ANN page rows overflow"))?;
        }
        if page_rows != inputs.router.physical_rows {
            return Err(invalid("native ANN page rows differ from router"));
        }

        let mut mutations = BTreeMap::new();
        for entry in &inputs.mutation_entries {
            if entry.sequence == 0 || mutations.insert(entry.id, entry.clone()).is_some() {
                return Err(invalid("native ANN mutation directory is not unique"));
            }
        }
        let mut previous_id = None;
        for row in &inputs.delta_rows {
            if row.sequence == 0
                || row.code.len() != dimensions
                || previous_id.is_some_and(|id| id >= row.id)
                || mutations
                    .get(&row.id)
                    .is_none_or(|entry| entry.sequence != row.sequence || entry.state != row.state)
            {
                return Err(invalid(
                    "native ANN resident delta differs from its directory",
                ));
            }
            previous_id = Some(row.id);
        }
        if mutations.len() != inputs.delta_rows.len() {
            return Err(invalid(
                "native ANN mutation directory contains a missing delta row",
            ));
        }
        Ok(Self {
            storage,
            generation: inputs.generation,
            dimensions,
            router: inputs.router,
            pages: inputs.pages,
            mutations,
            delta_rows: inputs.delta_rows,
            sq8_low: inputs.sq8_low,
            sq8_step: inputs.sq8_step,
            route_limits: inputs.route_limits,
            range_concurrency: inputs.range_concurrency,
            range_gate: Arc::new(AdmissionGate::new(inputs.range_concurrency)),
        })
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    fn score_code(&self, query: &[f32], code: &[u8]) -> Result<f32> {
        if query.len() != self.dimensions || code.len() != self.dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: self.dimensions,
                actual: query.len(),
            });
        }
        let distance = query
            .iter()
            .zip(code)
            .zip(self.sq8_low.iter().zip(&self.sq8_step))
            .fold(0.0_f32, |distance, ((query, code), (low, step))| {
                let value = low + step * f32::from(*code);
                let delta = query - value;
                distance + delta * delta
            });
        if !distance.is_finite() {
            return Err(invalid("native ANN row score is non-finite"));
        }
        Ok(distance)
    }

    fn physical_reads(&self, selected: &[NativePageRef]) -> Result<Vec<PhysicalPageRead>> {
        let mut reads = Vec::<PhysicalPageRead>::new();
        for (page_index, page) in selected.iter().enumerate() {
            let page_len = usize::try_from(page.range.end - page.range.start)
                .map_err(|_| invalid("native ANN page range exceeds usize"))?;
            if let Some(last) = reads.last_mut()
                && last.object == page.object
                && last.range.end == page.range.start
                && last.range.end - last.range.start + page_len as u64 <= MAX_PAGE_BODY_BYTES
            {
                let slice_start = usize::try_from(page.range.start - last.range.start)
                    .map_err(|_| invalid("native ANN coalesced page offset exceeds usize"))?;
                last.range.end = page.range.end;
                last.slices
                    .push((page_index, slice_start..slice_start + page_len));
            } else {
                reads.push(PhysicalPageRead {
                    object: page.object.clone(),
                    range: page.range.clone(),
                    slices: vec![(page_index, 0..page_len)],
                });
            }
        }
        Ok(reads)
    }

    pub(crate) fn search(&self, query: &[f32], k: usize) -> Result<NativeSearchOutcome> {
        if k == 0 || query.len() != self.dimensions || query.iter().any(|v| !v.is_finite()) {
            return Err(invalid("native ANN search query or k differs"));
        }
        let route = route_native_query(&self.router, query, self.route_limits)?;
        let selected = route
            .pages
            .iter()
            .map(|page| self.pages[*page as usize].clone())
            .collect::<Vec<_>>();
        let logical_bytes = selected.iter().try_fold(0_u64, |total, page| {
            total
                .checked_add(page.range.end - page.range.start)
                .ok_or_else(|| invalid("native ANN selected bytes overflow"))
        })?;
        if logical_bytes > self.route_limits.max_body_bytes || logical_bytes > MAX_PAGE_BODY_BYTES {
            return Err(invalid("native ANN selected bytes exceed the read budget"));
        }
        let physical = self.physical_reads(&selected)?;
        let requests = physical
            .iter()
            .map(|read| (read.object.clone(), read.range.clone()))
            .collect::<Vec<_>>();
        let mut bodies = (0..physical.len()).map(|_| None).collect::<Vec<_>>();
        self.storage.for_each_range_wave_completion(
            &requests,
            self.range_concurrency,
            Some(&self.range_gate),
            |index, result| bodies[index] = Some(result),
        );
        let mut page_bodies = (0..selected.len()).map(|_| None).collect::<Vec<_>>();
        for (read, body) in physical.iter().zip(bodies) {
            let body =
                body.ok_or_else(|| invalid("native ANN range wave omitted a completion"))??;
            for (page_index, slice) in &read.slices {
                if slice.end > body.len() {
                    return Err(invalid(
                        "native ANN coalesced page slice escapes its response",
                    ));
                }
                page_bodies[*page_index] = Some(body[slice.clone()].to_vec());
            }
        }

        let mut winners = BTreeMap::<i64, SearchCandidate>::new();
        for (reference, body) in selected.iter().zip(page_bodies) {
            let body = body.ok_or_else(|| invalid("native ANN page body is missing"))?;
            if format!("{:x}", Sha256::digest(&body)) != reference.sha256 {
                return Err(invalid("native ANN page checksum differs"));
            }
            for row in decode_native_page(&body, self.dimensions as u32, reference.rows)? {
                if row.state == NativeRowState::Tombstone {
                    continue;
                }
                if let Some(mutation) = self.mutations.get(&row.id) {
                    if mutation.sequence < row.sequence {
                        return Err(invalid("native ANN mutation directory is stale"));
                    }
                    continue;
                }
                winners.insert(
                    row.id,
                    SearchCandidate {
                        distance: self.score_code(query, &row.code)?,
                        id: row.id,
                        sequence: row.sequence,
                    },
                );
            }
        }
        for row in &self.delta_rows {
            if row.state == NativeRowState::Tombstone {
                continue;
            }
            winners.insert(
                row.id,
                SearchCandidate {
                    distance: self.score_code(query, &row.code)?,
                    id: row.id,
                    sequence: row.sequence,
                },
            );
        }
        let mut heap = BinaryHeap::with_capacity(k);
        for candidate in winners.into_values() {
            if heap.len() < k {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                heap.pop();
                heap.push(candidate);
            }
        }
        let hits = heap
            .into_sorted_vec()
            .into_iter()
            .map(|candidate| NativeSearchHit {
                id: candidate.id,
                sequence: candidate.sequence,
                distance: candidate.distance,
            })
            .collect();
        Ok(NativeSearchOutcome {
            generation: self.generation,
            hits,
            pages_read: selected.len(),
            physical_gets: requests.len() as u64,
            bytes_read: requests
                .iter()
                .map(|(_, range)| range.end - range.start)
                .sum(),
        })
    }
}

impl NativeAnnHandle {
    pub(crate) fn new(snapshot: Arc<NativeAnnSnapshot>) -> Self {
        Self {
            snapshot: RwLock::new(snapshot),
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<NativeAnnSnapshot> {
        Arc::clone(
            &self
                .snapshot
                .read()
                .unwrap_or_else(|error| error.into_inner()),
        )
    }

    pub(crate) fn replace_if_newer(&self, replacement: Arc<NativeAnnSnapshot>) -> Result<bool> {
        let mut current = self
            .snapshot
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if replacement.generation <= current.generation {
            return Err(invalid("native ANN replacement generation is not newer"));
        }
        *current = replacement;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, sync::Arc};

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Int64Array, RecordBatch, UInt8Array, UInt64Array,
    };
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{
        native_ann_format::NativeRouterArtifacts, native_ann_router::NativeRouteLimits,
        storage::Storage,
    };

    #[derive(Clone)]
    struct PageShape {
        id_name: &'static str,
        id_nullable: bool,
        sequence_type: DataType,
        child_nullable: bool,
        reverse_rows: bool,
    }

    impl Default for PageShape {
        fn default() -> Self {
            Self {
                id_name: "id",
                id_nullable: false,
                sequence_type: DataType::UInt64,
                child_nullable: false,
                reverse_rows: false,
            }
        }
    }

    fn encode_page(mut rows: Vec<(i64, u64, u8, Vec<u8>)>, shape: PageShape) -> Vec<u8> {
        if shape.reverse_rows {
            rows.reverse();
        }
        let dimensions = rows.first().unwrap().3.len();
        let child = Arc::new(Field::new("element", DataType::UInt8, shape.child_nullable));
        let codes = Arc::new(
            FixedSizeListArray::try_new(
                Arc::clone(&child),
                i32::try_from(dimensions).unwrap(),
                Arc::new(UInt8Array::from(
                    rows.iter()
                        .flat_map(|(_, _, _, code)| code.iter().copied())
                        .collect::<Vec<_>>(),
                )),
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        let sequence: ArrayRef = match shape.sequence_type {
            DataType::UInt64 => Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|(_, sequence, _, _)| *sequence)
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, sequence, _, _)| *sequence as i64)
                    .collect::<Vec<_>>(),
            )),
            ref other => panic!("unsupported sequence type {other:?}"),
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new(shape.id_name, DataType::Int64, shape.id_nullable),
            Field::new("sequence", sequence.data_type().clone(), false),
            Field::new("state", DataType::UInt8, false),
            Field::new(
                "code",
                DataType::FixedSizeList(child, i32::try_from(dimensions).unwrap()),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
                )),
                sequence,
                Arc::new(UInt8Array::from(
                    rows.iter()
                        .map(|(_, _, state, _)| *state)
                        .collect::<Vec<_>>(),
                )),
                codes,
            ],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        bytes
    }

    fn codebooks(dimensions: usize) -> Box<[f32]> {
        let width = dimensions.div_ceil(16);
        let mut values = vec![0.0_f32; 16 * 256 * width];
        for subspace in 0..16 {
            let active = ((subspace + 1) * dimensions / 16) - subspace * dimensions / 16;
            for codeword in 0..256 {
                let start = (subspace * 256 + codeword) * width;
                for lane in 0..active {
                    values[start + lane] = codeword as f32;
                }
            }
        }
        values.into_boxed_slice()
    }

    fn router() -> NativeRouterArtifacts {
        let dimensions = 16_u32;
        let mut row_codes = vec![255_u8; 258 * 16];
        row_codes[0..16].fill(1);
        row_codes[16..32].fill(2);
        row_codes[256 * 16..257 * 16].fill(3);
        row_codes[257 * 16..258 * 16].fill(4);
        NativeRouterArtifacts {
            dimensions,
            row_codebooks: codebooks(dimensions as usize),
            summary_codebooks: codebooks(dimensions as usize),
            row_codes: row_codes.into_boxed_slice(),
            summary_codes: vec![0_u8; 4 * 16].into_boxed_slice(),
            page_count: 2,
            physical_rows: 258,
        }
    }

    fn page_ref(
        page: u32,
        object: &str,
        range: Range<u64>,
        bytes: &[u8],
        rows: u32,
    ) -> NativePageRef {
        NativePageRef {
            page,
            object: object.to_owned(),
            range,
            sha256: format!("{:x}", Sha256::digest(bytes)),
            rows,
        }
    }

    fn limits() -> NativeRouteLimits {
        NativeRouteLimits {
            max_summary_pages: 2,
            max_candidate_rows: 16,
            max_output_pages: 2,
            bytes_per_page: 4096,
            max_body_bytes: 16 * 1024 * 1024,
        }
    }

    fn fixture() -> (Storage, NativeSnapshotInputs) {
        let storage = Storage::from_uri("memory:///native-ann-read").unwrap();
        let mut first_rows = vec![(1, 1, 0, vec![1; 16]), (2, 1, 0, vec![2; 16])];
        first_rows.extend((0..254).map(|ordinal| (1000 + ordinal, 1, 0, vec![255; 16])));
        let first = encode_page(first_rows, PageShape::default());
        let second = encode_page(
            vec![(3, 1, 0, vec![3; 16]), (5, 1, 0, vec![4; 16])],
            PageShape::default(),
        );
        let mut object = first.clone();
        object.extend_from_slice(&second);
        storage.write_bytes("runs/base.arrow", &object).unwrap();
        let first_len = first.len() as u64;
        let pages = vec![
            page_ref(0, "runs/base.arrow", 0..first_len, &first, 256),
            page_ref(
                1,
                "runs/base.arrow",
                first_len..object.len() as u64,
                &second,
                2,
            ),
        ];
        let inputs = NativeSnapshotInputs {
            generation: 7,
            dimensions: 16,
            router: router(),
            pages,
            mutation_entries: vec![
                NativeMutationEntry {
                    id: 1,
                    sequence: 2,
                    state: NativeRowState::Live,
                },
                NativeMutationEntry {
                    id: 2,
                    sequence: 2,
                    state: NativeRowState::Tombstone,
                },
                NativeMutationEntry {
                    id: 4,
                    sequence: 2,
                    state: NativeRowState::Live,
                },
            ],
            delta_rows: vec![
                NativeResidentRow {
                    id: 1,
                    sequence: 2,
                    state: NativeRowState::Live,
                    code: vec![0; 16],
                },
                NativeResidentRow {
                    id: 2,
                    sequence: 2,
                    state: NativeRowState::Tombstone,
                    code: vec![0; 16],
                },
                NativeResidentRow {
                    id: 4,
                    sequence: 2,
                    state: NativeRowState::Live,
                    code: vec![1; 16],
                },
            ],
            sq8_low: vec![0.0; 16],
            sq8_step: vec![1.0; 16],
            route_limits: limits(),
            range_concurrency: 2,
        };
        (storage, inputs)
    }

    #[test]
    fn native_ann_read_snapshot_coalesces_pages_and_applies_latest_wins() {
        let (storage, inputs) = fixture();
        let before = storage.request_counts();
        let snapshot = NativeAnnSnapshot::open(storage.clone(), inputs).unwrap();

        let outcome = snapshot.search(&[0.0; 16], 4).unwrap();

        assert_eq!(
            outcome.hits,
            vec![
                NativeSearchHit {
                    id: 1,
                    sequence: 2,
                    distance: 0.0,
                },
                NativeSearchHit {
                    id: 4,
                    sequence: 2,
                    distance: 16.0,
                },
                NativeSearchHit {
                    id: 3,
                    sequence: 1,
                    distance: 144.0,
                },
                NativeSearchHit {
                    id: 5,
                    sequence: 1,
                    distance: 256.0,
                },
            ]
        );
        assert_eq!(outcome.generation, 7);
        assert_eq!(outcome.pages_read, 2);
        assert_eq!(outcome.physical_gets, 1);
        assert!(outcome.bytes_read <= 16 * 1024 * 1024);
        assert_eq!(storage.request_counts().delta(&before).gets, 1);
    }

    #[test]
    fn native_ann_read_rejects_page_schema_order_checksum_and_mutation_drift() {
        for shape in [
            PageShape {
                id_name: "row_id",
                ..PageShape::default()
            },
            PageShape {
                id_nullable: true,
                ..PageShape::default()
            },
            PageShape {
                sequence_type: DataType::Int64,
                ..PageShape::default()
            },
            PageShape {
                child_nullable: true,
                ..PageShape::default()
            },
            PageShape {
                reverse_rows: true,
                ..PageShape::default()
            },
        ] {
            let bytes = encode_page(vec![(1, 1, 0, vec![1; 16]), (2, 1, 0, vec![2; 16])], shape);
            assert!(decode_native_page(&bytes, 16, 2).is_err());
        }

        let (storage, mut checksum_drift) = fixture();
        checksum_drift.pages[0].sha256 = "0".repeat(64);
        let snapshot = NativeAnnSnapshot::open(storage, checksum_drift).unwrap();
        assert!(snapshot.search(&[0.0; 16], 4).is_err());

        let (storage, mut missing_entry) = fixture();
        missing_entry.mutation_entries.pop();
        assert!(NativeAnnSnapshot::open(storage, missing_entry).is_err());

        let (storage, mut stale_entry) = fixture();
        stale_entry.mutation_entries[0].sequence = 1;
        assert!(NativeAnnSnapshot::open(storage, stale_entry).is_err());
    }

    #[test]
    fn native_ann_read_refresh_swaps_only_complete_newer_snapshots() {
        let (storage, inputs) = fixture();
        let first = Arc::new(NativeAnnSnapshot::open(storage.clone(), inputs.clone()).unwrap());
        let handle = NativeAnnHandle::new(Arc::clone(&first));

        let mut invalid = inputs.clone();
        invalid.generation = 8;
        invalid.mutation_entries.pop();
        assert!(NativeAnnSnapshot::open(storage.clone(), invalid).is_err());
        assert_eq!(handle.snapshot().generation(), 7);

        let mut newer = inputs;
        newer.generation = 8;
        let replacement = Arc::new(NativeAnnSnapshot::open(storage, newer).unwrap());
        assert!(handle.replace_if_newer(replacement).unwrap());
        assert_eq!(handle.snapshot().generation(), 8);
        assert!(handle.replace_if_newer(first).is_err());
        assert_eq!(handle.snapshot().generation(), 8);
    }
}
