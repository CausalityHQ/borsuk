use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap, HashMap},
    io::Cursor,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float16Array, ListArray, RecordBatch, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow_buffer::OffsetBuffer;
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
    v24_witness::{V24ObjectIdentity, V24SourceRow, validate_v24_identity},
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V24Witness {
    pub(crate) witness_ordinal: u32,
    pub(crate) source_ordinal: u64,
    pub(crate) vector: [f16; 96],
}

#[derive(Debug, Clone)]
struct Candidate {
    key: (u64, u64),
    row: V24SourceRow,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct V24WitnessSampler {
    capacity: usize,
    seed: u64,
    heap: BinaryHeap<Candidate>,
    last_source_ordinal: Option<u64>,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(crate) fn normalize_v24_witness_vector(vector: &[f32; 96]) -> Result<[f32; 96]> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V24 witness source vector is non-finite"));
    }
    let squared_norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>();
    if !squared_norm.is_finite() || squared_norm <= f64::from(f32::MIN_POSITIVE) {
        return Err(invalid("V24 witness source vector norm differs"));
    }
    let inverse = (1.0 / squared_norm.sqrt()) as f32;
    Ok(vector.map(|value| value * inverse))
}

impl V24WitnessSampler {
    pub(crate) fn new(capacity: usize, seed: u64) -> Result<Self> {
        if capacity == 0 || capacity > u32::MAX as usize {
            return Err(invalid("V24 witness sample capacity differs"));
        }
        Ok(Self {
            capacity,
            seed,
            heap: BinaryHeap::with_capacity(capacity),
            last_source_ordinal: None,
        })
    }

    pub(crate) fn consider(&mut self, row: V24SourceRow) -> Result<()> {
        if self
            .last_source_ordinal
            .is_some_and(|previous| row.source_ordinal <= previous)
        {
            return Err(invalid("V24 witness source order differs"));
        }
        self.last_source_ordinal = Some(row.source_ordinal);
        let row = V24SourceRow {
            source_ordinal: row.source_ordinal,
            vector: normalize_v24_witness_vector(&row.vector)?,
        };
        self.insert(Candidate {
            key: (
                splitmix64(row.source_ordinal ^ self.seed),
                row.source_ordinal,
            ),
            row,
        });
        Ok(())
    }

    fn insert(&mut self, candidate: Candidate) {
        if self.heap.len() < self.capacity {
            self.heap.push(candidate);
        } else if self
            .heap
            .peek()
            .is_some_and(|largest| candidate.key < largest.key)
        {
            self.heap.pop();
            self.heap.push(candidate);
        }
    }

    pub(crate) fn merge(&mut self, other: Self) -> Result<()> {
        if self.capacity != other.capacity || self.seed != other.seed {
            return Err(invalid("V24 witness sampler authority differs"));
        }
        for candidate in other.heap {
            self.insert(candidate);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<V24Witness>> {
        if self.heap.len() != self.capacity {
            return Err(invalid("V24 witness sample count differs"));
        }
        let mut candidates = self.heap.into_vec();
        candidates.sort_unstable_by_key(|candidate| candidate.key);
        let mut source_ordinals = BTreeSet::new();
        candidates
            .into_iter()
            .enumerate()
            .map(|(witness_ordinal, candidate)| {
                if !source_ordinals.insert(candidate.row.source_ordinal) {
                    return Err(invalid("V24 witness source ordinal is duplicated"));
                }
                Ok(V24Witness {
                    witness_ordinal: u32::try_from(witness_ordinal)
                        .map_err(|_| invalid("V24 witness ordinal overflows"))?,
                    source_ordinal: candidate.row.source_ordinal,
                    vector: candidate.row.vector.map(f16::from_f32),
                })
            })
            .collect()
    }
}

fn witness_schema() -> Schema {
    Schema::new(vec![
        Field::new("witness_ordinal", DataType::UInt32, false),
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float16, false)),
                96,
            ),
            false,
        ),
    ])
}

fn validate_witnesses(witnesses: &[V24Witness]) -> Result<()> {
    if witnesses.is_empty() {
        return Err(invalid("V24 witness rows are empty"));
    }
    let mut sources = BTreeSet::new();
    for (ordinal, witness) in witnesses.iter().enumerate() {
        let squared_norm = witness
            .vector
            .iter()
            .map(|value| {
                let value = f32::from(*value);
                value * value
            })
            .sum::<f32>();
        if witness.witness_ordinal != u32::try_from(ordinal).unwrap()
            || !sources.insert(witness.source_ordinal)
            || witness
                .vector
                .iter()
                .any(|value| !f32::from(*value).is_finite())
            || !(0.998..=1.002).contains(&squared_norm.sqrt())
        {
            return Err(invalid("V24 witness row authority differs"));
        }
    }
    Ok(())
}

pub(crate) fn write_v24_witnesses(witnesses: &[V24Witness]) -> Result<Vec<u8>> {
    validate_witnesses(witnesses)?;
    let child = Arc::new(Field::new("element", DataType::Float16, false));
    let vectors = FixedSizeListArray::try_new(
        child,
        96,
        Arc::new(Float16Array::from_iter_values(
            witnesses.iter().flat_map(|witness| witness.vector),
        )),
        None,
    )?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            witnesses.iter().map(|witness| witness.witness_ordinal),
        )),
        Arc::new(UInt64Array::from_iter_values(
            witnesses.iter().map(|witness| witness.source_ordinal),
        )),
        Arc::new(vectors),
    ];
    let schema = Arc::new(witness_schema());
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}

pub(crate) fn read_v24_witnesses(
    bytes: &[u8],
    identity: &V24ObjectIdentity,
    expected_rows: usize,
) -> Result<Vec<V24Witness>> {
    validate_v24_identity(identity, identity)?;
    if identity.role != "witnesses-arrow"
        || identity.encoded_bytes != bytes.len() as u64
        || identity.digest != format!("{:x}", Sha256::digest(bytes))
        || expected_rows == 0
    {
        return Err(invalid("V24 witness Arrow byte authority differs"));
    }
    let schema = witness_schema();
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &schema {
        return Err(invalid("V24 witness Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V24 witness Arrow batch is missing"))??;
    if reader.next().is_some()
        || batch.num_rows() != expected_rows
        || batch.num_columns() != 3
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V24 witness Arrow cardinality differs"));
    }
    let ordinals = batch.columns()[0]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V24 witness ordinal column differs"))?;
    let sources = batch.columns()[1]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V24 witness source column differs"))?;
    let vectors = batch.columns()[2]
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V24 witness vector column differs"))?;
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| invalid("V24 witness vector child differs"))?;
    let witnesses = (0..expected_rows)
        .map(|row| V24Witness {
            witness_ordinal: ordinals.value(row),
            source_ordinal: sources.value(row),
            vector: values.values()[row * 96..(row + 1) * 96]
                .try_into()
                .unwrap(),
        })
        .collect::<Vec<_>>();
    validate_witnesses(&witnesses)?;
    Ok(witnesses)
}

const V24_GRAPH_M: usize = 16;
const V24_GRAPH_EF_CONSTRUCTION: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V24WitnessGraph {
    source_ordinals: Vec<u64>,
    vectors: Vec<f16>,
    levels: Vec<u8>,
    node_bases: Vec<u64>,
    adjacency: Vec<u32>,
    entrypoint: u32,
    seed: u64,
    distance_backend: V24DistanceBackend,
}

#[derive(Debug, Clone, Copy)]
struct RankedWitness {
    distance: f32,
    ordinal: u32,
}

impl PartialEq for RankedWitness {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.ordinal == other.ordinal
    }
}

impl Eq for RankedWitness {}

impl PartialOrd for RankedWitness {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedWitness {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.ordinal.cmp(&other.ordinal))
    }
}

fn witness_level(source_ordinal: u64, seed: u64) -> u8 {
    u8::try_from((splitmix64(source_ordinal ^ seed).trailing_zeros() / 4).min(15)).unwrap()
}

/// Exact distance backend recorded by V24 scientific evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V24DistanceBackend {
    Aarch64NeonFma,
    X86AvxFma,
    ScalarControl,
}

impl V24DistanceBackend {
    fn authority_name(self) -> &'static str {
        match self {
            Self::Aarch64NeonFma => "aarch64-neon-fma",
            Self::X86AvxFma => "x86-avx-fma",
            Self::ScalarControl => "scalar-control",
        }
    }

    fn from_authority_name(value: &str) -> Option<Self> {
        match value {
            "aarch64-neon-fma" => Some(Self::Aarch64NeonFma),
            "x86-avx-fma" => Some(Self::X86AvxFma),
            "scalar-control" => Some(Self::ScalarControl),
            _ => None,
        }
    }
}

pub(crate) fn v24_scientific_distance_backend() -> Result<V24DistanceBackend> {
    let zeros = [0.0_f32; 96];
    let (_, backend) = borsuk_fma::fused_dot_8x12(&zeros, &zeros)
        .map_err(|_| invalid("V24 fused distance backend is unavailable"))?;
    Ok(match backend {
        borsuk_fma::FmaBackend::Aarch64NeonFma => V24DistanceBackend::Aarch64NeonFma,
        borsuk_fma::FmaBackend::X86AvxFma => V24DistanceBackend::X86AvxFma,
    })
}

fn scalar_control_distance(query: &[f32; 96], witness: &[f16; 96]) -> f32 {
    let dot = query
        .iter()
        .zip(witness)
        .map(|(left, right)| f64::from(*left) * f64::from(f32::from(*right)))
        .sum::<f64>();
    (1.0_f64 - dot) as f32
}

fn unchecked_distance(query: &[f32; 96], witness: &[f16; 96], backend: V24DistanceBackend) -> f32 {
    match backend {
        V24DistanceBackend::ScalarControl => scalar_control_distance(query, witness),
        V24DistanceBackend::Aarch64NeonFma | V24DistanceBackend::X86AvxFma => {
            let converted = witness.map(f32::from);
            let Ok((dot, observed)) = borsuk_fma::fused_dot_8x12(query, &converted) else {
                return f32::NAN;
            };
            let observed = match observed {
                borsuk_fma::FmaBackend::Aarch64NeonFma => V24DistanceBackend::Aarch64NeonFma,
                borsuk_fma::FmaBackend::X86AvxFma => V24DistanceBackend::X86AvxFma,
            };
            if observed == backend {
                1.0 - dot
            } else {
                f32::NAN
            }
        }
    }
}

pub(crate) fn v24_witness_distance(
    query: &[f32; 96],
    witness: &[f16; 96],
    backend: V24DistanceBackend,
) -> Result<f32> {
    if query.iter().any(|value| !value.is_finite())
        || witness.iter().any(|value| !value.is_finite())
        || backend != V24DistanceBackend::ScalarControl
            && v24_scientific_distance_backend()? != backend
    {
        return Err(invalid("V24 witness distance authority differs"));
    }
    let distance = unchecked_distance(query, witness, backend);
    if !distance.is_finite() {
        return Err(invalid("V24 witness distance is non-finite"));
    }
    Ok(distance)
}

impl V24WitnessGraph {
    pub(crate) fn node_count(&self) -> usize {
        self.source_ordinals.len()
    }

    pub(crate) fn packed_vector_bytes(&self) -> usize {
        self.vectors.len() * std::mem::size_of::<f16>()
    }

    pub(crate) fn source_ordinal_bytes(&self) -> usize {
        self.source_ordinals.len() * std::mem::size_of::<u64>()
    }

    pub(crate) fn distance_backend(&self) -> V24DistanceBackend {
        self.distance_backend
    }

    fn vector(&self, ordinal: u32) -> Option<&[f16; 96]> {
        let ordinal = usize::try_from(ordinal).ok()?;
        self.vectors
            .get(ordinal.checked_mul(96)?..ordinal.checked_add(1)?.checked_mul(96)?)?
            .try_into()
            .ok()
    }

    fn distance_with_backend(
        &self,
        query: &[f32; 96],
        ordinal: u32,
        backend: V24DistanceBackend,
    ) -> f32 {
        unchecked_distance(query, self.vector(ordinal).unwrap(), backend)
    }

    pub(crate) fn source_index(&self) -> Vec<(u64, u32)> {
        let mut index = self
            .source_ordinals
            .iter()
            .copied()
            .enumerate()
            .map(|(ordinal, source)| (source, u32::try_from(ordinal).unwrap()))
            .collect::<Vec<_>>();
        index.sort_unstable();
        index
    }

    pub(crate) fn witness_vector(&self, ordinal: u32) -> Option<&[f16; 96]> {
        self.vector(ordinal)
    }

    pub(crate) fn maximum_degree(&self) -> usize {
        self.adjacency
            .chunks_exact(V24_GRAPH_M)
            .map(|neighbors| {
                neighbors
                    .iter()
                    .take_while(|neighbor| **neighbor != u32::MAX)
                    .count()
            })
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn has_exact_sorted_unique_adjacency(&self) -> bool {
        self.adjacency.chunks_exact(V24_GRAPH_M).all(|neighbors| {
            let mut prior = None;
            let mut ended = false;
            neighbors.iter().copied().all(|neighbor| {
                if neighbor == u32::MAX {
                    ended = true;
                    true
                } else if ended || prior.is_some_and(|prior| neighbor <= prior) {
                    false
                } else {
                    prior = Some(neighbor);
                    true
                }
            })
        })
    }

    fn block_start(&self, node: u32, level: u8) -> Option<usize> {
        let node_index = usize::try_from(node).ok()?;
        if node_index >= self.node_count() || level > self.levels[node_index] {
            return None;
        }
        let base = *self.node_bases.get(node_index)?;
        usize::try_from(base.checked_add(u64::from(level) * V24_GRAPH_M as u64)?).ok()
    }

    fn neighbors(&self, node: u32, level: u8) -> &[u32] {
        let Some(start) = self.block_start(node, level) else {
            return &[];
        };
        let block = &self.adjacency[start..start + V24_GRAPH_M];
        let length = block
            .iter()
            .position(|neighbor| *neighbor == u32::MAX)
            .unwrap_or(V24_GRAPH_M);
        &block[..length]
    }

    fn set_neighbors(&mut self, node: u32, level: u8, neighbors: &[u32]) -> Result<()> {
        let start = self
            .block_start(node, level)
            .ok_or_else(|| invalid("V24 witness graph level differs"))?;
        let mut unique = neighbors.to_vec();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() > V24_GRAPH_M
            || unique.iter().any(|neighbor| {
                usize::try_from(*neighbor).map_or(true, |value| value >= self.node_count())
                    || *neighbor == node
            })
        {
            return Err(invalid("V24 witness graph neighbor differs"));
        }
        self.adjacency[start..start + V24_GRAPH_M].fill(u32::MAX);
        self.adjacency[start..start + unique.len()].copy_from_slice(&unique);
        Ok(())
    }

    fn add_and_prune_neighbor(
        &mut self,
        node: u32,
        level: u8,
        added: u32,
        backend: V24DistanceBackend,
    ) -> Result<()> {
        let mut candidates = self.neighbors(node, level).to_vec();
        candidates.push(added);
        candidates.sort_unstable();
        candidates.dedup();
        let query = self.vector(node).unwrap().map(f32::from);
        let mut ranked = candidates
            .into_iter()
            .map(|ordinal| RankedWitness {
                distance: self.distance_with_backend(&query, ordinal, backend),
                ordinal,
            })
            .collect::<Vec<_>>();
        ranked.sort_unstable();
        let mut selected = if level == 0 {
            let mut backbone = ranked
                .iter()
                .copied()
                .filter(|candidate| candidate.ordinal.abs_diff(node) == 1)
                .collect::<Vec<_>>();
            let remaining = V24_GRAPH_M - backbone.len();
            backbone.extend(
                ranked
                    .into_iter()
                    .filter(|candidate| candidate.ordinal.abs_diff(node) != 1)
                    .take(remaining),
            );
            backbone
        } else {
            ranked.into_iter().take(V24_GRAPH_M).collect()
        };
        selected.truncate(V24_GRAPH_M);
        self.set_neighbors(
            node,
            level,
            &selected
                .into_iter()
                .map(|candidate| candidate.ordinal)
                .collect::<Vec<_>>(),
        )
    }
}

fn greedy_descend(
    graph: &V24WitnessGraph,
    query: &[f32; 96],
    mut current: u32,
    level: u8,
    node_limit: usize,
    backend: V24DistanceBackend,
) -> u32 {
    loop {
        let mut best = RankedWitness {
            distance: graph.distance_with_backend(query, current, backend),
            ordinal: current,
        };
        for neighbor in graph.neighbors(current, level).iter().copied() {
            if usize::try_from(neighbor).unwrap() >= node_limit {
                continue;
            }
            let candidate = RankedWitness {
                distance: graph.distance_with_backend(query, neighbor, backend),
                ordinal: neighbor,
            };
            if candidate < best {
                best = candidate;
            }
        }
        if best.ordinal == current {
            return current;
        }
        current = best.ordinal;
    }
}

#[derive(Clone, Copy)]
struct SearchLayerOptions {
    ef: usize,
    level: u8,
    node_limit: usize,
    backend: V24DistanceBackend,
}

fn search_layer(
    graph: &V24WitnessGraph,
    query: &[f32; 96],
    entrypoints: &[u32],
    options: SearchLayerOptions,
    workspace: Option<&mut EpochWorkspace>,
) -> Vec<RankedWitness> {
    let mut visited =
        workspace.map_or_else(|| VisitedSet::Tree(BTreeSet::new()), EpochWorkspace::begin);
    let mut candidates = BinaryHeap::<std::cmp::Reverse<RankedWitness>>::new();
    let mut best = BinaryHeap::<RankedWitness>::new();
    for entry in entrypoints.iter().copied() {
        if usize::try_from(entry).map_or(true, |value| value >= options.node_limit)
            || !visited.insert(entry)
        {
            continue;
        }
        let ranked = RankedWitness {
            distance: graph.distance_with_backend(query, entry, options.backend),
            ordinal: entry,
        };
        candidates.push(std::cmp::Reverse(ranked));
        best.push(ranked);
    }
    while let Some(std::cmp::Reverse(candidate)) = candidates.pop() {
        if best.len() == options.ef && best.peek().is_some_and(|worst| candidate > *worst) {
            break;
        }
        for neighbor in graph
            .neighbors(candidate.ordinal, options.level)
            .iter()
            .copied()
        {
            if usize::try_from(neighbor).unwrap() >= options.node_limit || !visited.insert(neighbor)
            {
                continue;
            }
            let ranked = RankedWitness {
                distance: graph.distance_with_backend(query, neighbor, options.backend),
                ordinal: neighbor,
            };
            if best.len() < options.ef || best.peek().is_some_and(|worst| ranked < *worst) {
                candidates.push(std::cmp::Reverse(ranked));
                best.push(ranked);
                if best.len() > options.ef {
                    best.pop();
                }
            }
        }
    }
    let mut ranked = best.into_vec();
    ranked.sort_unstable();
    ranked
}

struct EpochWorkspace {
    marks: Vec<u32>,
    epoch: u32,
}

impl EpochWorkspace {
    fn begin(&mut self) -> VisitedSet<'_> {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.marks.fill(0);
            self.epoch = 1;
        }
        VisitedSet::Epoch {
            marks: &mut self.marks,
            epoch: self.epoch,
        }
    }
}

enum VisitedSet<'a> {
    Tree(BTreeSet<u32>),
    Epoch { marks: &'a mut [u32], epoch: u32 },
}

impl VisitedSet<'_> {
    fn insert(&mut self, ordinal: u32) -> bool {
        match self {
            Self::Tree(values) => values.insert(ordinal),
            Self::Epoch { marks, epoch } => {
                let mark = &mut marks[usize::try_from(ordinal).unwrap()];
                if *mark == *epoch {
                    false
                } else {
                    *mark = *epoch;
                    true
                }
            }
        }
    }
}

fn validate_graph(graph: &V24WitnessGraph) -> Result<()> {
    let rows = graph.node_count();
    if rows < 2
        || graph.distance_backend == V24DistanceBackend::ScalarControl
        || graph.vectors.len() != rows.saturating_mul(96)
        || graph
            .source_ordinals
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != rows
        || graph.levels.len() != rows
        || graph.node_bases.len() != rows + 1
        || usize::try_from(graph.entrypoint).map_or(true, |entry| entry >= rows)
        || graph.levels[usize::try_from(graph.entrypoint).unwrap()]
            != graph.levels.iter().copied().max().unwrap()
        || graph.node_bases.first() != Some(&0)
    {
        return Err(invalid("V24 witness graph authority differs"));
    }
    for ordinal in 0..rows {
        let vector = graph.vector(u32::try_from(ordinal).unwrap()).unwrap();
        let squared_norm = vector
            .iter()
            .map(|value| {
                let value = f32::from(*value);
                value * value
            })
            .sum::<f32>();
        if vector.iter().any(|value| !f32::from(*value).is_finite())
            || !(0.998..=1.002).contains(&squared_norm.sqrt())
        {
            return Err(invalid("V24 witness graph vector authority differs"));
        }
    }
    let mut expected = 0_u64;
    for (node, level) in graph.levels.iter().copied().enumerate() {
        if graph.node_bases[node] != expected {
            return Err(invalid("V24 witness graph offsets differ"));
        }
        expected = expected
            .checked_add((u64::from(level) + 1) * V24_GRAPH_M as u64)
            .ok_or_else(|| invalid("V24 witness graph offsets overflow"))?;
    }
    if graph.node_bases[rows] != expected
        || usize::try_from(expected).ok() != Some(graph.adjacency.len())
        || !graph.has_exact_sorted_unique_adjacency()
    {
        return Err(invalid("V24 witness graph adjacency shape differs"));
    }
    for node in 0..rows {
        for level in 0..=graph.levels[node] {
            if graph
                .neighbors(u32::try_from(node).unwrap(), level)
                .iter()
                .any(|neighbor| {
                    usize::try_from(*neighbor).map_or(true, |value| value >= rows)
                        || usize::try_from(*neighbor).ok() == Some(node)
                        || graph.levels[usize::try_from(*neighbor).unwrap()] < level
                })
            {
                return Err(invalid("V24 witness graph neighbor authority differs"));
            }
        }
    }
    let mut reachable = vec![false; rows];
    let mut pending = vec![graph.entrypoint];
    reachable[usize::try_from(graph.entrypoint).unwrap()] = true;
    while let Some(node) = pending.pop() {
        for neighbor in graph.neighbors(node, 0).iter().copied() {
            let index = usize::try_from(neighbor).unwrap();
            if !reachable[index] {
                reachable[index] = true;
                pending.push(neighbor);
            }
        }
    }
    if reachable.iter().any(|value| !value) {
        return Err(invalid("V24 witness graph is disconnected"));
    }
    Ok(())
}

pub(crate) fn build_v24_witness_graph(
    witnesses: &[V24Witness],
    seed: u64,
) -> Result<V24WitnessGraph> {
    build_v24_witness_graph_with_progress(witnesses, seed, |_| Ok(()))
}

pub(crate) fn build_v24_witness_graph_with_progress(
    witnesses: &[V24Witness],
    seed: u64,
    mut progress: impl FnMut(u64) -> Result<()>,
) -> Result<V24WitnessGraph> {
    let backend = v24_scientific_distance_backend()?;
    validate_witnesses(witnesses)?;
    if witnesses.len() < 2 {
        return Err(invalid("V24 witness graph row count differs"));
    }
    let levels = witnesses
        .iter()
        .map(|witness| witness_level(witness.source_ordinal, seed))
        .collect::<Vec<_>>();
    let mut node_bases = Vec::with_capacity(witnesses.len() + 1);
    let mut adjacency_len = 0_u64;
    for level in &levels {
        node_bases.push(adjacency_len);
        adjacency_len = adjacency_len
            .checked_add((u64::from(*level) + 1) * V24_GRAPH_M as u64)
            .ok_or_else(|| invalid("V24 witness graph allocation overflows"))?;
    }
    node_bases.push(adjacency_len);
    let mut graph = V24WitnessGraph {
        source_ordinals: witnesses
            .iter()
            .map(|witness| witness.source_ordinal)
            .collect(),
        vectors: witnesses
            .iter()
            .flat_map(|witness| witness.vector)
            .collect(),
        levels,
        node_bases,
        adjacency: vec![
            u32::MAX;
            usize::try_from(adjacency_len).map_err(|_| {
                invalid("V24 witness graph allocation exceeds address space")
            })?
        ],
        entrypoint: 0,
        seed,
        distance_backend: backend,
    };
    let mut maximum_level = graph.levels[0];
    for node_index in 1..graph.node_count() {
        let node = u32::try_from(node_index).unwrap();
        let node_level = graph.levels[node_index];
        let query = graph.vector(node).unwrap().map(f32::from);
        let mut current = graph.entrypoint;
        if maximum_level > node_level {
            for level in ((node_level + 1)..=maximum_level).rev() {
                current = greedy_descend(&graph, &query, current, level, node_index, backend);
            }
        }
        for level in (0..=node_level.min(maximum_level)).rev() {
            let found = search_layer(
                &graph,
                &query,
                &[current],
                SearchLayerOptions {
                    ef: V24_GRAPH_EF_CONSTRUCTION.min(node_index),
                    level,
                    node_limit: node_index,
                    backend,
                },
                None,
            );
            let mut selected = found
                .iter()
                .take(V24_GRAPH_M)
                .map(|ranked| ranked.ordinal)
                .collect::<Vec<_>>();
            if level == 0 {
                let predecessor = node - 1;
                selected.retain(|neighbor| *neighbor != predecessor);
                selected.truncate(V24_GRAPH_M - 1);
                selected.push(predecessor);
            }
            if let Some(first) = selected.first() {
                current = *first;
            }
            graph.set_neighbors(node, level, &selected)?;
            for neighbor in selected {
                graph.add_and_prune_neighbor(neighbor, level, node, backend)?;
            }
        }
        if node_level > maximum_level {
            graph.entrypoint = node;
            maximum_level = node_level;
        }
        let completed_nodes = u64::try_from(node_index + 1).unwrap();
        if completed_nodes.is_multiple_of(16_384) || node_index + 1 == graph.node_count() {
            progress(completed_nodes)?;
        }
    }
    validate_graph(&graph)?;
    Ok(graph)
}

#[cfg(test)]
pub(crate) fn search_v24_witness_graph(
    graph: &V24WitnessGraph,
    query: &[f32; 96],
    k: usize,
    ef: usize,
) -> Result<Vec<u32>> {
    V24WitnessSearch::new(graph)?.search(query, k, ef)
}

pub(crate) struct V24WitnessSearch<'a> {
    graph: &'a V24WitnessGraph,
    backend: V24DistanceBackend,
    workspace: RefCell<EpochWorkspace>,
}

impl<'a> V24WitnessSearch<'a> {
    pub(crate) fn new(graph: &'a V24WitnessGraph) -> Result<Self> {
        let backend = v24_scientific_distance_backend()?;
        validate_graph(graph)?;
        if graph.distance_backend != backend {
            return Err(invalid("V24 witness graph distance backend differs"));
        }
        Ok(Self {
            graph,
            backend,
            workspace: RefCell::new(EpochWorkspace {
                marks: vec![0; graph.node_count()],
                epoch: 0,
            }),
        })
    }

    pub(crate) fn workspace_bytes(&self) -> usize {
        self.workspace.borrow().marks.len() * std::mem::size_of::<u32>()
    }

    pub(crate) fn search(&self, query: &[f32; 96], k: usize, ef: usize) -> Result<Vec<u32>> {
        self.search_with_backend(query, k, ef, self.backend)
    }

    pub(crate) fn search_scalar_control(
        &self,
        query: &[f32; 96],
        k: usize,
        ef: usize,
    ) -> Result<Vec<u32>> {
        self.search_with_backend(query, k, ef, V24DistanceBackend::ScalarControl)
    }

    fn search_with_backend(
        &self,
        query: &[f32; 96],
        k: usize,
        ef: usize,
        backend: V24DistanceBackend,
    ) -> Result<Vec<u32>> {
        let graph = self.graph;
        if query.iter().any(|value| !value.is_finite())
            || k == 0
            || k > graph.node_count()
            || ef < k
        {
            return Err(invalid("V24 witness graph query differs"));
        }
        if ef >= graph.node_count() {
            let mut ranked = (0..graph.node_count())
                .map(|ordinal| {
                    let ordinal = u32::try_from(ordinal).unwrap();
                    RankedWitness {
                        distance: graph.distance_with_backend(query, ordinal, backend),
                        ordinal,
                    }
                })
                .collect::<Vec<_>>();
            ranked.sort_unstable();
            return Ok(ranked
                .into_iter()
                .take(k)
                .map(|value| value.ordinal)
                .collect());
        }
        let maximum_level = graph.levels[usize::try_from(graph.entrypoint).unwrap()];
        let mut current = graph.entrypoint;
        for level in (1..=maximum_level).rev() {
            current = greedy_descend(graph, query, current, level, graph.node_count(), backend);
        }
        let mut workspace = self.workspace.borrow_mut();
        Ok(search_layer(
            graph,
            query,
            &[current],
            SearchLayerOptions {
                ef,
                level: 0,
                node_limit: graph.node_count(),
                backend,
            },
            Some(&mut workspace),
        )
        .into_iter()
        .take(k)
        .map(|value| value.ordinal)
        .collect())
    }
}

fn graph_schema(backend: V24DistanceBackend) -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("witness_ordinal", DataType::UInt32, false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("level", DataType::UInt8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float16, false)),
                    96,
                ),
                false,
            ),
            Field::new(
                "adjacency",
                DataType::List(Arc::new(Field::new("neighbor", DataType::UInt32, false))),
                false,
            ),
            Field::new("entrypoint", DataType::UInt32, false),
            Field::new("seed", DataType::UInt64, false),
        ],
        HashMap::from([(
            "distance_backend".to_owned(),
            backend.authority_name().to_owned(),
        )]),
    )
}

pub(crate) fn write_v24_witness_graph(graph: &V24WitnessGraph) -> Result<Vec<u8>> {
    validate_graph(graph)?;
    let child = Arc::new(Field::new("element", DataType::Float16, false));
    let vectors = FixedSizeListArray::try_new(
        child,
        96,
        Arc::new(Float16Array::from(graph.vectors.clone())),
        None,
    )?;
    let adjacency = ListArray::try_new(
        Arc::new(Field::new("neighbor", DataType::UInt32, false)),
        OffsetBuffer::from_lengths((0..graph.node_count()).map(|node| {
            usize::try_from(graph.node_bases[node + 1] - graph.node_bases[node]).unwrap()
        })),
        Arc::new(UInt32Array::from(graph.adjacency.clone())),
        None,
    )?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            (0..graph.node_count()).map(|ordinal| u32::try_from(ordinal).unwrap()),
        )),
        Arc::new(UInt64Array::from(graph.source_ordinals.clone())),
        Arc::new(UInt8Array::from(graph.levels.clone())),
        Arc::new(vectors),
        Arc::new(adjacency),
        Arc::new(UInt32Array::from_iter_values(std::iter::repeat_n(
            graph.entrypoint,
            graph.node_count(),
        ))),
        Arc::new(UInt64Array::from_iter_values(std::iter::repeat_n(
            graph.seed,
            graph.node_count(),
        ))),
    ];
    let schema = Arc::new(graph_schema(graph.distance_backend));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}

pub(crate) fn read_v24_witness_graph(
    bytes: &[u8],
    identity: &V24ObjectIdentity,
    expected_rows: usize,
) -> Result<V24WitnessGraph> {
    validate_v24_identity(identity, identity)?;
    if identity.role != "witness-graph"
        || identity.encoded_bytes != bytes.len() as u64
        || identity.digest != format!("{:x}", Sha256::digest(bytes))
        || expected_rows < 2
    {
        return Err(invalid("V24 witness graph byte authority differs"));
    }
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    let distance_backend = reader
        .schema()
        .metadata()
        .get("distance_backend")
        .and_then(|value| V24DistanceBackend::from_authority_name(value))
        .filter(|backend| *backend != V24DistanceBackend::ScalarControl)
        .ok_or_else(|| invalid("V24 witness graph distance backend differs"))?;
    let schema = graph_schema(distance_backend);
    if reader.schema().as_ref() != &schema {
        return Err(invalid("V24 witness graph Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V24 witness graph batch is missing"))??;
    if reader.next().is_some()
        || batch.num_rows() != expected_rows
        || batch.num_columns() != 7
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V24 witness graph cardinality differs"));
    }
    let ordinals = batch.columns()[0]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V24 witness graph ordinal differs"))?;
    let sources = batch.columns()[1]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V24 witness graph source differs"))?;
    let levels = batch.columns()[2]
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| invalid("V24 witness graph level differs"))?;
    let vectors = batch.columns()[3]
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V24 witness graph vector differs"))?;
    let vector_values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| invalid("V24 witness graph vector child differs"))?;
    let lists = batch.columns()[4]
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| invalid("V24 witness graph adjacency differs"))?;
    let entrypoints = batch.columns()[5]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V24 witness graph entrypoint differs"))?;
    let seeds = batch.columns()[6]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V24 witness graph seed differs"))?;
    let entrypoint = entrypoints.value(0);
    let seed = seeds.value(0);
    if (0..expected_rows)
        .any(|row| entrypoints.value(row) != entrypoint || seeds.value(row) != seed)
    {
        return Err(invalid("V24 witness graph repeated authority differs"));
    }
    let mut source_ordinals = Vec::with_capacity(expected_rows);
    let mut packed_vectors = Vec::with_capacity(expected_rows * 96);
    let mut graph_levels = Vec::with_capacity(expected_rows);
    let mut node_bases = Vec::with_capacity(expected_rows + 1);
    let mut adjacency = Vec::new();
    for row in 0..expected_rows {
        if ordinals.value(row) != u32::try_from(row).unwrap() {
            return Err(invalid("V24 witness graph ordinal differs"));
        }
        source_ordinals.push(sources.value(row));
        packed_vectors.extend_from_slice(&vector_values.values()[row * 96..(row + 1) * 96]);
        graph_levels.push(levels.value(row));
        node_bases.push(u64::try_from(adjacency.len()).unwrap());
        let list = lists.value(row);
        let values = list
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V24 witness graph adjacency child differs"))?;
        adjacency.extend_from_slice(values.values());
    }
    node_bases.push(u64::try_from(adjacency.len()).unwrap());
    let graph = V24WitnessGraph {
        source_ordinals,
        vectors: packed_vectors,
        levels: graph_levels,
        node_bases,
        adjacency,
        entrypoint,
        seed,
        distance_backend,
    };
    validate_graph(&graph)?;
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float16Array, RecordBatch, UInt32Array, UInt64Array,
    };
    use arrow_ipc::{
        MetadataVersion,
        writer::{FileWriter, IpcWriteOptions},
    };
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use sha2::{Digest, Sha256};

    use super::{
        V24DistanceBackend, V24Witness, V24WitnessSampler, V24WitnessSearch,
        build_v24_witness_graph, read_v24_witness_graph, read_v24_witnesses,
        search_v24_witness_graph, v24_scientific_distance_backend, v24_witness_distance,
        write_v24_witness_graph, write_v24_witnesses,
    };
    use crate::v24_witness::{V24ObjectIdentity, V24SourceRow};

    const SEED: u64 = 0x1234_5678_9abc_def0;
    const EXPECTED: [u64; 17] = [
        165, 213, 181, 75, 144, 51, 29, 248, 251, 201, 87, 82, 125, 107, 233, 239, 35,
    ];

    fn row(source_ordinal: u64) -> V24SourceRow {
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        vector[1] = source_ordinal as f32 / 512.0;
        V24SourceRow {
            source_ordinal,
            vector,
        }
    }

    fn identity(bytes: &[u8]) -> V24ObjectIdentity {
        V24ObjectIdentity {
            role: "witnesses-arrow".to_owned(),
            uri: "s3://borsuk-v24/witnesses.arrow".to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: format!("{:x}", Sha256::digest(bytes)),
            encoded_bytes: bytes.len() as u64,
            generation: "generation-witnesses".to_owned(),
        }
    }

    fn sample_ranges(ranges: &[std::ops::Range<u64>]) -> Vec<super::V24Witness> {
        let mut samplers = ranges
            .iter()
            .map(|range| {
                let mut sampler = V24WitnessSampler::new(17, SEED).unwrap();
                for source_ordinal in range.clone() {
                    sampler.consider(row(source_ordinal)).unwrap();
                }
                sampler
            })
            .collect::<Vec<_>>();
        let mut merged = samplers.remove(0);
        for sampler in samplers.into_iter().rev() {
            merged.merge(sampler).unwrap();
        }
        merged.finish().unwrap()
    }

    #[test]
    fn v24_witness_sample_is_order_partition_and_thread_invariant() {
        let single_range = std::iter::once(0..257).collect::<Vec<_>>();
        let single = sample_ranges(&single_range);
        let partitioned = sample_ranges(&[0..61, 61..129, 129..200, 200..257]);
        assert_eq!(single, partitioned);
        assert_eq!(
            single
                .iter()
                .map(|witness| witness.source_ordinal)
                .collect::<Vec<_>>(),
            EXPECTED
        );
        assert_eq!(
            single
                .iter()
                .map(|witness| witness.witness_ordinal)
                .collect::<Vec<_>>(),
            (0_u32..17).collect::<Vec<_>>()
        );
        assert!(single.iter().all(|witness| {
            let norm = witness
                .vector
                .iter()
                .map(|value| f32::from(*value).powi(2))
                .sum::<f32>()
                .sqrt();
            norm.is_finite() && (norm - 1.0).abs() < 0.001
        }));
    }

    fn wrong_child_name_bytes(witnesses: &[super::V24Witness]) -> Vec<u8> {
        let child = Arc::new(Field::new("item", DataType::Float16, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("witness_ordinal", DataType::UInt32, false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::clone(&child), 96),
                false,
            ),
        ]));
        let vectors = FixedSizeListArray::try_new(
            child,
            96,
            Arc::new(Float16Array::from_iter_values(
                witnesses.iter().flat_map(|witness| witness.vector),
            )),
            None,
        )
        .unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from_iter_values(
                witnesses.iter().map(|witness| witness.witness_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                witnesses.iter().map(|witness| witness.source_ordinal),
            )),
            Arc::new(vectors),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut bytes = Vec::new();
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        bytes
    }

    #[test]
    fn v24_witness_sample_arrow_rejects_schema_identity_and_vector_drift() {
        let single_range = std::iter::once(0..257).collect::<Vec<_>>();
        let witnesses = sample_ranges(&single_range);
        let bytes = write_v24_witnesses(&witnesses).unwrap();
        let registered = identity(&bytes);
        assert_eq!(
            read_v24_witnesses(&bytes, &registered, 17).unwrap(),
            witnesses
        );

        let mut changed = registered.clone();
        changed.digest = "00".repeat(32);
        assert!(read_v24_witnesses(&bytes, &changed, 17).is_err());
        assert!(read_v24_witnesses(&bytes, &registered, 16).is_err());

        let malformed = wrong_child_name_bytes(&witnesses);
        let malformed_identity = identity(&malformed);
        assert!(read_v24_witnesses(&malformed, &malformed_identity, 17).is_err());

        let mut nonmonotone = witnesses.clone();
        nonmonotone.swap(0, 1);
        assert!(write_v24_witnesses(&nonmonotone).is_err());
        let mut zero = witnesses;
        zero[0].vector = [f16::ZERO; 96];
        assert!(write_v24_witnesses(&zero).is_err());
    }

    fn graph_witnesses(count: u32) -> Vec<V24Witness> {
        (0..count)
            .map(|witness_ordinal| {
                let mut vector = [0.0_f32; 96];
                let primary = usize::try_from(witness_ordinal % 64).unwrap();
                let secondary = usize::try_from((witness_ordinal * 17 + 11) % 96).unwrap();
                vector[primary] = 1.0;
                vector[secondary] += 0.25;
                let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
                V24Witness {
                    witness_ordinal,
                    source_ordinal: u64::from(witness_ordinal) * 3 + 7,
                    vector: vector.map(|value| f16::from_f32(value / norm)),
                }
            })
            .collect()
    }

    fn graph_identity(bytes: &[u8]) -> V24ObjectIdentity {
        V24ObjectIdentity {
            role: "witness-graph".to_owned(),
            uri: "s3://borsuk-v24/witness-graph.arrow".to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: format!("{:x}", Sha256::digest(bytes)),
            encoded_bytes: bytes.len() as u64,
            generation: "generation-witness-graph".to_owned(),
        }
    }

    #[test]
    fn v24_witness_graph_is_byte_deterministic_and_bounded() {
        let witnesses = graph_witnesses(96);
        let first = build_v24_witness_graph(&witnesses, SEED).unwrap();
        let second = build_v24_witness_graph(&witnesses, SEED).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.node_count(), 96);
        assert_eq!(first.packed_vector_bytes(), 96 * 96 * 2);
        assert_eq!(first.source_ordinal_bytes(), 96 * 8);
        assert_eq!(
            first.distance_backend(),
            v24_scientific_distance_backend().unwrap()
        );
        assert!(first.maximum_degree() <= 16);
        assert!(first.has_exact_sorted_unique_adjacency());

        let first_bytes = write_v24_witness_graph(&first).unwrap();
        let second_bytes = write_v24_witness_graph(&second).unwrap();
        assert_eq!(first_bytes, second_bytes);
        let registered = graph_identity(&first_bytes);
        assert_eq!(
            read_v24_witness_graph(&first_bytes, &registered, 96).unwrap(),
            first
        );

        let mut changed = registered;
        changed.digest = "00".repeat(32);
        assert!(read_v24_witness_graph(&first_bytes, &changed, 96).is_err());
        let mut invalid = first;
        invalid.adjacency.fill(u32::MAX);
        assert!(write_v24_witness_graph(&invalid).is_err());
        let mut invalid_backend = second;
        invalid_backend.distance_backend = V24DistanceBackend::ScalarControl;
        assert!(write_v24_witness_graph(&invalid_backend).is_err());
    }

    fn scalar_topk(witnesses: &[V24Witness], query: &[f32; 96], k: usize) -> Vec<u32> {
        let mut ranked = witnesses
            .iter()
            .map(|witness| {
                let distance = 1.0_f64
                    - witness
                        .vector
                        .iter()
                        .zip(query)
                        .map(|(left, right)| f64::from(f32::from(*left)) * f64::from(*right))
                        .sum::<f64>();
                (distance, witness.witness_ordinal)
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        ranked.into_iter().take(k).map(|ranked| ranked.1).collect()
    }

    #[test]
    fn v24_witness_graph_search_matches_scalar_control_on_reduced_fixture() {
        let witnesses = graph_witnesses(96);
        let graph = build_v24_witness_graph(&witnesses, SEED).unwrap();
        for query_ordinal in [0_usize, 1, 17, 63, 95] {
            let query = witnesses[query_ordinal].vector.map(f32::from);
            assert_eq!(
                search_v24_witness_graph(&graph, &query, 8, 96).unwrap(),
                scalar_topk(&witnesses, &query, 8)
            );
            let traversed = search_v24_witness_graph(&graph, &query, 8, 32).unwrap();
            assert_eq!(
                traversed,
                search_v24_witness_graph(&graph, &query, 8, 32).unwrap()
            );
            assert_eq!(traversed.len(), 8);
            assert_eq!(traversed.iter().copied().collect::<BTreeSet<_>>().len(), 8);
            assert_eq!(traversed[0], u32::try_from(query_ordinal).unwrap());
        }
        let mut nonfinite = witnesses[0].vector.map(f32::from);
        nonfinite[3] = f32::NAN;
        assert!(search_v24_witness_graph(&graph, &nonfinite, 8, 96).is_err());
        assert!(
            search_v24_witness_graph(&graph, &witnesses[0].vector.map(f32::from), 0, 96).is_err()
        );
    }

    #[test]
    fn v24_witness_graph_explicit_simd_backend_matches_independent_scalar_control() {
        let backend = v24_scientific_distance_backend().unwrap();
        assert!(matches!(
            backend,
            V24DistanceBackend::Aarch64NeonFma | V24DistanceBackend::X86AvxFma
        ));
        let witnesses = graph_witnesses(96);
        let graph = build_v24_witness_graph(&witnesses, SEED).unwrap();
        let search = V24WitnessSearch::new(&graph).unwrap();
        assert_eq!(search.workspace_bytes(), 96 * std::mem::size_of::<u32>());
        for query_ordinal in [0_usize, 1, 17, 63, 95] {
            let query = witnesses[query_ordinal].vector.map(f32::from);
            for witness in &witnesses {
                let fused = v24_witness_distance(&query, &witness.vector, backend).unwrap();
                let scalar = v24_witness_distance(
                    &query,
                    &witness.vector,
                    V24DistanceBackend::ScalarControl,
                )
                .unwrap();
                assert!((fused - scalar).abs() <= 2.0e-6);
            }
            assert_eq!(
                search.search(&query, 8, 32).unwrap(),
                search.search_scalar_control(&query, 8, 32).unwrap()
            );
        }

        let mut nonfinite = witnesses[0].vector.map(f32::from);
        nonfinite[4] = f32::INFINITY;
        assert!(v24_witness_distance(&nonfinite, &witnesses[0].vector, backend).is_err());
    }
}
