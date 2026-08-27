use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    BorsukError, Result, VectorElementType,
    global_cell_card::{
        CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION, CellCardExactBlockRef, CellCardGroupRef,
        CellCardGroupWriter, CellCardPush, EncodedCellCardGroup, RankedCellCardExactBlock,
        encode_cell_card_group, plan_cell_card_exact_wave_with_amplification,
    },
    global_leaf::{GlobalLeafCodeInput, GlobalLeafPageInput, GlobalLeafRowInput},
    mutation::{MutationStamp, MutationVersion},
    record::RecordId,
};

const V22_EXACT_PREFIX_ROWS: [u16; 6] = [10, 256, 512, 1024, 1536, 2048];
pub(crate) const V22_MAX_EXACT_PREFIX_ROWS: usize = 2048;
pub(crate) const V22_STAGE_L_MAX_CELL_ROWS: u64 = 512_000;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V22ExactPrefixRow {
    pub(crate) distance: f32,
    pub(crate) record_id: u64,
    pub(crate) canonical_record_id: Box<[u8]>,
    pub(crate) primary_cell: u32,
}

#[derive(Debug, Clone)]
struct V22ExactCandidate(V22ExactPrefixRow);

impl PartialEq for V22ExactCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for V22ExactCandidate {}

impl PartialOrd for V22ExactCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V22ExactCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then_with(|| self.0.canonical_record_id.cmp(&other.0.canonical_record_id))
            .then_with(|| self.0.record_id.cmp(&other.0.record_id))
    }
}

#[derive(Debug)]
pub(crate) struct V22ExactPrefixAccumulator {
    heaps: Vec<BinaryHeap<V22ExactCandidate>>,
}

impl V22ExactPrefixAccumulator {
    pub(crate) fn new(query_count: usize) -> Result<Self> {
        if query_count == 0 {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 exact-prefix query authority is empty".to_string(),
            ));
        }
        Ok(Self {
            heaps: (0..query_count)
                .map(|_| BinaryHeap::with_capacity(V22_MAX_EXACT_PREFIX_ROWS))
                .collect(),
        })
    }

    pub(crate) fn observe(
        &mut self,
        record_id: u64,
        canonical_record_id: &[u8],
        primary_cell: u32,
        distances: &[f32],
    ) -> Result<()> {
        if canonical_record_id.is_empty()
            || distances.len() != self.heaps.len()
            || distances.iter().any(|distance| !distance.is_finite())
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 exact-prefix row authority is empty or invalid".to_string(),
            ));
        }
        for (heap, distance) in self.heaps.iter_mut().zip(distances) {
            let retain = heap.len() < V22_MAX_EXACT_PREFIX_ROWS
                || heap.peek().is_some_and(|worst| {
                    distance
                        .total_cmp(&worst.0.distance)
                        .then_with(|| canonical_record_id.cmp(worst.0.canonical_record_id.as_ref()))
                        .then_with(|| record_id.cmp(&worst.0.record_id))
                        .is_lt()
                });
            if retain {
                if heap.len() == V22_MAX_EXACT_PREFIX_ROWS {
                    heap.pop();
                }
                heap.push(V22ExactCandidate(V22ExactPrefixRow {
                    distance: *distance,
                    record_id,
                    canonical_record_id: canonical_record_id.into(),
                    primary_cell,
                }));
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<Vec<V22ExactPrefixRow>>> {
        self.heaps
            .into_iter()
            .map(|heap| {
                let mut rows = heap
                    .into_vec()
                    .into_iter()
                    .map(|candidate| candidate.0)
                    .collect::<Vec<_>>();
                rows.sort_by(|left, right| {
                    left.distance
                        .total_cmp(&right.distance)
                        .then_with(|| left.canonical_record_id.cmp(&right.canonical_record_id))
                        .then_with(|| left.record_id.cmp(&right.record_id))
                });
                if rows.is_empty() {
                    return Err(BorsukError::InvalidStorage(
                        "V22 exact-prefix corpus authority is empty".to_string(),
                    ));
                }
                Ok(rows)
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
/// One exact-ranked corpus row and its primary-cell routing authority.
pub struct V22StageLExactRow {
    /// Exact metric distance to the frozen query.
    pub distance: f32,
    /// Deterministic diagnostic row ordinal.
    pub record_id: u64,
    /// Authenticated raw record-ID bytes.
    pub canonical_record_id: Box<[u8]>,
    /// Primary V20 routing cell containing the row.
    pub primary_cell: u32,
    /// One-based rank of the primary cell in the complete routing order.
    pub primary_cell_routing_rank: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
/// Bounded exact corpus prefix for one frozen query.
pub struct V22StageLQueryPrefix {
    /// Zero-based query position in caller authority.
    pub query_index: usize,
    /// Exact rows ordered by metric distance and authenticated ID.
    pub rows: Vec<V22StageLExactRow>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
/// Claim-ineligible V22 Stage-L generation and exact-prefix evidence.
pub struct V22StageLReport {
    /// Authenticated V20 cell-card root checksum.
    pub v20_root_checksum: String,
    /// Authenticated V20 codebook descriptor checksum.
    pub v20_codebook_checksum: String,
    /// Rows represented by the exact pinned V20 generation.
    pub rows: u64,
    /// Complete authenticated routing-cell count.
    pub routing_cell_count: usize,
    /// Query-major bounded exact-prefix evidence.
    pub query_prefixes: Vec<V22StageLQueryPrefix>,
    /// Canonical seven-layout by six-prefix census evidence.
    pub layout_censuses: Vec<V22StageLLayoutArmReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Frozen Stage-L physical-layout authority.
pub enum V22LayoutKind {
    /// Existing V20 immutable cell-card physical order.
    V20Physical,
    /// V20 rows repacked within each cell by a deterministic two-pivot projection.
    V20TwoPivotRepacked,
    /// Semantic microclusters ordered independently inside each authenticated cell.
    SemanticWithinCell,
    /// Semantic microclusters with both within-cell and cross-cell ordering.
    SemanticCrossCell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V22LayoutCensusArm {
    pub(crate) layout: V22LayoutKind,
    pub(crate) microcluster_rows: Option<u8>,
    pub(crate) exact_prefix_rows: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
/// One physical range selected by the production exact-wave planner.
pub struct V22StageLRange {
    /// Content-addressed object path.
    pub path: String,
    /// Inclusive byte offset.
    pub start: u64,
    /// Exclusive byte offset.
    pub end: u64,
    /// Encoded bytes belonging to selected blocks inside this range.
    pub selected_bytes: u64,
    /// Complete decoded rows carried by the selected blocks.
    pub rows: u64,
    /// Selected exact blocks coalesced into this range.
    pub blocks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
/// Exact Stage-L result for one query in one layout/prefix arm.
pub struct V22StageLLayoutQuerySample {
    /// Zero-based query index.
    pub query_index: usize,
    /// Frozen exact prefix size.
    pub exact_prefix_rows: u16,
    /// Smallest complete routing prefix covering all ten GT cells.
    pub required_routing_cells: usize,
    /// Ground-truth rows covered by the required routing prefix.
    pub gt_cell_hits: usize,
    /// Ground-truth cell coverage in parts per million.
    pub gt_cell_coverage_ppm: u64,
    /// Corpus rows in that complete routing prefix.
    pub routed_rows: u64,
    /// Useful exact-vector bytes in the requested prefix.
    pub useful_bytes: u64,
    /// Encoded bytes in selected exact blocks.
    pub selected_bytes: u64,
    /// Actual bytes in coalesced physical reads.
    pub physical_bytes: u64,
    /// Physical bytes not belonging to selected blocks.
    pub speculative_bytes: u64,
    /// Number of physical object-store requests.
    pub requests: usize,
    /// Complete rows carried by selected blocks.
    pub selected_rows: u64,
    /// Useful-to-physical packing purity in parts per million.
    pub packing_purity_ppm: u64,
    /// Physical-to-selected amplification in parts per million.
    pub physical_amplification_ppm: u64,
    /// Exact physical planner classification independent of routing eligibility.
    pub physical_limiting_bound: V22LayoutLimitingBound,
    /// Whether the routing prefix satisfies coverage and row-count authority.
    pub routing_eligible: bool,
    /// First bound preventing eligibility, or eligible.
    pub limiting_bound: V22LayoutLimitingBound,
    /// True only when routing and physical gates both pass.
    pub eligible: bool,
    /// Exact production-planner physical ranges.
    pub ranges: Vec<V22StageLRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
/// Canonical Stage-L arm and all query-major samples.
pub struct V22StageLLayoutArmReport {
    /// Frozen layout family.
    pub layout: V22LayoutKind,
    /// Unit-row authority for repacked layouts.
    pub microcluster_rows: Option<u8>,
    /// Exact ranked prefix size.
    pub exact_prefix_rows: u16,
    /// Content-addressed object authority reachable by candidate units.
    pub projected_objects: Vec<V22StageLProjectedObject>,
    /// Query-major samples.
    pub query_samples: Vec<V22StageLLayoutQuerySample>,
    /// True only when every query passes every Stage-L gate.
    pub eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
/// One content-addressed object emitted or referenced by a Stage-L layout.
pub struct V22StageLProjectedObject {
    /// Object path.
    pub path: String,
    /// BLAKE3 checksum encoded as lowercase hexadecimal.
    pub checksum: String,
    /// Complete encoded object length.
    pub encoded_bytes: u64,
}

impl V22LayoutCensusArm {
    pub(crate) fn validate(self) -> Result<()> {
        let layout_is_valid = matches!(
            (self.layout, self.microcluster_rows),
            (V22LayoutKind::V20Physical, None)
                | (
                    V22LayoutKind::V20TwoPivotRepacked
                        | V22LayoutKind::SemanticWithinCell
                        | V22LayoutKind::SemanticCrossCell,
                    Some(32 | 64)
                )
        );
        if !layout_is_valid || !V22_EXACT_PREFIX_ROWS.contains(&self.exact_prefix_rows) {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 layout census arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn v22_layout_census_arms() -> Result<Vec<V22LayoutCensusArm>> {
    let mut arms = Vec::with_capacity(V22_EXACT_PREFIX_ROWS.len() * 7);
    for (layout, microcluster_rows) in [
        (V22LayoutKind::V20Physical, None),
        (V22LayoutKind::V20TwoPivotRepacked, Some(32)),
        (V22LayoutKind::V20TwoPivotRepacked, Some(64)),
        (V22LayoutKind::SemanticWithinCell, Some(32)),
        (V22LayoutKind::SemanticWithinCell, Some(64)),
        (V22LayoutKind::SemanticCrossCell, Some(32)),
        (V22LayoutKind::SemanticCrossCell, Some(64)),
    ] {
        for exact_prefix_rows in V22_EXACT_PREFIX_ROWS {
            let arm = V22LayoutCensusArm {
                layout,
                microcluster_rows,
                exact_prefix_rows,
            };
            arm.validate()?;
            arms.push(arm);
        }
    }
    Ok(arms)
}

pub(crate) fn routing_rank(ordered_cells: &[u32], primary_cell: u32) -> Result<usize> {
    if ordered_cells.is_empty() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 ordered routing authority is empty".to_string(),
        ));
    }
    let unique = ordered_cells.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != ordered_cells.len() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 ordered routing authority contains duplicate cells".to_string(),
        ));
    }
    ordered_cells
        .iter()
        .position(|cell| *cell == primary_cell)
        .map(|rank| rank + 1)
        .ok_or_else(|| {
            BorsukError::InvalidSearchOptions(
                "V22 primary cell is absent from ordered routing authority".to_string(),
            )
        })
}

pub(crate) fn routing_coverage_at_probe(
    ranks: &[usize],
    probes: usize,
    routing_cell_count: usize,
) -> Result<usize> {
    if ranks.is_empty()
        || routing_cell_count == 0
        || probes == 0
        || probes > routing_cell_count
        || ranks
            .iter()
            .any(|rank| *rank == 0 || *rank > routing_cell_count)
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 routing-rank evidence is empty or invalid".to_string(),
        ));
    }
    Ok(ranks.iter().filter(|rank| **rank <= probes).count())
}

fn stage_l_io(path: &Path, source: std::io::Error) -> BorsukError {
    BorsukError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct V22StageLSpillRow {
    pub(crate) source_ordinal: u64,
    pub(crate) canonical_record_id: Box<[u8]>,
    pub(crate) stamp: MutationStamp,
    pub(crate) code: Box<[u8]>,
    pub(crate) exact: Box<[u8]>,
}

impl V22StageLSpillRow {
    pub(crate) fn geometry(
        &self,
        dimensions: usize,
        element_type: VectorElementType,
        normalize: bool,
    ) -> Result<Box<[f32]>> {
        let decoded = element_type.decode_fixed_width(&self.exact, dimensions)?;
        Ok(if normalize {
            crate::metric::unit_l2_normalized(&decoded).into()
        } else {
            decoded.into()
        })
    }
}

#[derive(Debug, Clone)]
struct V22StageLSpillExtent {
    primary_cell: u32,
    start_ordinal: u64,
    rows: u64,
    offset: u64,
    bytes: u64,
    checksum: [u8; 32],
}

struct V22OpenSpillExtent {
    primary_cell: u32,
    start_ordinal: u64,
    rows: u64,
    offset: u64,
    bytes: u64,
    hasher: blake3::Hasher,
}

pub(crate) struct V22StageLSpillWriter {
    directory: tempfile::TempDir,
    path: PathBuf,
    writer: BufWriter<File>,
    dimensions: usize,
    element_type: VectorElementType,
    exact_width: usize,
    code_width: usize,
    header_bytes: u64,
    header_checksum: [u8; 32],
    authenticated_cells: BTreeSet<u32>,
    completed_cells: BTreeSet<u32>,
    extents: Vec<V22StageLSpillExtent>,
    open_extent: Option<V22OpenSpillExtent>,
    total_rows: u64,
    max_cell_rows: u64,
}

impl V22StageLSpillWriter {
    pub(crate) fn create(
        parent: &Path,
        root_checksum: &str,
        dimensions: usize,
        element_type: VectorElementType,
        code_width: usize,
        authenticated_cells: &BTreeSet<u32>,
        max_cell_rows: u64,
    ) -> Result<Self> {
        if root_checksum.is_empty()
            || dimensions == 0
            || code_width == 0
            || authenticated_cells.is_empty()
            || max_cell_rows == 0
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 Stage L scratch authority is empty".to_string(),
            ));
        }
        let directory = tempfile::Builder::new()
            .prefix("borsuk-v22-stage-l-")
            .tempdir_in(parent)
            .map_err(|source| stage_l_io(parent, source))?;
        let path = directory.path().join("rows.bin");
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|source| stage_l_io(&path, source))?;
        let mut header = b"BORSUK-V22-STAGE-L-SCRATCH\0".to_vec();
        header.extend_from_slice(&(root_checksum.len() as u64).to_le_bytes());
        header.extend_from_slice(root_checksum.as_bytes());
        let header_bytes = header.len() as u64;
        let header_checksum = *blake3::hash(&header).as_bytes();
        let mut writer = BufWriter::new(file);
        writer
            .write_all(&header)
            .map_err(|source| stage_l_io(&path, source))?;
        Ok(Self {
            directory,
            path,
            writer,
            dimensions,
            element_type,
            exact_width: element_type.fixed_width_bytes(dimensions)?,
            code_width,
            header_bytes,
            header_checksum,
            authenticated_cells: authenticated_cells.clone(),
            completed_cells: BTreeSet::new(),
            extents: Vec::with_capacity(authenticated_cells.len()),
            open_extent: None,
            total_rows: 0,
            max_cell_rows,
        })
    }

    fn finish_open_extent(&mut self) -> Result<()> {
        let Some(open) = self.open_extent.take() else {
            return Ok(());
        };
        if open.rows == 0 || open.rows > self.max_cell_rows {
            return Err(BorsukError::InvalidStorage(
                "V22 Stage L scratch cell is empty or exceeds its row bound".to_string(),
            ));
        }
        self.completed_cells.insert(open.primary_cell);
        self.extents.push(V22StageLSpillExtent {
            primary_cell: open.primary_cell,
            start_ordinal: open.start_ordinal,
            rows: open.rows,
            offset: open.offset,
            bytes: open.bytes,
            checksum: *open.hasher.finalize().as_bytes(),
        });
        Ok(())
    }

    pub(crate) fn append_batch(
        &mut self,
        primary_cell: u32,
        rows: &[V22StageLSpillRow],
    ) -> Result<()> {
        if rows.is_empty() || !self.authenticated_cells.contains(&primary_cell) {
            return Err(BorsukError::InvalidStorage(
                "V22 Stage L scratch batch is empty or references an unauthenticated cell"
                    .to_string(),
            ));
        }
        if self
            .open_extent
            .as_ref()
            .is_some_and(|open| open.primary_cell != primary_cell)
        {
            self.finish_open_extent()?;
        }
        if self.open_extent.is_none() {
            if self.completed_cells.contains(&primary_cell) {
                return Err(BorsukError::InvalidStorage(
                    "V22 Stage L scratch cells are not contiguous".to_string(),
                ));
            }
            let offset = self
                .writer
                .stream_position()
                .map_err(|source| stage_l_io(&self.path, source))?;
            self.open_extent = Some(V22OpenSpillExtent {
                primary_cell,
                start_ordinal: self.total_rows,
                rows: 0,
                offset,
                bytes: 0,
                hasher: blake3::Hasher::new(),
            });
        }
        let open = self.open_extent.as_ref().expect("scratch extent is open");
        let prospective_rows = open.rows.checked_add(rows.len() as u64).ok_or_else(|| {
            BorsukError::InvalidStorage("V22 Stage L scratch row count overflows".to_string())
        })?;
        if prospective_rows > self.max_cell_rows
            || rows.iter().enumerate().any(|(offset, row)| {
                row.source_ordinal != self.total_rows + offset as u64
                    || row.canonical_record_id.is_empty()
                    || row.canonical_record_id.len() > u16::MAX as usize
                    || row.code.len() != self.code_width
                    || row.exact.len() != self.exact_width
            })
        {
            return Err(BorsukError::InvalidStorage(
                "V22 Stage L scratch row authority is invalid or exceeds its bound".to_string(),
            ));
        }
        let mut encoded = Vec::new();
        for row in rows {
            encoded.extend_from_slice(&(row.canonical_record_id.len() as u16).to_le_bytes());
            encoded.extend_from_slice(&row.canonical_record_id);
            encoded.extend_from_slice(&row.stamp.version().to_bytes());
            encoded.extend_from_slice(&row.stamp.digest());
            encoded.extend_from_slice(&row.code);
            encoded.extend_from_slice(&row.exact);
        }
        self.writer
            .write_all(&encoded)
            .map_err(|source| stage_l_io(&self.path, source))?;
        let open = self.open_extent.as_mut().expect("scratch extent is open");
        open.hasher.update(&encoded);
        open.rows = prospective_rows;
        open.bytes = open
            .bytes
            .checked_add(encoded.len() as u64)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V22 Stage L scratch bytes overflow".to_string())
            })?;
        self.total_rows = self
            .total_rows
            .checked_add(rows.len() as u64)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V22 Stage L total row count overflows".to_string())
            })?;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<V22StageLSpill> {
        self.finish_open_extent()?;
        if self.completed_cells != self.authenticated_cells {
            return Err(BorsukError::InvalidStorage(
                "V22 Stage L scratch does not cover every authenticated cell".to_string(),
            ));
        }
        self.writer
            .flush()
            .map_err(|source| stage_l_io(&self.path, source))?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|source| stage_l_io(&self.path, source))?;
        Ok(V22StageLSpill {
            _directory: self.directory,
            path: self.path,
            dimensions: self.dimensions,
            element_type: self.element_type,
            exact_width: self.exact_width,
            code_width: self.code_width,
            header_bytes: self.header_bytes,
            header_checksum: self.header_checksum,
            extents: self.extents,
            total_rows: self.total_rows,
        })
    }
}

pub(crate) struct V22StageLSpill {
    _directory: tempfile::TempDir,
    path: PathBuf,
    dimensions: usize,
    element_type: VectorElementType,
    exact_width: usize,
    code_width: usize,
    header_bytes: u64,
    header_checksum: [u8; 32],
    extents: Vec<V22StageLSpillExtent>,
    total_rows: u64,
}

impl V22StageLSpill {
    pub(crate) fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub(crate) fn cell_rows(&self) -> Vec<(u32, u64)> {
        self.extents
            .iter()
            .map(|extent| (extent.primary_cell, extent.rows))
            .collect()
    }

    pub(crate) fn read_cell(&self, primary_cell: u32) -> Result<Vec<V22StageLSpillRow>> {
        let extent = self
            .extents
            .iter()
            .find(|extent| extent.primary_cell == primary_cell)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V22 Stage L scratch omits an authenticated cell".to_string(),
                )
            })?;
        let mut reader = BufReader::new(
            File::open(&self.path).map_err(|source| stage_l_io(&self.path, source))?,
        );
        let header_bytes = usize::try_from(self.header_bytes).map_err(|_| {
            BorsukError::InvalidStorage("V22 Stage L scratch header is oversized".to_string())
        })?;
        let mut header = vec![0_u8; header_bytes];
        reader
            .read_exact(&mut header)
            .map_err(|source| stage_l_io(&self.path, source))?;
        if *blake3::hash(&header).as_bytes() != self.header_checksum {
            return Err(BorsukError::InvalidStorage(
                "V22 Stage L scratch header checksum differs".to_string(),
            ));
        }
        reader
            .seek(SeekFrom::Start(extent.offset))
            .map_err(|source| stage_l_io(&self.path, source))?;
        let mut encoded = vec![0_u8; extent.bytes as usize];
        reader
            .read_exact(&mut encoded)
            .map_err(|source| stage_l_io(&self.path, source))?;
        if *blake3::hash(&encoded).as_bytes() != extent.checksum {
            return Err(BorsukError::InvalidStorage(
                "V22 Stage L scratch cell checksum differs".to_string(),
            ));
        }
        let mut cursor = 0_usize;
        let mut rows = Vec::with_capacity(extent.rows as usize);
        for row_index in 0..extent.rows {
            let id_len_end = cursor.checked_add(2).ok_or_else(|| {
                BorsukError::InvalidStorage("V22 Stage L scratch ID range overflows".to_string())
            })?;
            let id_len = u16::from_le_bytes(
                encoded
                    .get(cursor..id_len_end)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V22 Stage L scratch ID length is truncated".to_string(),
                        )
                    })?
                    .try_into()
                    .expect("two-byte ID length"),
            ) as usize;
            cursor = id_len_end;
            let id_end = cursor.checked_add(id_len).ok_or_else(|| {
                BorsukError::InvalidStorage("V22 Stage L scratch ID overflows".to_string())
            })?;
            let canonical_record_id = encoded
                .get(cursor..id_end)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V22 Stage L scratch ID is truncated".to_string())
                })?
                .to_vec()
                .into_boxed_slice();
            cursor = id_end;
            let version_end = cursor.checked_add(24).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V22 Stage L scratch mutation version range overflows".to_string(),
                )
            })?;
            let version = MutationVersion::from_bytes(
                encoded.get(cursor..version_end).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V22 Stage L scratch mutation version is truncated".to_string(),
                    )
                })?,
            )?;
            cursor = version_end;
            let digest_end = cursor.checked_add(32).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V22 Stage L scratch mutation digest range overflows".to_string(),
                )
            })?;
            let digest: [u8; 32] = encoded
                .get(cursor..digest_end)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V22 Stage L scratch mutation digest is truncated".to_string(),
                    )
                })?
                .try_into()
                .expect("fixed mutation digest");
            cursor = digest_end;
            let code_end = cursor.checked_add(self.code_width).ok_or_else(|| {
                BorsukError::InvalidStorage("V22 Stage L scratch code range overflows".to_string())
            })?;
            let code = encoded
                .get(cursor..code_end)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V22 Stage L scratch code is truncated".to_string())
                })?
                .to_vec()
                .into_boxed_slice();
            cursor = code_end;
            let exact_end = cursor.checked_add(self.exact_width).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V22 Stage L scratch exact row range overflows".to_string(),
                )
            })?;
            let exact = encoded
                .get(cursor..exact_end)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V22 Stage L scratch exact row is truncated".to_string(),
                    )
                })?
                .to_vec()
                .into_boxed_slice();
            cursor = exact_end;
            rows.push(V22StageLSpillRow {
                source_ordinal: extent.start_ordinal + row_index,
                canonical_record_id,
                stamp: MutationStamp::new(version, digest),
                code,
                exact,
            });
        }
        if cursor != encoded.len() || rows.len() != extent.rows as usize {
            return Err(BorsukError::InvalidStorage(
                "V22 Stage L scratch cell has trailing or missing rows".to_string(),
            ));
        }
        Ok(rows)
    }

    pub(crate) fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub(crate) fn element_type(&self) -> VectorElementType {
        self.element_type
    }
}

#[derive(Debug, Clone)]
pub(crate) struct V22SemanticRow {
    pub(crate) record_id: u64,
    pub(crate) canonical_record_id: Box<[u8]>,
    pub(crate) primary_cell: u32,
    /// Authenticated metric-prepared geometry (including normalization when
    /// required), matching the production V20 locality builder's input.
    pub(crate) geometry: Box<[f32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22SemanticUnit {
    pub(crate) primary_cell: u32,
    pub(crate) record_ids: Box<[u64]>,
}

#[derive(Debug)]
pub(crate) struct V22SemanticCell {
    pub(crate) primary_cell: u32,
    pub(crate) centroid: Box<[f64]>,
    pub(crate) units: Vec<V22SemanticUnit>,
}

fn semantic_squared_distance(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(*right);
            delta * delta
        })
        .sum()
}

fn semantic_centroid(rows: &[V22SemanticRow], indexes: &[usize]) -> Box<[f64]> {
    let mut centroid = vec![0.0_f64; rows[indexes[0]].geometry.len()];
    for index in indexes {
        for (sum, value) in centroid.iter_mut().zip(rows[*index].geometry.iter()) {
            *sum += f64::from(*value);
        }
    }
    for value in &mut centroid {
        *value /= indexes.len() as f64;
    }
    centroid.into()
}

fn semantic_centroid_distance(left: &[f64], right: &[f64]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

fn semantic_farthest(rows: &[V22SemanticRow], indexes: &[usize], from: usize) -> usize {
    let mut farthest = indexes[0];
    let mut farthest_distance = semantic_squared_distance(
        rows[farthest].geometry.as_ref(),
        rows[from].geometry.as_ref(),
    );
    for &index in &indexes[1..] {
        let distance =
            semantic_squared_distance(rows[index].geometry.as_ref(), rows[from].geometry.as_ref());
        if distance.total_cmp(&farthest_distance).is_gt()
            || (distance.total_cmp(&farthest_distance).is_eq()
                && rows[index].canonical_record_id < rows[farthest].canonical_record_id)
        {
            farthest = index;
            farthest_distance = distance;
        }
    }
    farthest
}

fn two_pivot_farthest(rows: &[V22SemanticRow], indexes: &[usize], from: usize) -> usize {
    let mut farthest = indexes[0];
    let mut farthest_distance = crate::metric::squared_euclidean_simd(
        rows[farthest].geometry.as_ref(),
        rows[from].geometry.as_ref(),
    );
    for &index in &indexes[1..] {
        let distance = crate::metric::squared_euclidean_simd(
            rows[index].geometry.as_ref(),
            rows[from].geometry.as_ref(),
        );
        if distance.total_cmp(&farthest_distance).is_gt()
            || (distance.total_cmp(&farthest_distance).is_eq()
                && rows[index].canonical_record_id < rows[farthest].canonical_record_id)
        {
            farthest = index;
            farthest_distance = distance;
        }
    }
    farthest
}

fn split_semantic_rows(
    rows: &[V22SemanticRow],
    indexes: &mut [usize],
    leaf_count: usize,
    leaves: &mut Vec<Vec<usize>>,
) {
    if leaf_count == 1 {
        leaves.push(indexes.to_vec());
        return;
    }
    let anchor = *indexes
        .iter()
        .min_by(|left, right| {
            rows[**left]
                .canonical_record_id
                .cmp(&rows[**right].canonical_record_id)
        })
        .expect("nonempty semantic split");
    let first_pivot = semantic_farthest(rows, indexes, anchor);
    let second_pivot = semantic_farthest(rows, indexes, first_pivot);
    let mut scored = indexes
        .iter()
        .map(|index| {
            let score = semantic_squared_distance(
                rows[*index].geometry.as_ref(),
                rows[first_pivot].geometry.as_ref(),
            ) - semantic_squared_distance(
                rows[*index].geometry.as_ref(),
                rows[second_pivot].geometry.as_ref(),
            );
            (*index, score)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|(left, left_score), (right, right_score)| {
        left_score.total_cmp(right_score).then_with(|| {
            rows[*left]
                .canonical_record_id
                .cmp(&rows[*right].canonical_record_id)
        })
    });
    for (target, (index, _)) in indexes.iter_mut().zip(scored) {
        *target = index;
    }
    let left_leaf_count = leaf_count / 2;
    let right_leaf_count = leaf_count - left_leaf_count;
    let middle = indexes.len() * left_leaf_count / leaf_count;
    let (left, right) = indexes.split_at_mut(middle);
    split_semantic_rows(rows, left, left_leaf_count, leaves);
    split_semantic_rows(rows, right, right_leaf_count, leaves);
}

fn nearest_neighbor_order<K: Ord>(centroids: &[Box<[f64]>], keys: &[K]) -> Vec<usize> {
    let mut remaining = (0..centroids.len()).collect::<BTreeSet<_>>();
    let first = *remaining
        .iter()
        .min_by(|left, right| keys[**left].cmp(&keys[**right]))
        .expect("nonempty nearest-neighbor authority");
    remaining.remove(&first);
    let mut order = vec![first];
    while !remaining.is_empty() {
        let prior = *order.last().expect("nearest-neighbor order is nonempty");
        let next = remaining
            .iter()
            .map(|index| {
                (
                    *index,
                    semantic_centroid_distance(&centroids[prior], &centroids[*index]),
                )
            })
            .min_by(|(left, left_distance), (right, right_distance)| {
                left_distance
                    .total_cmp(right_distance)
                    .then_with(|| keys[*left].cmp(&keys[*right]))
            })
            .map(|(index, _)| index)
            .expect("nearest-neighbor remainder is nonempty");
        remaining.remove(&next);
        order.push(next);
    }
    order
}

pub(crate) fn project_v22_semantic_cell(
    rows: &[V22SemanticRow],
    primary_cell: u32,
    microcluster_rows: u8,
) -> Result<V22SemanticCell> {
    if rows.is_empty()
        || !matches!(microcluster_rows, 32 | 64)
        || rows.iter().any(|row| row.primary_cell != primary_cell)
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 semantic cell authority is empty or mismatched".to_string(),
        ));
    }
    let mut indexes = (0..rows.len()).collect::<Vec<_>>();
    indexes.sort_unstable_by(|left, right| {
        rows[*left]
            .canonical_record_id
            .cmp(&rows[*right].canonical_record_id)
            .then_with(|| rows[*left].record_id.cmp(&rows[*right].record_id))
    });
    let cell_centroid = semantic_centroid(rows, &indexes);
    let mut leaves = Vec::new();
    let leaf_count = indexes.len().div_ceil(usize::from(microcluster_rows));
    split_semantic_rows(rows, &mut indexes, leaf_count, &mut leaves);
    let leaf_centroids = leaves
        .iter()
        .map(|leaf| semantic_centroid(rows, leaf))
        .collect::<Vec<_>>();
    let leaf_keys = leaves
        .iter()
        .map(|leaf| {
            leaf.iter()
                .map(|index| rows[*index].canonical_record_id.as_ref())
                .min()
                .expect("semantic leaf is nonempty")
                .to_vec()
        })
        .collect::<Vec<_>>();
    let units = nearest_neighbor_order(&leaf_centroids, &leaf_keys)
        .into_iter()
        .map(|leaf_index| V22SemanticUnit {
            primary_cell,
            record_ids: leaves[leaf_index]
                .iter()
                .map(|index| rows[*index].record_id)
                .collect(),
        })
        .collect();
    Ok(V22SemanticCell {
        primary_cell,
        centroid: cell_centroid,
        units,
    })
}

pub(crate) fn v22_semantic_cell_centroid(
    rows: &[V22SemanticRow],
    primary_cell: u32,
) -> Result<Box<[f64]>> {
    if rows.is_empty() || rows.iter().any(|row| row.primary_cell != primary_cell) {
        return Err(BorsukError::InvalidStorage(
            "V22 semantic centroid cell authority is empty or mismatched".to_string(),
        ));
    }
    let mut indexes = (0..rows.len()).collect::<Vec<_>>();
    indexes.sort_unstable_by(|left, right| {
        rows[*left]
            .canonical_record_id
            .cmp(&rows[*right].canonical_record_id)
            .then_with(|| rows[*left].record_id.cmp(&rows[*right].record_id))
    });
    Ok(semantic_centroid(rows, &indexes))
}

pub(crate) fn project_v22_semantic_layout(
    rows: &[V22SemanticRow],
    authenticated_cell_order: &[u32],
    microcluster_rows: u8,
    reorder_cells: bool,
) -> Result<Vec<V22SemanticUnit>> {
    if rows.is_empty()
        || !matches!(microcluster_rows, 32 | 64)
        || authenticated_cell_order.is_empty()
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 semantic layout authority is empty or invalid".to_string(),
        ));
    }
    let dimensions = rows[0].geometry.len();
    let unique_ids = rows
        .iter()
        .map(|row| row.record_id)
        .collect::<BTreeSet<_>>();
    let unique_canonical_ids = rows
        .iter()
        .map(|row| row.canonical_record_id.as_ref())
        .collect::<BTreeSet<_>>();
    if dimensions == 0
        || unique_ids.len() != rows.len()
        || unique_canonical_ids.len() != rows.len()
        || rows.iter().any(|row| row.canonical_record_id.is_empty())
        || rows.iter().any(|row| {
            row.geometry.len() != dimensions || row.geometry.iter().any(|value| !value.is_finite())
        })
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 semantic rows are duplicate, nonfinite, or dimensionally inconsistent".to_string(),
        ));
    }

    let mut rows_by_cell = BTreeMap::<u32, Vec<usize>>::new();
    for (index, row) in rows.iter().enumerate() {
        rows_by_cell
            .entry(row.primary_cell)
            .or_default()
            .push(index);
    }
    let ordered_cells = authenticated_cell_order
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered_cells.len() != authenticated_cell_order.len()
        || ordered_cells != rows_by_cell.keys().copied().collect()
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 authenticated cell order does not cover semantic rows exactly".to_string(),
        ));
    }

    let mut cells = BTreeMap::<u32, V22SemanticCell>::new();
    for (&primary_cell, row_indexes) in &rows_by_cell {
        let cell_rows = row_indexes
            .iter()
            .map(|index| rows[*index].clone())
            .collect::<Vec<_>>();
        cells.insert(
            primary_cell,
            project_v22_semantic_cell(&cell_rows, primary_cell, microcluster_rows)?,
        );
    }

    let cell_order = if reorder_cells {
        let sorted_cells = cells.values().collect::<Vec<_>>();
        let centroids = sorted_cells
            .iter()
            .map(|cell| cell.centroid.clone())
            .collect::<Vec<_>>();
        let keys = sorted_cells
            .iter()
            .map(|cell| u64::from(cell.primary_cell))
            .collect::<Vec<_>>();
        nearest_neighbor_order(&centroids, &keys)
            .into_iter()
            .map(|index| sorted_cells[index].primary_cell)
            .collect::<Vec<_>>()
    } else {
        authenticated_cell_order.to_vec()
    };
    let mut projected = Vec::new();
    for primary_cell in cell_order {
        projected.extend(
            cells
                .remove(&primary_cell)
                .expect("validated semantic cell remains present")
                .units,
        );
    }
    Ok(projected)
}

pub(crate) fn project_v22_two_pivot_layout(
    rows: &[V22SemanticRow],
    authenticated_cell_order: &[u32],
    unit_rows: u8,
) -> Result<Vec<V22SemanticUnit>> {
    if rows.is_empty() || !matches!(unit_rows, 32 | 64) || authenticated_cell_order.is_empty() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 two-pivot layout authority is empty or invalid".to_string(),
        ));
    }
    let dimensions = rows[0].geometry.len();
    let unique_ids = rows
        .iter()
        .map(|row| row.record_id)
        .collect::<BTreeSet<_>>();
    let unique_canonical_ids = rows
        .iter()
        .map(|row| row.canonical_record_id.as_ref())
        .collect::<BTreeSet<_>>();
    if dimensions == 0
        || unique_ids.len() != rows.len()
        || unique_canonical_ids.len() != rows.len()
        || rows.iter().any(|row| row.canonical_record_id.is_empty())
        || rows.iter().any(|row| {
            row.geometry.len() != dimensions || row.geometry.iter().any(|value| !value.is_finite())
        })
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 two-pivot rows are duplicate, nonfinite, or dimensionally inconsistent"
                .to_string(),
        ));
    }
    let mut rows_by_cell = BTreeMap::<u32, Vec<usize>>::new();
    for (index, row) in rows.iter().enumerate() {
        rows_by_cell
            .entry(row.primary_cell)
            .or_default()
            .push(index);
    }
    let ordered_cells = authenticated_cell_order
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered_cells.len() != authenticated_cell_order.len()
        || ordered_cells != rows_by_cell.keys().copied().collect()
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 authenticated cell order does not cover two-pivot rows exactly".to_string(),
        ));
    }

    let mut projected = Vec::new();
    for primary_cell in authenticated_cell_order {
        let cell_rows = rows_by_cell
            .remove(primary_cell)
            .expect("validated two-pivot cell remains present")
            .into_iter()
            .map(|index| rows[index].clone())
            .collect::<Vec<_>>();
        projected.extend(project_v22_two_pivot_cell(
            &cell_rows,
            *primary_cell,
            unit_rows,
        )?);
    }
    Ok(projected)
}

pub(crate) fn project_v22_two_pivot_cell(
    rows: &[V22SemanticRow],
    primary_cell: u32,
    unit_rows: u8,
) -> Result<Vec<V22SemanticUnit>> {
    if rows.is_empty()
        || !matches!(unit_rows, 32 | 64)
        || rows.iter().any(|row| row.primary_cell != primary_cell)
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 two-pivot cell authority is empty or mismatched".to_string(),
        ));
    }
    let mut indexes = (0..rows.len()).collect::<Vec<_>>();
    indexes.sort_unstable_by(|left, right| {
        rows[*left]
            .canonical_record_id
            .cmp(&rows[*right].canonical_record_id)
            .then_with(|| rows[*left].record_id.cmp(&rows[*right].record_id))
    });
    let first = indexes[0];
    let second = two_pivot_farthest(rows, &indexes, first);
    let first_geometry = &rows[first].geometry;
    let projection_axis = rows[second]
        .geometry
        .iter()
        .zip(first_geometry)
        .map(|(second, first)| second - first)
        .collect::<Vec<_>>();
    let mut offset_geometry = vec![0.0_f32; first_geometry.len()];
    let mut scored = indexes
        .into_iter()
        .map(|index| {
            for ((offset, value), first) in offset_geometry
                .iter_mut()
                .zip(rows[index].geometry.iter())
                .zip(first_geometry)
            {
                *offset = value - first;
            }
            let score = crate::metric::dot_product(&offset_geometry, &projection_axis);
            (index, score)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|(left, left_score), (right, right_score)| {
        left_score.total_cmp(right_score).then_with(|| {
            rows[*left]
                .canonical_record_id
                .cmp(&rows[*right].canonical_record_id)
                .then_with(|| rows[*left].record_id.cmp(&rows[*right].record_id))
        })
    });
    Ok(scored
        .chunks(usize::from(unit_rows))
        .map(|chunk| V22SemanticUnit {
            primary_cell,
            record_ids: chunk
                .iter()
                .map(|(index, _)| rows[*index].record_id)
                .collect(),
        })
        .collect())
}

pub(crate) fn v22_cross_cell_order(cells: &[(u32, Box<[f64]>)]) -> Result<Vec<u32>> {
    if cells.is_empty()
        || cells.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        || cells[0].1.is_empty()
        || cells.iter().any(|(_, centroid)| {
            centroid.len() != cells[0].1.len() || centroid.iter().any(|value| !value.is_finite())
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V22 cross-cell centroid authority is empty or invalid".to_string(),
        ));
    }
    let centroids = cells
        .iter()
        .map(|(_, centroid)| centroid.clone())
        .collect::<Vec<_>>();
    let keys = cells.iter().map(|(cell, _)| *cell).collect::<Vec<_>>();
    Ok(nearest_neighbor_order(&centroids, &keys)
        .into_iter()
        .map(|index| cells[index].0)
        .collect())
}

pub(crate) fn v22_stage_l_cell_rows(
    spill: &V22StageLSpill,
    primary_cell: u32,
    normalize: bool,
) -> Result<(Vec<V22StageLSpillRow>, Vec<V22SemanticRow>)> {
    let rows = spill.read_cell(primary_cell)?;
    let semantic = rows
        .iter()
        .map(|row| {
            Ok(V22SemanticRow {
                record_id: row.source_ordinal,
                canonical_record_id: row.canonical_record_id.clone(),
                primary_cell,
                geometry: row.geometry(spill.dimensions(), spill.element_type(), normalize)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((rows, semantic))
}

pub(crate) fn v22_stage_l_pages_for_units(
    rows: Vec<V22StageLSpillRow>,
    semantic_rows: &[V22SemanticRow],
    units: &[V22SemanticUnit],
    projected_cell_index: u32,
    mut encode_centroid: impl FnMut(&[f32]) -> Result<Vec<u8>>,
) -> Result<Vec<(GlobalLeafPageInput, Box<[V22EncodedRecordAuthority]>)>> {
    if rows.is_empty()
        || semantic_rows.len() != rows.len()
        || units.is_empty()
        || rows
            .iter()
            .zip(semantic_rows)
            .any(|(row, semantic)| row.source_ordinal != semantic.record_id)
    {
        return Err(BorsukError::InvalidStorage(
            "V22 Stage L cell page authority is empty or mismatched".to_string(),
        ));
    }
    let first_ordinal = rows[0].source_ordinal;
    if rows
        .iter()
        .enumerate()
        .any(|(index, row)| row.source_ordinal != first_ordinal + index as u64)
    {
        return Err(BorsukError::InvalidStorage(
            "V22 Stage L cell scratch ordinals are not contiguous".to_string(),
        ));
    }
    let dimensions = semantic_rows[0].geometry.len();
    let primary_cell = semantic_rows[0].primary_cell;
    let mut owned = rows.into_iter().map(Some).collect::<Vec<_>>();
    let mut pages = Vec::with_capacity(units.len());
    for (leaf_ordinal, unit) in units.iter().enumerate() {
        if unit.primary_cell != primary_cell || unit.record_ids.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "V22 Stage L semantic unit conflicts with its cell".to_string(),
            ));
        }
        let mut centroid = vec![0.0_f64; dimensions];
        let mut page_rows = Vec::with_capacity(unit.record_ids.len());
        let mut authority = Vec::with_capacity(unit.record_ids.len());
        for ordinal in &unit.record_ids {
            let index = ordinal
                .checked_sub(first_ordinal)
                .and_then(|index| usize::try_from(index).ok())
                .filter(|index| *index < owned.len())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V22 Stage L semantic unit ordinal is outside its cell".to_string(),
                    )
                })?;
            let semantic = &semantic_rows[index];
            for (sum, value) in centroid.iter_mut().zip(semantic.geometry.iter()) {
                *sum += f64::from(*value);
            }
            let row = owned[index].take().ok_or_else(|| {
                BorsukError::InvalidStorage("V22 Stage L semantic units repeat a row".to_string())
            })?;
            authority.push(V22EncodedRecordAuthority {
                canonical_record_id: row.canonical_record_id.clone(),
                record_id: row.source_ordinal,
            });
            page_rows.push(GlobalLeafRowInput {
                id: RecordId::from_bytes(row.canonical_record_id.to_vec()),
                stamp: row.stamp,
                code: GlobalLeafCodeInput::from(row.code.into_vec()),
                exact: row.exact.into_vec(),
            });
        }
        let denominator = unit.record_ids.len() as f64;
        let centroid = centroid
            .into_iter()
            .map(|value| (value / denominator) as f32)
            .collect::<Vec<_>>();
        pages.push((
            GlobalLeafPageInput {
                cell_index: projected_cell_index,
                leaf_ordinal: u32::try_from(leaf_ordinal).map_err(|_| {
                    BorsukError::InvalidStorage("V22 Stage L leaf ordinal exceeds u32".to_string())
                })?,
                centroid_code: encode_centroid(&centroid)?,
                rows: page_rows,
            },
            authority.into_boxed_slice(),
        ));
    }
    if owned.iter().any(Option::is_some) {
        return Err(BorsukError::InvalidStorage(
            "V22 Stage L semantic units omit rows".to_string(),
        ));
    }
    Ok(pages)
}

#[derive(Debug, Clone)]
pub(crate) struct V22StageLPhysicalBlock {
    pub(crate) path: String,
    pub(crate) object_checksum: [u8; 32],
    pub(crate) object_encoded_bytes: u64,
    pub(crate) offset: u64,
    pub(crate) encoded_bytes: u32,
    pub(crate) decoded_bytes: u64,
    pub(crate) first_ordinal: u64,
    pub(crate) rows: u32,
}

pub(crate) fn v22_physical_candidate_units(
    blocks: &[V22StageLPhysicalBlock],
    total_rows: u64,
    candidate_ordinals: &[u64],
) -> Result<Vec<V22ProjectedUnit>> {
    if blocks.is_empty()
        || total_rows == 0
        || candidate_ordinals.is_empty()
        || candidate_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(BorsukError::InvalidStorage(
            "V22 physical layout authority is empty or invalid".to_string(),
        ));
    }
    let mut expected_ordinal = 0_u64;
    let mut projected = Vec::new();
    for block in blocks {
        let end_ordinal = block
            .first_ordinal
            .checked_add(u64::from(block.rows))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V22 physical ordinal range overflows".to_string())
            })?;
        if block.first_ordinal != expected_ordinal
            || block.path.is_empty()
            || block.rows == 0
            || block.encoded_bytes == 0
            || block.offset + u64::from(block.encoded_bytes) > block.object_encoded_bytes
        {
            return Err(BorsukError::InvalidStorage(
                "V22 physical block authority is noncanonical".to_string(),
            ));
        }
        let intersects = candidate_ordinals
            .partition_point(|ordinal| *ordinal < block.first_ordinal)
            < candidate_ordinals.partition_point(|ordinal| *ordinal < end_ordinal);
        if intersects {
            projected.push(V22ProjectedUnit {
                path: block.path.clone(),
                object_checksum: block.object_checksum,
                object_encoded_bytes: block.object_encoded_bytes,
                offset: block.offset,
                encoded_bytes: block.encoded_bytes,
                decoded_bytes: block.decoded_bytes,
                record_ids: (block.first_ordinal..end_ordinal).collect(),
            });
        }
        expected_ordinal = end_ordinal;
    }
    if expected_ordinal != total_rows {
        return Err(BorsukError::InvalidStorage(
            "V22 physical blocks do not cover the corpus".to_string(),
        ));
    }
    Ok(projected)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22ProjectedUnit {
    pub(crate) path: String,
    pub(crate) object_checksum: [u8; 32],
    pub(crate) object_encoded_bytes: u64,
    pub(crate) offset: u64,
    pub(crate) encoded_bytes: u32,
    pub(crate) decoded_bytes: u64,
    pub(crate) record_ids: Box<[u64]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22ProjectedObjectAuthority {
    pub(crate) path: String,
    pub(crate) checksum: [u8; 32],
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22EncodedRecordAuthority {
    pub(crate) canonical_record_id: Box<[u8]>,
    pub(crate) record_id: u64,
}

#[derive(Debug)]
pub(crate) struct V22EncodedProjection {
    pub(crate) encoded: EncodedCellCardGroup,
    pub(crate) units: Vec<V22ProjectedUnit>,
}

pub(crate) fn project_v22_encoded_cell_card_group(
    pages: &[GlobalLeafPageInput],
    records_by_card: &[Box<[V22EncodedRecordAuthority]>],
    dimensions: usize,
    element_type: VectorElementType,
    content_prefix: &str,
) -> Result<V22EncodedProjection> {
    let exact_row_bytes_usize = element_type.fixed_width_bytes(dimensions)?;
    if exact_row_bytes_usize == 0 || pages.len() != records_by_card.len() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 encoded group projection authority is empty or mismatched".to_string(),
        ));
    }
    let encoded = encode_cell_card_group(pages, dimensions, element_type)?;
    let mut canonical_record_ids = BTreeSet::new();
    let mut numeric_record_ids = BTreeSet::new();
    let units = project_v22_encoded_group(
        &encoded,
        pages,
        records_by_card,
        exact_row_bytes_usize,
        content_prefix,
        &mut canonical_record_ids,
        &mut numeric_record_ids,
    )?;
    Ok(V22EncodedProjection { encoded, units })
}

fn project_v22_encoded_group(
    encoded: &EncodedCellCardGroup,
    pages: &[GlobalLeafPageInput],
    records_by_card: &[Box<[V22EncodedRecordAuthority]>],
    exact_row_bytes_usize: usize,
    content_prefix: &str,
    canonical_record_ids: &mut BTreeSet<Box<[u8]>>,
    numeric_record_ids: &mut BTreeSet<u64>,
) -> Result<Vec<V22ProjectedUnit>> {
    let exact_row_bytes = exact_row_bytes_usize as u64;
    if exact_row_bytes == 0
        || encoded.cards.len() != pages.len()
        || pages.len() != records_by_card.len()
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 encoded group projection authority is empty or mismatched".to_string(),
        ));
    }
    let path = encoded.content_addressed_path(content_prefix)?;
    let (group, cards) = encoded.references(&path)?;
    let mut projected = Vec::new();
    for ((card, page), records) in cards.iter().zip(pages).zip(records_by_card) {
        if records.is_empty()
            || records.len() != card.head.rows as usize
            || records.len() != page.rows.len()
            || card.head.cell_index != page.cell_index
            || card.head.card_ordinal != page.leaf_ordinal
            || card.head.leaf_ordinal != page.leaf_ordinal
            || page.rows.iter().zip(records).any(|(row, record)| {
                row.id.as_bytes() != record.canonical_record_id.as_ref()
                    || row.exact.len() != exact_row_bytes_usize
                    || !canonical_record_ids.insert(record.canonical_record_id.clone())
                    || !numeric_record_ids.insert(record.record_id)
            })
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 encoded card record authority is empty or mismatched".to_string(),
            ));
        }
        let mut row_offset = 0_usize;
        let mut previous_block_end = 0_u64;
        for (block_index, block) in card.head.exact_blocks.iter().enumerate() {
            if block.block_ordinal != block_index as u32 || block.offset < previous_block_end {
                return Err(BorsukError::InvalidSearchOptions(
                    "V22 encoded blocks are not in canonical row order".to_string(),
                ));
            }
            let block_rows = block.rows as usize;
            let row_end = row_offset.checked_add(block_rows).ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 encoded block row range overflows".to_string(),
                )
            })?;
            let block_record_ids = records.get(row_offset..row_end).ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 encoded blocks exceed record authority".to_string(),
                )
            })?;
            let decoded_bytes = u64::try_from(block_rows)
                .ok()
                .and_then(|rows| rows.checked_mul(exact_row_bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidSearchOptions(
                        "V22 encoded block decoded bytes overflow".to_string(),
                    )
                })?;
            projected.push(V22ProjectedUnit {
                path: group.path.clone(),
                object_checksum: group.checksum,
                object_encoded_bytes: group.encoded_bytes,
                offset: block.offset,
                encoded_bytes: block.bytes,
                decoded_bytes,
                record_ids: block_record_ids
                    .iter()
                    .map(|record| record.record_id)
                    .collect(),
            });
            previous_block_end = block.offset + u64::from(block.bytes);
            row_offset = row_end;
        }
        if row_offset != records.len() {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 encoded blocks do not cover record authority".to_string(),
            ));
        }
    }
    Ok(projected)
}

pub(crate) fn project_v22_encoded_layout(
    pages: impl IntoIterator<Item = (GlobalLeafPageInput, Box<[V22EncodedRecordAuthority]>)>,
    dimensions: usize,
    element_type: VectorElementType,
    code_width: usize,
    content_prefix: &str,
) -> Result<Vec<V22ProjectedUnit>> {
    if code_width == 0 {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 encoded layout authority is empty".to_string(),
        ));
    }
    let exact_row_bytes = element_type.fixed_width_bytes(dimensions)?;
    let mut writer = CellCardGroupWriter::new(dimensions, element_type, code_width)?;
    let mut pending_authority = Vec::new();
    let mut projected = Vec::new();
    let mut canonical_record_ids = BTreeSet::new();
    let mut numeric_record_ids = BTreeSet::new();
    let mut saw_page = false;

    let flush = |writer: CellCardGroupWriter,
                 authority: &[Box<[V22EncodedRecordAuthority]>],
                 projected: &mut Vec<V22ProjectedUnit>,
                 canonical_record_ids: &mut BTreeSet<Box<[u8]>>,
                 numeric_record_ids: &mut BTreeSet<u64>|
     -> Result<()> {
        let (encoded, encoded_pages) = writer.finish_with_pages()?;
        projected.extend(project_v22_encoded_group(
            &encoded,
            &encoded_pages,
            authority,
            exact_row_bytes,
            content_prefix,
            canonical_record_ids,
            numeric_record_ids,
        )?);
        Ok(())
    };

    for (page, authority) in pages {
        saw_page = true;
        match writer.try_push(page)? {
            CellCardPush::Accepted => pending_authority.push(authority),
            CellCardPush::Full(page) => {
                flush(
                    writer,
                    &pending_authority,
                    &mut projected,
                    &mut canonical_record_ids,
                    &mut numeric_record_ids,
                )?;
                writer = CellCardGroupWriter::new(dimensions, element_type, code_width)?;
                match writer.try_push(page)? {
                    CellCardPush::Accepted => pending_authority = vec![authority],
                    CellCardPush::Full(_) => {
                        return Err(BorsukError::InvalidStorage(
                            "V22 encoded layout page exceeds an empty group".to_string(),
                        ));
                    }
                }
            }
        }
    }
    if !saw_page {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 encoded layout authority is empty".to_string(),
        ));
    }
    flush(
        writer,
        &pending_authority,
        &mut projected,
        &mut canonical_record_ids,
        &mut numeric_record_ids,
    )?;
    Ok(projected)
}

pub(crate) struct V22CandidateLayoutEncoder {
    dimensions: usize,
    element_type: VectorElementType,
    exact_row_bytes: usize,
    code_width: usize,
    content_prefix: String,
    total_rows: u64,
    candidate_ordinals: Box<[u64]>,
    writer: Option<CellCardGroupWriter>,
    pending_authority: Vec<Box<[V22EncodedRecordAuthority]>>,
    seen: Vec<u64>,
    seen_rows: u64,
    retained: Vec<V22ProjectedUnit>,
    saw_page: bool,
}

impl V22CandidateLayoutEncoder {
    pub(crate) fn new(
        dimensions: usize,
        element_type: VectorElementType,
        code_width: usize,
        content_prefix: &str,
        total_rows: u64,
        candidate_ordinals: &[u64],
    ) -> Result<Self> {
        if dimensions == 0
            || code_width == 0
            || content_prefix.is_empty()
            || total_rows == 0
            || candidate_ordinals.is_empty()
            || candidate_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
            || candidate_ordinals
                .iter()
                .any(|ordinal| *ordinal >= total_rows)
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 candidate encoder authority is empty or invalid".to_string(),
            ));
        }
        let rows = usize::try_from(total_rows).map_err(|_| {
            BorsukError::InvalidSearchOptions(
                "V22 candidate encoder row count exceeds usize".to_string(),
            )
        })?;
        Ok(Self {
            dimensions,
            element_type,
            exact_row_bytes: element_type.fixed_width_bytes(dimensions)?,
            code_width,
            content_prefix: content_prefix.to_string(),
            total_rows,
            candidate_ordinals: candidate_ordinals.into(),
            writer: Some(CellCardGroupWriter::new(
                dimensions,
                element_type,
                code_width,
            )?),
            pending_authority: Vec::new(),
            seen: vec![0; rows.div_ceil(64)],
            seen_rows: 0,
            retained: Vec::new(),
            saw_page: false,
        })
    }

    fn flush(&mut self) -> Result<()> {
        let writer = self.writer.take().ok_or_else(|| {
            BorsukError::InvalidStorage("V22 candidate encoder writer is absent".to_string())
        })?;
        let (encoded, encoded_pages) = writer.finish_with_pages()?;
        let mut canonical = BTreeSet::new();
        let mut numeric = BTreeSet::new();
        for unit in project_v22_encoded_group(
            &encoded,
            &encoded_pages,
            &self.pending_authority,
            self.exact_row_bytes,
            &self.content_prefix,
            &mut canonical,
            &mut numeric,
        )? {
            let mut intersects = false;
            for ordinal in &unit.record_ids {
                if *ordinal >= self.total_rows {
                    return Err(BorsukError::InvalidStorage(
                        "V22 candidate encoder ordinal exceeds scratch authority".to_string(),
                    ));
                }
                let ordinal = *ordinal as usize;
                let mask = 1_u64 << (ordinal % 64);
                let word = &mut self.seen[ordinal / 64];
                if *word & mask != 0 {
                    return Err(BorsukError::InvalidStorage(
                        "V22 candidate encoder repeats a scratch ordinal".to_string(),
                    ));
                }
                *word |= mask;
                self.seen_rows += 1;
                intersects |= self
                    .candidate_ordinals
                    .binary_search(&(ordinal as u64))
                    .is_ok();
            }
            if intersects {
                self.retained.push(unit);
            }
        }
        self.pending_authority.clear();
        Ok(())
    }

    pub(crate) fn push(
        &mut self,
        page: GlobalLeafPageInput,
        authority: Box<[V22EncodedRecordAuthority]>,
    ) -> Result<()> {
        self.saw_page = true;
        let writer = self.writer.as_mut().ok_or_else(|| {
            BorsukError::InvalidStorage("V22 candidate encoder is already finished".to_string())
        })?;
        match writer.try_push(page)? {
            CellCardPush::Accepted => self.pending_authority.push(authority),
            CellCardPush::Full(page) => {
                self.flush()?;
                let mut writer =
                    CellCardGroupWriter::new(self.dimensions, self.element_type, self.code_width)?;
                match writer.try_push(page)? {
                    CellCardPush::Accepted => {
                        self.pending_authority.push(authority);
                        self.writer = Some(writer);
                    }
                    CellCardPush::Full(_) => {
                        return Err(BorsukError::InvalidStorage(
                            "V22 candidate encoder page exceeds an empty group".to_string(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Vec<V22ProjectedUnit>> {
        if !self.saw_page {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 candidate encoder layout is empty".to_string(),
            ));
        }
        self.flush()?;
        let total_rows = self.total_rows as usize;
        let complete = self.seen_rows == self.total_rows
            && self.seen.iter().enumerate().all(|(word_index, word)| {
                let remaining = total_rows.saturating_sub(word_index * 64);
                let expected = if remaining >= 64 {
                    u64::MAX
                } else if remaining == 0 {
                    0
                } else {
                    (1_u64 << remaining) - 1
                };
                *word == expected
            });
        if !complete {
            return Err(BorsukError::InvalidStorage(
                "V22 candidate encoder does not cover scratch authority exactly".to_string(),
            ));
        }
        Ok(self.retained)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
/// First frozen Stage-L physical gate preventing an arm from advancing.
pub enum V22LayoutLimitingBound {
    /// The arm satisfies every frozen Stage-L eligibility bound.
    Eligible,
    /// The routed row set exceeds the frozen Stage-L row bound.
    RoutingRows,
    /// The exact prefix requires too many Standard S3 requests.
    Requests,
    /// The exact prefix exceeds the frozen physical-byte allowance.
    Bytes,
    /// Coalescing would exceed the frozen physical-amplification bound.
    Amplification,
}

pub(crate) fn v22_stage_l_layout_reports(
    layout: V22LayoutKind,
    microcluster_rows: Option<u8>,
    units: &[V22ProjectedUnit],
    query_prefixes: &[V22StageLQueryPrefix],
    routing_gates: &[(usize, u64)],
    exact_row_bytes: u64,
) -> Result<Vec<V22StageLLayoutArmReport>> {
    if units.is_empty()
        || query_prefixes.is_empty()
        || query_prefixes.len() != routing_gates.len()
        || exact_row_bytes == 0
    {
        return Err(BorsukError::InvalidStorage(
            "V22 Stage L layout report authority is empty or mismatched".to_string(),
        ));
    }
    let prepared = V22PreparedLayout::new(units, exact_row_bytes)?;
    let projected_objects = units
        .iter()
        .map(|unit| {
            (
                unit.path.clone(),
                (unit.object_checksum, unit.object_encoded_bytes),
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(
            |(path, (checksum, encoded_bytes))| V22StageLProjectedObject {
                path,
                checksum: blake3::Hash::from_bytes(checksum).to_hex().to_string(),
                encoded_bytes,
            },
        )
        .collect::<Vec<_>>();
    let mut reports = Vec::with_capacity(V22_EXACT_PREFIX_ROWS.len());
    for exact_prefix_rows in V22_EXACT_PREFIX_ROWS {
        let arm = V22LayoutCensusArm {
            layout,
            microcluster_rows,
            exact_prefix_rows,
        };
        arm.validate()?;
        let mut query_samples = Vec::with_capacity(query_prefixes.len());
        for (query, &(required_routing_cells, routed_rows)) in
            query_prefixes.iter().zip(routing_gates)
        {
            let prefix_rows = usize::from(exact_prefix_rows);
            let ranked = query.rows.get(..prefix_rows).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "V22 Stage L exact corpus is smaller than a frozen prefix".to_string(),
                )
            })?;
            let ranked = ranked.iter().map(|row| row.record_id).collect::<Vec<_>>();
            let census = prepared.census_prefix(&ranked, exact_row_bytes, 1_048_576, 4, 2)?;
            let routing_eligible = required_routing_cells > 0 && routed_rows <= 512_000;
            query_samples.push(V22StageLLayoutQuerySample {
                query_index: query.query_index,
                exact_prefix_rows,
                required_routing_cells,
                gt_cell_hits: 10,
                gt_cell_coverage_ppm: 1_000_000,
                routed_rows,
                useful_bytes: census.useful_bytes,
                selected_bytes: census.selected_bytes,
                physical_bytes: census.physical_bytes,
                speculative_bytes: census.speculative_bytes,
                requests: census.requests,
                selected_rows: census.selected_rows,
                packing_purity_ppm: census.packing_purity_ppm,
                physical_amplification_ppm: census.physical_amplification_ppm,
                physical_limiting_bound: census.limiting_bound,
                routing_eligible,
                limiting_bound: if routing_eligible {
                    census.limiting_bound
                } else {
                    V22LayoutLimitingBound::RoutingRows
                },
                eligible: routing_eligible && census.eligible,
                ranges: census.ranges.into_vec(),
            });
        }
        let eligible = query_samples.iter().all(|sample| sample.eligible);
        reports.push(V22StageLLayoutArmReport {
            layout,
            microcluster_rows,
            exact_prefix_rows,
            projected_objects: projected_objects.clone(),
            query_samples,
            eligible,
        });
    }
    Ok(reports)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22LayoutCensus {
    pub(crate) projected_objects: Box<[V22ProjectedObjectAuthority]>,
    pub(crate) useful_bytes: u64,
    pub(crate) selected_bytes: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) speculative_bytes: u64,
    pub(crate) requests: usize,
    pub(crate) selected_rows: u64,
    pub(crate) rows_per_range: Box<[u64]>,
    pub(crate) blocks_per_range: Box<[usize]>,
    pub(crate) ranges: Box<[V22StageLRange]>,
    pub(crate) packing_purity_ppm: u64,
    pub(crate) physical_amplification_ppm: u64,
    pub(crate) limiting_bound: V22LayoutLimitingBound,
    pub(crate) eligible: bool,
}

struct V22PreparedLayout<'a> {
    units: &'a [V22ProjectedUnit],
    record_to_unit: BTreeMap<u64, usize>,
    groups: BTreeMap<&'a str, Arc<CellCardGroupRef>>,
    projected_objects: Box<[V22ProjectedObjectAuthority]>,
}

impl<'a> V22PreparedLayout<'a> {
    fn new(units: &'a [V22ProjectedUnit], exact_row_bytes: u64) -> Result<Self> {
        if units.is_empty() || exact_row_bytes == 0 {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 projected layout authority is empty".to_string(),
            ));
        }
        let mut record_to_unit = BTreeMap::<u64, usize>::new();
        let mut ranges_by_path = BTreeMap::<&str, Vec<(u64, u64)>>::new();
        let mut path_authority = BTreeMap::<&str, (u64, [u8; 32])>::new();
        for (unit_index, unit) in units.iter().enumerate() {
            let expected_decoded_bytes = u64::try_from(unit.record_ids.len())
                .ok()
                .and_then(|rows| rows.checked_mul(exact_row_bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidSearchOptions(
                        "V22 projected unit decoded byte count overflows".to_string(),
                    )
                })?;
            let end = unit
                .offset
                .checked_add(u64::from(unit.encoded_bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidSearchOptions(
                        "V22 projected unit range overflows".to_string(),
                    )
                })?;
            if unit.path.is_empty()
                || unit.encoded_bytes == 0
                || unit.record_ids.is_empty()
                || u32::try_from(unit.record_ids.len()).is_err()
                || unit.decoded_bytes != expected_decoded_bytes
                || end > unit.object_encoded_bytes
            {
                return Err(BorsukError::InvalidSearchOptions(
                    "V22 projected unit is empty or oversized".to_string(),
                ));
            }
            for record_id in &unit.record_ids {
                if record_to_unit.insert(*record_id, unit_index).is_some() {
                    return Err(BorsukError::InvalidSearchOptions(
                        "V22 projected units contain a duplicate record".to_string(),
                    ));
                }
            }
            ranges_by_path
                .entry(unit.path.as_str())
                .or_default()
                .push((unit.offset, end));
            match path_authority.entry(unit.path.as_str()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((unit.object_encoded_bytes, unit.object_checksum));
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    if *entry.get() != (unit.object_encoded_bytes, unit.object_checksum) {
                        return Err(BorsukError::InvalidSearchOptions(
                            "V22 projected object checksum authority conflicts".to_string(),
                        ));
                    }
                }
            }
        }
        for ranges in ranges_by_path.values_mut() {
            ranges.sort_unstable();
            if ranges.windows(2).any(|pair| pair[1].0 < pair[0].1) {
                return Err(BorsukError::InvalidSearchOptions(
                    "V22 projected unit ranges overlap".to_string(),
                ));
            }
        }
        let groups = path_authority
            .into_iter()
            .map(|(path, (encoded_bytes, checksum))| {
                (
                    path,
                    Arc::new(CellCardGroupRef {
                        path: path.to_string(),
                        checksum,
                        encoded_bytes,
                        code_plane_offset: 0,
                        code_plane_bytes: 0,
                        code_plane_checksum: [0; 32],
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let projected_objects = groups
            .values()
            .map(|group| V22ProjectedObjectAuthority {
                path: group.path.clone(),
                checksum: group.checksum,
                encoded_bytes: group.encoded_bytes,
            })
            .collect();
        Ok(Self {
            units,
            record_to_unit,
            groups,
            projected_objects,
        })
    }

    fn census_prefix(
        &self,
        ranked_record_ids: &[u64],
        exact_row_bytes: u64,
        max_physical_bytes: u64,
        max_requests: usize,
        max_physical_amplification: u64,
    ) -> Result<V22LayoutCensus> {
        if ranked_record_ids.is_empty()
            || exact_row_bytes == 0
            || max_physical_bytes == 0
            || max_requests == 0
            || !(1..=CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION)
                .contains(&max_physical_amplification)
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 projected layout census authority is empty".to_string(),
            ));
        }
        let ranked_unique = ranked_record_ids.iter().copied().collect::<BTreeSet<_>>();
        if ranked_unique.len() != ranked_record_ids.len()
            || ranked_record_ids
                .iter()
                .any(|record_id| !self.record_to_unit.contains_key(record_id))
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 ranked prefix is duplicate or absent from the projected layout".to_string(),
            ));
        }
        let mut selected_units = BTreeSet::<usize>::new();
        let mut ranked = Vec::new();
        for (rank, record_id) in ranked_record_ids.iter().enumerate() {
            let unit_index = self.record_to_unit[record_id];
            if !selected_units.insert(unit_index) {
                continue;
            }
            let unit = &self.units[unit_index];
            ranked.push(RankedCellCardExactBlock {
                head_index: unit_index,
                group: Arc::clone(&self.groups[unit.path.as_str()]),
                cell_index: 0,
                card_ordinal: u32::try_from(unit_index).map_err(|_| {
                    BorsukError::InvalidSearchOptions(
                        "V22 projected unit ordinal overflows".to_string(),
                    )
                })?,
                reference: CellCardExactBlockRef {
                    block_ordinal: 0,
                    offset: unit.offset,
                    metadata_bytes: 0,
                    body_bytes: unit.encoded_bytes,
                    bytes: unit.encoded_bytes,
                    rows: unit.record_ids.len() as u32,
                    checksum: [0; 32],
                },
                distance: rank as f32,
                row_distances: Box::default(),
            });
        }
        self.finish_census(
            ranked_record_ids,
            ranked,
            exact_row_bytes,
            max_physical_bytes,
            max_requests,
            max_physical_amplification,
        )
    }

    fn finish_census(
        &self,
        ranked_record_ids: &[u64],
        ranked: Vec<RankedCellCardExactBlock>,
        exact_row_bytes: u64,
        max_physical_bytes: u64,
        max_requests: usize,
        max_physical_amplification: u64,
    ) -> Result<V22LayoutCensus> {
        let selected_bytes = ranked.iter().try_fold(0_u64, |total, block| {
            total
                .checked_add(u64::from(block.reference.bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidSearchOptions(
                        "V22 selected encoded byte count overflows".to_string(),
                    )
                })
        })?;
        let measurement_ceiling = if selected_bytes <= max_physical_bytes {
            max_physical_bytes
        } else {
            selected_bytes
                .checked_mul(max_physical_amplification)
                .ok_or_else(|| {
                    BorsukError::InvalidSearchOptions(
                        "V22 physical measurement ceiling overflows".to_string(),
                    )
                })?
        };
        let plan = plan_cell_card_exact_wave_with_amplification(
            &ranked,
            measurement_ceiling,
            ranked.len(),
            max_physical_amplification,
        )?;
        let useful_bytes = u64::try_from(ranked_record_ids.len())
            .ok()
            .and_then(|rows| rows.checked_mul(exact_row_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions("V22 useful byte count overflows".to_string())
            })?;
        let purity_numerator = useful_bytes.checked_mul(1_000_000).ok_or_else(|| {
            BorsukError::InvalidSearchOptions("V22 packing purity overflows".to_string())
        })?;
        let amplification_numerator =
            plan.physical_bytes()
                .checked_mul(1_000_000)
                .ok_or_else(|| {
                    BorsukError::InvalidSearchOptions(
                        "V22 physical amplification overflows".to_string(),
                    )
                })?;
        let limiting_bound = if plan.physical_bytes() > max_physical_bytes {
            V22LayoutLimitingBound::Bytes
        } else if plan.requests() > max_requests {
            let maximum_amplification_plan = plan_cell_card_exact_wave_with_amplification(
                &ranked,
                max_physical_bytes,
                ranked.len(),
                CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION,
            )?;
            if max_physical_amplification < CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION
                && maximum_amplification_plan.requests() <= max_requests
                && maximum_amplification_plan.physical_bytes() <= max_physical_bytes
            {
                V22LayoutLimitingBound::Amplification
            } else {
                V22LayoutLimitingBound::Requests
            }
        } else {
            V22LayoutLimitingBound::Eligible
        };
        let projected_objects = self.projected_objects.clone();
        let ranges = plan
            .reads()
            .iter()
            .map(|read| V22StageLRange {
                path: read.group.path.clone(),
                start: read.start,
                end: read.end,
                selected_bytes: read.selected_bytes,
                rows: read
                    .blocks
                    .iter()
                    .map(|block| u64::from(block.reference.rows))
                    .sum(),
                blocks: read.blocks.len(),
            })
            .collect();
        Ok(V22LayoutCensus {
            projected_objects,
            useful_bytes,
            selected_bytes: plan.selected_bytes(),
            physical_bytes: plan.physical_bytes(),
            speculative_bytes: plan.speculative_bytes(),
            requests: plan.requests(),
            selected_rows: plan.rows(),
            rows_per_range: plan
                .reads()
                .iter()
                .map(|read| {
                    read.blocks
                        .iter()
                        .map(|block| u64::from(block.reference.rows))
                        .sum()
                })
                .collect(),
            blocks_per_range: plan.reads().iter().map(|read| read.blocks.len()).collect(),
            ranges,
            packing_purity_ppm: purity_numerator / plan.physical_bytes(),
            physical_amplification_ppm: amplification_numerator / plan.selected_bytes(),
            limiting_bound,
            eligible: limiting_bound == V22LayoutLimitingBound::Eligible,
        })
    }
}

pub(crate) fn v22_census_layout_prefix(
    units: &[V22ProjectedUnit],
    ranked_record_ids: &[u64],
    exact_row_bytes: u64,
    max_physical_bytes: u64,
    max_requests: usize,
    max_physical_amplification: u64,
) -> Result<V22LayoutCensus> {
    V22PreparedLayout::new(units, exact_row_bytes)?.census_prefix(
        ranked_record_ids,
        exact_row_bytes,
        max_physical_bytes,
        max_requests,
        max_physical_amplification,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        V22CandidateLayoutEncoder, V22EncodedRecordAuthority, V22ExactPrefixAccumulator,
        V22LayoutCensusArm, V22LayoutKind, V22LayoutLimitingBound, V22ProjectedUnit,
        V22SemanticRow, V22SemanticUnit, V22StageLSpillRow, V22StageLSpillWriter,
        nearest_neighbor_order, project_v22_encoded_cell_card_group, project_v22_encoded_layout,
        project_v22_semantic_layout, project_v22_two_pivot_layout, routing_coverage_at_probe,
        routing_rank, v22_census_layout_prefix, v22_layout_census_arms,
        v22_stage_l_pages_for_units,
    };
    use crate::{
        VectorElementType,
        global_leaf::{GlobalLeafCodeInput, GlobalLeafPageInput, GlobalLeafRowInput},
        mutation::{MutationStamp, MutationVersion},
        record::RecordId,
    };
    use std::{
        collections::BTreeSet,
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
    };

    fn stage_l_spill_row(source_ordinal: u64) -> V22StageLSpillRow {
        V22StageLSpillRow {
            source_ordinal,
            canonical_record_id: format!("row-{source_ordinal}").into_bytes().into(),
            stamp: MutationStamp::new(
                MutationVersion::from_parts(source_ordinal + 1, [7; 16]),
                [source_ordinal as u8; 32],
            ),
            code: vec![source_ordinal as u8; 2].into(),
            exact: vec![source_ordinal as u8; 2].into(),
        }
    }

    #[test]
    fn v22_stage_l_scratch_is_exact_bounded_authenticated_and_ephemeral() {
        let parent = tempfile::tempdir().unwrap();
        let cells = BTreeSet::from([3, 4]);
        let mut writer = V22StageLSpillWriter::create(
            parent.path(),
            "root-checksum",
            2,
            VectorElementType::Int8,
            2,
            &cells,
            2,
        )
        .unwrap();
        writer.append_batch(3, &[stage_l_spill_row(0)]).unwrap();
        writer.append_batch(3, &[stage_l_spill_row(1)]).unwrap();
        writer.append_batch(4, &[stage_l_spill_row(2)]).unwrap();
        let spill = writer.finish().unwrap();
        assert_eq!(spill.total_rows(), 3);
        assert_eq!(spill.cell_rows(), vec![(3, 2), (4, 1)]);
        assert_eq!(spill.read_cell(3).unwrap()[1].source_ordinal, 1);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 1);
        drop(spill);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);

        let one_cell = BTreeSet::from([3]);
        let mut bounded = V22StageLSpillWriter::create(
            parent.path(),
            "root-checksum",
            2,
            VectorElementType::Int8,
            2,
            &one_cell,
            1,
        )
        .unwrap();
        assert!(
            bounded
                .append_batch(3, &[stage_l_spill_row(0), stage_l_spill_row(1)])
                .is_err()
        );

        let mut noncontiguous = V22StageLSpillWriter::create(
            parent.path(),
            "root-checksum",
            2,
            VectorElementType::Int8,
            2,
            &cells,
            2,
        )
        .unwrap();
        noncontiguous
            .append_batch(3, &[stage_l_spill_row(0)])
            .unwrap();
        noncontiguous
            .append_batch(4, &[stage_l_spill_row(1)])
            .unwrap();
        assert!(
            noncontiguous
                .append_batch(3, &[stage_l_spill_row(2)])
                .is_err()
        );
    }

    #[test]
    fn v22_stage_l_scratch_rejects_header_and_cell_corruption() {
        let parent = tempfile::tempdir().unwrap();
        let cells = BTreeSet::from([3]);
        let make_spill = || {
            let mut writer = V22StageLSpillWriter::create(
                parent.path(),
                "root-checksum",
                2,
                VectorElementType::Int8,
                2,
                &cells,
                2,
            )
            .unwrap();
            writer.append_batch(3, &[stage_l_spill_row(0)]).unwrap();
            writer.finish().unwrap()
        };

        let spill = make_spill();
        let mut file = OpenOptions::new().write(true).open(&spill.path).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        assert!(spill.read_cell(3).is_err());
        drop(spill);

        let spill = make_spill();
        let mut file = OpenOptions::new().write(true).open(&spill.path).unwrap();
        file.seek(SeekFrom::Start(spill.extents[0].offset)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        assert!(spill.read_cell(3).is_err());
    }

    #[test]
    fn v22_candidate_encoder_retains_reachable_units_and_requires_exact_coverage() {
        let page = GlobalLeafPageInput {
            cell_index: 3,
            leaf_ordinal: 0,
            centroid_code: vec![1, 2],
            rows: (0_u64..3)
                .map(|record_id| GlobalLeafRowInput {
                    id: RecordId::from_bytes(format!("row-{record_id}").into_bytes()),
                    stamp: MutationStamp::new(
                        MutationVersion::from_parts(record_id + 1, [3; 16]),
                        [record_id as u8; 32],
                    ),
                    code: GlobalLeafCodeInput::from(vec![record_id as u8; 2]),
                    exact: vec![record_id as u8; 2],
                })
                .collect(),
        };
        let authority = (0_u64..3)
            .map(|record_id| V22EncodedRecordAuthority {
                canonical_record_id: format!("row-{record_id}").into_bytes().into(),
                record_id,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let mut encoder = V22CandidateLayoutEncoder::new(
            2,
            VectorElementType::Int8,
            2,
            "v22-stage-l/candidates",
            3,
            &[1],
        )
        .unwrap();
        encoder.push(page.clone(), authority.clone()).unwrap();
        let retained = encoder.finish().unwrap();
        assert!(!retained.is_empty());
        assert!(
            retained
                .iter()
                .any(|unit| unit.record_ids.binary_search(&1).is_ok())
        );

        let mut incomplete = V22CandidateLayoutEncoder::new(
            2,
            VectorElementType::Int8,
            2,
            "v22-stage-l/incomplete",
            4,
            &[1],
        )
        .unwrap();
        incomplete.push(page.clone(), authority).unwrap();
        assert!(incomplete.finish().is_err());

        let duplicate_authority = [0_u64, 0, 2]
            .into_iter()
            .map(|record_id| V22EncodedRecordAuthority {
                canonical_record_id: format!("duplicate-{record_id}").into_bytes().into(),
                record_id,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut duplicate = V22CandidateLayoutEncoder::new(
            2,
            VectorElementType::Int8,
            2,
            "v22-stage-l/duplicate",
            3,
            &[1],
        )
        .unwrap();
        duplicate.push(page, duplicate_authority).unwrap();
        assert!(duplicate.finish().is_err());
    }

    #[test]
    fn v22_candidate_encoder_uses_layout_order_not_source_cell_order() {
        let mut first = stage_l_spill_row(0);
        first.canonical_record_id = b"first".to_vec().into();
        let mut second = stage_l_spill_row(1);
        second.canonical_record_id = b"second".to_vec().into();
        let first_semantic = [V22SemanticRow {
            record_id: 0,
            canonical_record_id: first.canonical_record_id.clone(),
            primary_cell: 9,
            geometry: vec![0.0, 1.0].into(),
        }];
        let second_semantic = [V22SemanticRow {
            record_id: 1,
            canonical_record_id: second.canonical_record_id.clone(),
            primary_cell: 2,
            geometry: vec![1.0, 0.0].into(),
        }];
        let first_units = [V22SemanticUnit {
            primary_cell: 9,
            record_ids: vec![0].into(),
        }];
        let second_units = [V22SemanticUnit {
            primary_cell: 2,
            record_ids: vec![1].into(),
        }];
        let first_pages =
            v22_stage_l_pages_for_units(vec![first], &first_semantic, &first_units, 0, |_| {
                Ok(vec![0, 0])
            })
            .unwrap();
        let second_pages =
            v22_stage_l_pages_for_units(vec![second], &second_semantic, &second_units, 1, |_| {
                Ok(vec![1, 1])
            })
            .unwrap();
        let mut encoder = V22CandidateLayoutEncoder::new(
            2,
            VectorElementType::Int8,
            2,
            "v22-stage-l/nonmonotonic-source-cells",
            2,
            &[0],
        )
        .unwrap();
        for (page, authority) in first_pages.into_iter().chain(second_pages) {
            encoder.push(page, authority).unwrap();
        }
        assert!(!encoder.finish().unwrap().is_empty());
    }

    #[test]
    fn v22_layout_census_authority_is_exact_and_canonical() {
        let arms = v22_layout_census_arms().unwrap();
        assert_eq!(arms.len(), 42);
        assert_eq!(
            arms[0],
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20Physical,
                microcluster_rows: None,
                exact_prefix_rows: 10,
            }
        );
        assert_eq!(arms[5].exact_prefix_rows, 2048);
        assert_eq!(arms[6].layout, V22LayoutKind::V20TwoPivotRepacked);
        assert_eq!(arms[6].microcluster_rows, Some(32));
        assert_eq!(arms[12].microcluster_rows, Some(64));
        assert_eq!(arms[18].layout, V22LayoutKind::SemanticWithinCell);
        assert_eq!(arms[18].microcluster_rows, Some(32));
        assert_eq!(arms[24].microcluster_rows, Some(64));
        assert_eq!(arms[30].layout, V22LayoutKind::SemanticCrossCell);
        assert_eq!(arms[30].microcluster_rows, Some(32));
        assert_eq!(arms[36].microcluster_rows, Some(64));
        assert_eq!(arms[41].exact_prefix_rows, 2048);
        for arm in arms {
            arm.validate().unwrap();
        }
    }

    #[test]
    fn v22_stage_l_exact_prefix_is_bounded_and_deterministic() {
        let mut accumulator = V22ExactPrefixAccumulator::new(2).unwrap();
        for record_id in (0_u64..2055).rev() {
            accumulator
                .observe(
                    record_id,
                    &record_id.to_be_bytes(),
                    (record_id % 7) as u32,
                    &[record_id as f32, (2054 - record_id) as f32],
                )
                .unwrap();
        }
        let prefixes = accumulator.finish().unwrap();
        assert_eq!(prefixes.len(), 2);
        assert_eq!(prefixes[0].len(), 2048);
        assert_eq!(prefixes[1].len(), 2048);
        assert_eq!(prefixes[0][0].record_id, 0);
        assert_eq!(prefixes[0][2047].record_id, 2047);
        assert_eq!(prefixes[1][0].record_id, 2054);
        assert_eq!(prefixes[1][2047].record_id, 7);

        let mut ties = V22ExactPrefixAccumulator::new(1).unwrap();
        ties.observe(1, b"z", 0, &[1.0]).unwrap();
        ties.observe(2, b"a", 0, &[1.0]).unwrap();
        let tied = ties.finish().unwrap();
        assert_eq!(tied[0][0].canonical_record_id.as_ref(), b"a");
        assert!(V22ExactPrefixAccumulator::new(0).is_err());
        let mut invalid = V22ExactPrefixAccumulator::new(1).unwrap();
        assert!(invalid.observe(0, b"", 0, &[0.0]).is_err());
        assert!(invalid.observe(0, b"x", 0, &[f32::NAN]).is_err());
        assert!(invalid.observe(0, b"x", 0, &[0.0, 1.0]).is_err());
    }

    #[test]
    fn v22_layout_census_authority_rejects_factor_drift() {
        for arm in [
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20Physical,
                microcluster_rows: Some(32),
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20TwoPivotRepacked,
                microcluster_rows: None,
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::SemanticWithinCell,
                microcluster_rows: Some(48),
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::SemanticWithinCell,
                microcluster_rows: Some(32),
                exact_prefix_rows: 768,
            },
        ] {
            assert!(arm.validate().is_err());
        }
    }

    #[test]
    fn v22_layout_census_routing_rank_subsumes_the_probe_sweep() {
        let ordered_cells = [9, 3, 7, 2, 5];
        assert_eq!(routing_rank(&ordered_cells, 9).unwrap(), 1);
        assert_eq!(routing_rank(&ordered_cells, 2).unwrap(), 4);
        assert_eq!(
            routing_coverage_at_probe(&[1, 4, 4, 5], 3, ordered_cells.len()).unwrap(),
            1
        );
        assert_eq!(
            routing_coverage_at_probe(&[1, 4, 4, 5], 4, ordered_cells.len()).unwrap(),
            3
        );
        assert!(routing_rank(&[], 3).is_err());
        assert!(routing_rank(&[3, 3], 3).is_err());
        assert!(routing_rank(&[3], 7).is_err());
        assert!(routing_coverage_at_probe(&[], 4, 5).is_err());
        assert!(routing_coverage_at_probe(&[1, 0], 4, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 0, 5).is_err());
        assert!(routing_coverage_at_probe(&[6], 5, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 6, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 1, 0).is_err());
        assert_eq!(routing_coverage_at_probe(&[4096], 4096, 4096).unwrap(), 1);
        assert_eq!(
            routing_coverage_at_probe(&[16384], 16384, 16384).unwrap(),
            1
        );
    }

    #[test]
    fn v22_layout_oracle_is_deterministic_and_separates_cell_placement() {
        let mut rows = Vec::new();
        for (primary_cell, cell_position) in [(0_u32, 0.0_f32), (1, 10.0), (2, -10.0)] {
            for ordinal in 0_u64..321 {
                let quadrant = (ordinal % 4) as f32;
                let record_id = u64::from(primary_cell) * 1000 + ordinal;
                rows.push(V22SemanticRow {
                    record_id,
                    canonical_record_id: record_id.to_be_bytes().into(),
                    primary_cell,
                    geometry: vec![
                        cell_position + quadrant * 100.0,
                        quadrant * 10.0 + ordinal as f32 / 1000.0,
                    ]
                    .into(),
                });
            }
        }
        let within = project_v22_semantic_layout(&rows, &[2, 1, 0], 32, false).unwrap();
        assert_eq!(within.len(), 33);
        assert!(within[..11].iter().all(|unit| unit.primary_cell == 2));
        assert!(within[11..22].iter().all(|unit| unit.primary_cell == 1));
        assert!(within[22..].iter().all(|unit| unit.primary_cell == 0));
        for unit in &within {
            assert!((29..=30).contains(&unit.record_ids.len()));
        }

        let cross = project_v22_semantic_layout(&rows, &[2, 1, 0], 32, true).unwrap();
        assert_eq!(cross.len(), 33);
        assert!(cross[..11].iter().all(|unit| unit.primary_cell == 0));
        assert!(cross[11..22].iter().all(|unit| unit.primary_cell == 1));
        assert!(cross[22..].iter().all(|unit| unit.primary_cell == 2));
        assert_ne!(within, cross);

        let two_pivot = project_v22_two_pivot_layout(&rows, &[2, 1, 0], 32).unwrap();
        assert_eq!(two_pivot.len(), 33);
        assert!(two_pivot[..11].iter().all(|unit| unit.primary_cell == 2));
        assert!(two_pivot[11..22].iter().all(|unit| unit.primary_cell == 1));
        assert!(two_pivot[22..].iter().all(|unit| unit.primary_cell == 0));
        assert_eq!(
            two_pivot
                .iter()
                .map(|unit| unit.record_ids.len())
                .collect::<Vec<_>>(),
            [32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 1].repeat(3)
        );
        assert_ne!(two_pivot, within);

        rows.reverse();
        assert_eq!(
            project_v22_semantic_layout(&rows, &[2, 1, 0], 32, false).unwrap(),
            within
        );
        assert_eq!(
            project_v22_semantic_layout(&rows, &[2, 1, 0], 32, true).unwrap(),
            cross
        );
        assert_eq!(
            project_v22_two_pivot_layout(&rows, &[2, 1, 0], 32).unwrap(),
            two_pivot
        );

        let centroids = vec![
            vec![0.0].into_boxed_slice(),
            vec![4.0].into_boxed_slice(),
            vec![1.0].into_boxed_slice(),
            vec![10.0].into_boxed_slice(),
        ];
        assert_eq!(
            nearest_neighbor_order(&centroids, &[0, 1, 2, 3]),
            [0, 2, 1, 3]
        );

        let metric_rows = (0_u64..128)
            .map(|record_id| {
                let cluster = (record_id % 4) as f32;
                V22SemanticRow {
                    record_id,
                    canonical_record_id: record_id.to_be_bytes().into(),
                    primary_cell: 0,
                    geometry: vec![cluster * 1000.0, record_id as f32 / 1000.0].into(),
                }
            })
            .collect::<Vec<_>>();
        let metric_units = project_v22_semantic_layout(&metric_rows, &[0], 32, false).unwrap();
        assert_eq!(metric_units.len(), 4);
        for unit in metric_units {
            let cluster = unit.record_ids[0] % 4;
            assert!(
                unit.record_ids
                    .iter()
                    .all(|record_id| record_id % 4 == cluster)
            );
        }

        let two_pivot_bytes = [
            V22SemanticRow {
                record_id: 10,
                canonical_record_id: vec![2].into(),
                primary_cell: 0,
                geometry: vec![0.0].into(),
            },
            V22SemanticRow {
                record_id: 20,
                canonical_record_id: vec![1].into(),
                primary_cell: 0,
                geometry: vec![10.0].into(),
            },
            V22SemanticRow {
                record_id: 30,
                canonical_record_id: vec![3].into(),
                primary_cell: 0,
                geometry: vec![20.0].into(),
            },
        ];
        assert_eq!(
            project_v22_two_pivot_layout(&two_pivot_bytes, &[0], 32).unwrap()[0]
                .record_ids
                .as_ref(),
            &[30, 20, 10]
        );
        assert_eq!(
            project_v22_semantic_layout(&two_pivot_bytes, &[0], 32, false).unwrap()[0]
                .record_ids
                .as_ref(),
            &[20, 10, 30]
        );
    }

    #[test]
    fn v22_layout_oracle_rejects_unauthenticated_geometry_and_order() {
        let valid = [V22SemanticRow {
            record_id: 7,
            canonical_record_id: vec![7].into(),
            primary_cell: 3,
            geometry: vec![1.0, 2.0].into(),
        }];
        assert!(project_v22_semantic_layout(&valid, &[3], 32, false).is_ok());
        assert!(project_v22_two_pivot_layout(&valid, &[3], 32).is_ok());
        assert!(project_v22_two_pivot_layout(&valid, &[3], 48).is_err());
        assert!(project_v22_two_pivot_layout(&valid, &[4], 32).is_err());
        assert!(project_v22_semantic_layout(&valid, &[3], 48, false).is_err());
        assert!(project_v22_semantic_layout(&valid, &[], 32, false).is_err());
        assert!(project_v22_semantic_layout(&valid, &[3, 3], 32, false).is_err());
        assert!(project_v22_semantic_layout(&valid, &[4], 32, false).is_err());

        let duplicate = [valid[0].clone(), valid[0].clone()];
        assert!(project_v22_semantic_layout(&duplicate, &[3], 32, false).is_err());
        assert!(project_v22_two_pivot_layout(&duplicate, &[3], 32).is_err());
        let duplicate_canonical = [
            valid[0].clone(),
            V22SemanticRow {
                record_id: 8,
                canonical_record_id: vec![7].into(),
                primary_cell: 3,
                geometry: vec![3.0, 4.0].into(),
            },
        ];
        assert!(project_v22_semantic_layout(&duplicate_canonical, &[3], 32, false).is_err());
        assert!(project_v22_two_pivot_layout(&duplicate_canonical, &[3], 32).is_err());
        let mismatched = [
            valid[0].clone(),
            V22SemanticRow {
                record_id: 8,
                canonical_record_id: vec![8].into(),
                primary_cell: 3,
                geometry: vec![1.0].into(),
            },
        ];
        assert!(project_v22_semantic_layout(&mismatched, &[3], 32, false).is_err());
        let nonfinite = [V22SemanticRow {
            record_id: 8,
            canonical_record_id: vec![8].into(),
            primary_cell: 3,
            geometry: vec![f32::NAN, 2.0].into(),
        }];
        assert!(project_v22_semantic_layout(&nonfinite, &[3], 32, false).is_err());
    }

    #[test]
    fn v22_layout_oracle_derives_ranges_from_the_real_encoder() {
        const DIMENSIONS: usize = 96;
        const EXACT_ROW_BYTES: usize = DIMENSIONS * std::mem::size_of::<f32>();

        assert!(
            project_v22_encoded_cell_card_group(
                &[],
                &[],
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/empty",
            )
            .is_err()
        );

        let mut next_record_id = 0_u64;
        let mut make_page = |cell_index: u32, rows: usize, compressible: bool| {
            let mut authority = Vec::with_capacity(rows);
            let rows = (0..rows)
                .map(|ordinal| {
                    let record_id = next_record_id;
                    next_record_id += 1;
                    let canonical = format!("v22-{cell_index:02}-{ordinal:04}").into_bytes();
                    authority.push(V22EncodedRecordAuthority {
                        canonical_record_id: canonical.clone().into(),
                        record_id,
                    });
                    let exact = if compressible {
                        vec![0; EXACT_ROW_BYTES]
                    } else {
                        (0..EXACT_ROW_BYTES)
                            .map(|byte| {
                                let mixed = record_id
                                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                                    .rotate_left((byte % 63) as u32)
                                    ^ byte as u64;
                                mixed as u8
                            })
                            .collect()
                    };
                    GlobalLeafRowInput {
                        id: RecordId::from_bytes(canonical),
                        stamp: MutationStamp::new(
                            MutationVersion::from_parts(record_id + 1, [cell_index as u8; 16]),
                            [record_id as u8; 32],
                        ),
                        code: GlobalLeafCodeInput::from(vec![cell_index as u8, ordinal as u8]),
                        exact,
                    }
                })
                .collect();
            (
                GlobalLeafPageInput {
                    cell_index,
                    leaf_ordinal: cell_index,
                    centroid_code: vec![cell_index as u8, 0],
                    rows,
                },
                authority.into_boxed_slice(),
            )
        };
        let fixtures = [
            make_page(0, 1, false),
            make_page(1, 32, true),
            make_page(2, 65, false),
        ];
        let pages = fixtures
            .iter()
            .map(|(page, _)| page.clone())
            .collect::<Vec<_>>();
        let authority = fixtures
            .into_iter()
            .map(|(_, authority)| authority)
            .collect::<Vec<_>>();
        let projection = project_v22_encoded_cell_card_group(
            &pages,
            &authority,
            DIMENSIONS,
            VectorElementType::Float32,
            "v22-stage-l/layout",
        )
        .unwrap();
        let encoded = &projection.encoded;
        let expected_path = encoded
            .content_addressed_path("v22-stage-l/layout")
            .unwrap();
        let (group, _) = encoded.references(&expected_path).unwrap();
        let projected = &projection.units;
        let packed = project_v22_encoded_layout(
            pages.iter().cloned().zip(authority.iter().cloned()),
            DIMENSIONS,
            VectorElementType::Float32,
            2,
            "v22-stage-l/layout",
        )
        .unwrap();
        assert_eq!(packed, *projected);

        assert_eq!(projected.len(), 5);
        assert_eq!(
            projected
                .iter()
                .map(|unit| unit.record_ids.len())
                .collect::<Vec<_>>(),
            [1, 32, 32, 32, 1]
        );
        let expected_blocks = encoded
            .cards
            .iter()
            .flat_map(|card| card.head.exact_blocks.iter())
            .collect::<Vec<_>>();
        for (unit, block) in projected.iter().zip(expected_blocks) {
            assert_eq!(unit.object_checksum, group.checksum);
            assert_eq!(unit.object_encoded_bytes, encoded.bytes.len() as u64);
            assert_eq!(unit.offset, block.offset);
            assert_eq!(unit.encoded_bytes, block.bytes);
            assert_eq!(
                unit.decoded_bytes,
                u64::from(block.rows) * EXACT_ROW_BYTES as u64
            );
            assert!(unit.offset + u64::from(unit.encoded_bytes) <= unit.object_encoded_bytes);
        }
        assert_eq!(projected[0].path, expected_path);
        assert_eq!(
            projected[1].encoded_bytes, projected[2].encoded_bytes,
            "the current uncompressed encoder must not claim a compressibility-dependent size"
        );
        let ranked = authority
            .iter()
            .flat_map(|card| card.iter().map(|record| record.record_id))
            .collect::<Vec<_>>();
        let census = v22_census_layout_prefix(
            projected,
            &ranked,
            EXACT_ROW_BYTES as u64,
            encoded.bytes.len() as u64,
            projected.len(),
            5,
        )
        .unwrap();
        assert!(census.eligible);
        assert_eq!(census.selected_rows, 98);
        assert_eq!(census.projected_objects.len(), 1);
        assert_eq!(census.projected_objects[0].checksum, group.checksum);
        assert_eq!(
            census.projected_objects[0].encoded_bytes,
            encoded.bytes.len() as u64
        );

        let mut mismatched_authority = authority.clone();
        mismatched_authority[0][0].canonical_record_id = b"wrong-record".to_vec().into();
        assert!(
            project_v22_encoded_cell_card_group(
                &pages,
                &mismatched_authority,
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/layout",
            )
            .is_err()
        );
        let mut duplicate_numeric_authority = authority.clone();
        duplicate_numeric_authority[1][0].record_id = duplicate_numeric_authority[0][0].record_id;
        assert!(
            project_v22_encoded_cell_card_group(
                &pages,
                &duplicate_numeric_authority,
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/layout",
            )
            .is_err()
        );
        let mut duplicate_canonical_pages = pages.clone();
        let mut duplicate_canonical_authority = authority.clone();
        let duplicate_canonical = authority[0][0].canonical_record_id.clone();
        duplicate_canonical_pages[1].rows[0].id =
            RecordId::from_bytes(duplicate_canonical.to_vec());
        duplicate_canonical_authority[1][0].canonical_record_id = duplicate_canonical;
        assert!(
            project_v22_encoded_cell_card_group(
                &duplicate_canonical_pages,
                &duplicate_canonical_authority,
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/layout",
            )
            .is_err()
        );
        let mut wrong_width_pages = pages.clone();
        wrong_width_pages[0].rows[0].exact.pop();
        assert!(
            project_v22_encoded_cell_card_group(
                &wrong_width_pages,
                &authority,
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/layout",
            )
            .is_err()
        );

        const WIDE_DIMENSIONS: usize = 1536;
        const WIDE_ROW_BYTES: usize = WIDE_DIMENSIONS * std::mem::size_of::<f32>();
        let wide_records = (0_u64..17)
            .map(|record_id| V22EncodedRecordAuthority {
                canonical_record_id: format!("wide-{record_id:04}").into_bytes().into(),
                record_id,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let wide_page = GlobalLeafPageInput {
            cell_index: 9,
            leaf_ordinal: 4,
            centroid_code: vec![9, 4],
            rows: wide_records
                .iter()
                .map(|record| GlobalLeafRowInput {
                    id: RecordId::from_bytes(record.canonical_record_id.to_vec()),
                    stamp: MutationStamp::new(
                        MutationVersion::from_parts(record.record_id + 1, [9; 16]),
                        [record.record_id as u8; 32],
                    ),
                    code: GlobalLeafCodeInput::from(vec![9, record.record_id as u8]),
                    exact: vec![record.record_id as u8; WIDE_ROW_BYTES],
                })
                .collect(),
        };
        let wide_projection = project_v22_encoded_cell_card_group(
            &[wide_page],
            &[wide_records],
            WIDE_DIMENSIONS,
            VectorElementType::Float32,
            "v22-stage-l/wide",
        )
        .unwrap();
        assert_eq!(
            wide_projection
                .units
                .iter()
                .map(|unit| unit.record_ids.len())
                .collect::<Vec<_>>(),
            [16, 1],
            "the real encoder must apply its 96-KiB decoded payload cap below the 32-row clamp"
        );
    }

    #[test]
    fn v22_streaming_layout_rejects_duplicate_ids_across_group_boundaries() {
        for duplicate_canonical_id in [false, true] {
            let pages = (0_u32..=3012).map(|cell_index| {
                let source_record_id = u64::from(cell_index);
                let canonical_record_id = if duplicate_canonical_id && cell_index == 3012 {
                    0_u64.to_be_bytes()
                } else {
                    source_record_id.to_be_bytes()
                };
                (
                    GlobalLeafPageInput {
                        cell_index,
                        leaf_ordinal: 0,
                        centroid_code: vec![0, 0],
                        rows: vec![GlobalLeafRowInput {
                            id: RecordId::from_bytes(canonical_record_id.to_vec()),
                            stamp: MutationStamp::new(
                                MutationVersion::from_parts(source_record_id + 1, [0; 16]),
                                [cell_index as u8; 32],
                            ),
                            code: GlobalLeafCodeInput::from(vec![0, 0]),
                            exact: vec![cell_index as u8; 4],
                        }],
                    },
                    vec![V22EncodedRecordAuthority {
                        canonical_record_id: canonical_record_id.to_vec().into_boxed_slice(),
                        record_id: if !duplicate_canonical_id && cell_index == 3012 {
                            0
                        } else {
                            source_record_id
                        },
                    }]
                    .into_boxed_slice(),
                )
            });

            assert!(
                project_v22_encoded_layout(
                    pages,
                    4,
                    VectorElementType::Int8,
                    2,
                    "v22-stage-l/cross-group-duplicate",
                )
                .is_err(),
                "duplicate_canonical_id={duplicate_canonical_id}"
            );
        }
    }

    #[test]
    fn v22_layout_oracle_reuses_exact_wave_bounds_and_reports_purity() {
        let semantic = [V22ProjectedUnit {
            path: "semantic/group.arrow".to_string(),
            object_checksum: [1; 32],
            object_encoded_bytes: 5632,
            offset: 4096,
            encoded_bytes: 1536,
            decoded_bytes: 1536,
            record_ids: vec![10, 20, 30, 40].into(),
        }];
        let census =
            v22_census_layout_prefix(&semantic, &[10, 20, 30], 384, 1_048_576, 4, 2).unwrap();
        assert_eq!(census.useful_bytes, 1152);
        assert_eq!(census.selected_bytes, 1536);
        assert_eq!(census.physical_bytes, 1536);
        assert_eq!(census.requests, 1);
        assert_eq!(census.selected_rows, 4);
        assert_eq!(census.rows_per_range.as_ref(), &[4]);
        assert_eq!(census.blocks_per_range.as_ref(), &[1]);
        assert_eq!(census.packing_purity_ppm, 750_000);
        assert_eq!(census.speculative_bytes, 0);
        assert_eq!(census.projected_objects.len(), 1);
        assert_eq!(census.projected_objects[0].checksum, [1; 32]);
        assert_eq!(census.projected_objects[0].encoded_bytes, 5632);
        assert_eq!(census.physical_amplification_ppm, 1_000_000);
        assert_eq!(census.limiting_bound, V22LayoutLimitingBound::Eligible);
        assert!(census.eligible);

        let physical = [
            V22ProjectedUnit {
                path: "physical/a.arrow".to_string(),
                object_checksum: [2; 32],
                object_encoded_bytes: 512,
                offset: 0,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![10].into(),
            },
            V22ProjectedUnit {
                path: "physical/b.arrow".to_string(),
                object_checksum: [3; 32],
                object_encoded_bytes: 512,
                offset: 0,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![20].into(),
            },
            V22ProjectedUnit {
                path: "physical/c.arrow".to_string(),
                object_checksum: [4; 32],
                object_encoded_bytes: 512,
                offset: 0,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![30].into(),
            },
        ];
        let census =
            v22_census_layout_prefix(&physical, &[10, 20, 30], 384, 1_048_576, 4, 2).unwrap();
        assert_eq!(census.requests, 3);
        assert_eq!(census.rows_per_range.as_ref(), &[1, 1, 1]);
        let request_limited =
            v22_census_layout_prefix(&physical, &[10, 20, 30], 384, 1_048_576, 2, 5).unwrap();
        assert_eq!(request_limited.requests, 3);
        assert_eq!(
            request_limited.limiting_bound,
            V22LayoutLimitingBound::Requests
        );
        assert!(!request_limited.eligible);

        let coalesced = [
            V22ProjectedUnit {
                path: "coalesced.arrow".to_string(),
                object_checksum: [5; 32],
                object_encoded_bytes: 250,
                offset: 0,
                encoded_bytes: 100,
                decoded_bytes: 384,
                record_ids: vec![10].into(),
            },
            V22ProjectedUnit {
                path: "coalesced.arrow".to_string(),
                object_checksum: [5; 32],
                object_encoded_bytes: 250,
                offset: 100,
                encoded_bytes: 50,
                decoded_bytes: 384,
                record_ids: vec![99].into(),
            },
            V22ProjectedUnit {
                path: "coalesced.arrow".to_string(),
                object_checksum: [5; 32],
                object_encoded_bytes: 250,
                offset: 150,
                encoded_bytes: 100,
                decoded_bytes: 384,
                record_ids: vec![20].into(),
            },
        ];
        let census = v22_census_layout_prefix(&coalesced, &[10, 20], 384, 250, 1, 2).unwrap();
        assert_eq!(census.selected_bytes, 200);
        assert_eq!(census.physical_bytes, 250);
        assert_eq!(census.requests, 1);
        assert_eq!(census.rows_per_range.as_ref(), &[2]);
        assert_eq!(census.blocks_per_range.as_ref(), &[2]);
        assert_eq!(census.physical_amplification_ppm, 1_250_000);
        assert_eq!(census.packing_purity_ppm, 3_072_000);
        assert_eq!(census.speculative_bytes, 50);
        assert!(census.eligible);

        let tighter_bytes =
            v22_census_layout_prefix(&coalesced, &[10, 20], 384, 200, 2, 2).unwrap();
        assert_eq!(tighter_bytes.selected_bytes, 200);
        assert_eq!(tighter_bytes.physical_bytes, 200);
        assert_eq!(tighter_bytes.requests, 2);
        assert_eq!(
            tighter_bytes.limiting_bound,
            V22LayoutLimitingBound::Eligible
        );
        assert!(tighter_bytes.eligible);

        let request_limited =
            v22_census_layout_prefix(&coalesced, &[10, 20], 384, 250, 1, 1).unwrap();
        assert_eq!(request_limited.physical_bytes, 200);
        assert_eq!(request_limited.requests, 2);
        assert_eq!(
            request_limited.limiting_bound,
            V22LayoutLimitingBound::Amplification
        );
        assert!(!request_limited.eligible);
        let byte_limited = v22_census_layout_prefix(&coalesced, &[10, 20], 384, 199, 4, 2).unwrap();
        assert_eq!(byte_limited.selected_bytes, 200);
        assert_eq!(byte_limited.limiting_bound, V22LayoutLimitingBound::Bytes);
        assert!(!byte_limited.eligible);
    }

    #[test]
    fn v22_layout_oracle_rejects_malformed_projected_ranges() {
        let valid = V22ProjectedUnit {
            path: "group.arrow".to_string(),
            object_checksum: [6; 32],
            object_encoded_bytes: 1024,
            offset: 0,
            encoded_bytes: 512,
            decoded_bytes: 384,
            record_ids: vec![10].into(),
        };
        assert!(
            v22_census_layout_prefix(std::slice::from_ref(&valid), &[10], 384, 1_048_576, 4, 2)
                .is_ok()
        );
        assert!(v22_census_layout_prefix(&[], &[10], 384, 1_048_576, 4, 2).is_err());
        assert!(
            v22_census_layout_prefix(std::slice::from_ref(&valid), &[], 384, 1_048_576, 4, 2)
                .is_err()
        );
        assert!(
            v22_census_layout_prefix(std::slice::from_ref(&valid), &[11], 384, 1_048_576, 4, 2)
                .is_err()
        );
        assert!(
            v22_census_layout_prefix(
                std::slice::from_ref(&valid),
                &[10, 10],
                384,
                1_048_576,
                4,
                2,
            )
            .is_err()
        );
        let short_object = V22ProjectedUnit {
            object_encoded_bytes: 511,
            ..valid.clone()
        };
        assert!(v22_census_layout_prefix(&[short_object], &[10], 384, 1_048_576, 4, 2).is_err());
        let overlapping = [
            valid.clone(),
            V22ProjectedUnit {
                path: valid.path.clone(),
                object_checksum: valid.object_checksum,
                object_encoded_bytes: 1024,
                offset: 100,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![11].into(),
            },
        ];
        assert!(v22_census_layout_prefix(&overlapping, &[10, 11], 384, 1_048_576, 4, 2).is_err());
        let conflicting_checksum = [
            valid.clone(),
            V22ProjectedUnit {
                path: valid.path.clone(),
                object_checksum: [8; 32],
                object_encoded_bytes: 1024,
                offset: 512,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![11].into(),
            },
        ];
        assert!(
            v22_census_layout_prefix(&conflicting_checksum, &[10, 11], 384, 1_048_576, 4, 2)
                .is_err()
        );
        let conflicting_object_length = [
            valid.clone(),
            V22ProjectedUnit {
                path: valid.path.clone(),
                object_checksum: valid.object_checksum,
                object_encoded_bytes: 2048,
                offset: 512,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![11].into(),
            },
        ];
        assert!(
            v22_census_layout_prefix(&conflicting_object_length, &[10, 11], 384, 1_048_576, 4, 2)
                .is_err()
        );
        let duplicate_row = [
            valid.clone(),
            V22ProjectedUnit {
                path: "other.arrow".to_string(),
                object_checksum: [7; 32],
                object_encoded_bytes: 512,
                offset: 0,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![10].into(),
            },
        ];
        assert!(v22_census_layout_prefix(&duplicate_row, &[10], 384, 1_048_576, 4, 2).is_err());
        let empty_path = V22ProjectedUnit {
            path: String::new(),
            ..valid.clone()
        };
        assert!(v22_census_layout_prefix(&[empty_path], &[10], 384, 1_048_576, 4, 2).is_err());
        let wrong_decoded_size = V22ProjectedUnit {
            path: "group.arrow".to_string(),
            object_checksum: [6; 32],
            object_encoded_bytes: 64,
            offset: 0,
            encoded_bytes: 64,
            decoded_bytes: 383,
            record_ids: vec![10].into(),
        };
        assert!(
            v22_census_layout_prefix(&[wrong_decoded_size], &[10], 384, 1_048_576, 4, 2).is_err()
        );
        assert!(v22_census_layout_prefix(&[valid], &[10], 384, 1_048_576, 4, 0).is_err());
    }
}
