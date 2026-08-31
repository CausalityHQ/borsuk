use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use half::f16;
use serde::{Deserialize, Serialize};

use crate::{
    BorsukError, Result,
    v23_diagnostic::V23DecodedPage,
    v23_incidence_tree::{
        V23IncidenceTree, assign_one_leaf, assign_one_leaf_normalized, assign_two_beam_leaves,
        assign_two_beam_leaves_normalized, normalize_incidence_row,
    },
};

const LEAF_COUNT: usize = 65_536;
pub(crate) const V23_POSTING_PARTITIONS: usize = 256;
pub(crate) const V23_POSTING_RUN_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const V23_POSTING_MAX_PAGES: usize = 2048;
pub(crate) const V23_POSTING_ONE_ARM_RECORDS: u64 = 18_620_111;
pub(crate) const V23_POSTING_TWO_ARM_RECORDS: u64 = 37_240_222;
pub(crate) const V23_POSTING_COMBINED_RECORDS: u64 = 55_860_333;
const POSTING_RECORD_BYTES: u64 = 8;
const MAX_RUN_RECORDS: usize = V23_POSTING_RUN_BYTES as usize / POSTING_RECORD_BYTES as usize;
const SCRATCH_CEILING_BYTES: u64 = 1_027_983_056;
const PREFIX_CAPS: [usize; 3] = [512, 1024, 2048];
const RETAINED_MASS_MINIMUM_PPM: u64 = 995_000;
const QUANTIZATION_TV_MAXIMUM_PPM: u64 = 5_000;
const PLANE_MAGIC: &[u8; 8] = b"BVIP\x02\0\0\0";
const PLANE_HEADER_BYTES: u64 = 60;
const PLANE_OFFSET_BYTES: u64 = (LEAF_COUNT as u64 + 1) * 4;
const PLANE_LEAF_EVIDENCE_BYTES: u64 = LEAF_COUNT as u64 * 80;
const PLANE_ENTRIES_OFFSET: u64 =
    PLANE_HEADER_BYTES + PLANE_OFFSET_BYTES + PLANE_LEAF_EVIDENCE_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum PostingAssignmentArm {
    OneLeaf,
    TwoBeamLeaves,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct V23PostingRecord {
    pub(crate) leaf: u16,
    pub(crate) page: u32,
    pub(crate) reserved: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V23PostingArmRecords {
    pub(crate) one: V23PostingRecord,
    pub(crate) two: [V23PostingRecord; 2],
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct V23PostingPrefixEvidence {
    pub(crate) retained_assignments: u64,
    pub(crate) retained_mass_ppm: u32,
    pub(crate) quantization_error_numerator: u64,
    pub(crate) quantization_tv_ppm: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23PostingLeaf {
    pub(crate) pages: Vec<u32>,
    pub(crate) masses: Vec<u16>,
    pub(crate) total_mass: u64,
    pub(crate) prefixes: [V23PostingPrefixEvidence; 3],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23PostingPlane {
    pub(crate) arm: PostingAssignmentArm,
    pub(crate) max_pages_per_leaf: u16,
    pub(crate) partition_count: u16,
    pub(crate) source_records: u64,
    pub(crate) maximum_resident_records: u64,
    pub(crate) maximum_merge_entries: u32,
    pub(crate) scratch_bytes_peak: u64,
    pub(crate) leaves: Vec<V23PostingLeaf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23PostingArtifact {
    pub(crate) arm: PostingAssignmentArm,
    pub(crate) max_pages_per_leaf: u16,
    pub(crate) path: PathBuf,
    pub(crate) digest: String,
    pub(crate) encoded_bytes: u64,
    pub(crate) source_records: u64,
    pub(crate) maximum_resident_records: u64,
    pub(crate) maximum_merge_entries: u32,
    pub(crate) scratch_bytes_peak: u64,
}

impl V23PostingPlane {
    #[cfg(test)]
    fn entries_as_map(&self) -> std::collections::BTreeMap<(u16, u32), u16> {
        let mut entries = std::collections::BTreeMap::new();
        for (leaf, postings) in self.leaves.iter().enumerate() {
            for (&page, &mass) in postings.pages.iter().zip(&postings.masses) {
                entries.insert((leaf as u16, page), mass);
            }
        }
        entries
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn io_error(path: &Path, source: std::io::Error) -> BorsukError {
    BorsukError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn round_ratio_half_even(numerator: u128, denominator: u128) -> Result<u64> {
    if denominator == 0 {
        return Err(invalid("V23 posting ratio denominator is zero"));
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let rounded = quotient
        .checked_add(u128::from(
            remainder > denominator / 2
                || (denominator.is_multiple_of(2)
                    && remainder == denominator / 2
                    && !quotient.is_multiple_of(2)),
        ))
        .ok_or_else(|| invalid("V23 posting ratio overflows"))?;
    u64::try_from(rounded).map_err(|_| invalid("V23 posting ratio exceeds u64"))
}

fn normalized_mass(count: u64, total: u64) -> Result<u16> {
    let mass = round_ratio_half_even(u128::from(count) * 65_535, u128::from(total))?;
    u16::try_from(mass).map_err(|_| invalid("V23 posting normalized mass exceeds u16"))
}

fn prefix_evidence(
    pages: &[(u32, u16, u64)],
    total: u64,
    cap: usize,
) -> Result<V23PostingPrefixEvidence> {
    if total == 0 {
        return Ok(V23PostingPrefixEvidence {
            retained_assignments: 0,
            retained_mass_ppm: 0,
            quantization_error_numerator: 0,
            quantization_tv_ppm: 0,
        });
    }
    let retained = pages.iter().take(cap).try_fold(0_u64, |sum, entry| {
        sum.checked_add(entry.2)
            .ok_or_else(|| invalid("V23 posting retained assignments overflow"))
    })?;
    let error_numerator = pages.iter().take(cap).try_fold(0_u128, |sum, entry| {
        let exact = u128::from(entry.2) * 65_535;
        let quantized = u128::from(entry.1) * u128::from(total);
        sum.checked_add(exact.abs_diff(quantized))
            .ok_or_else(|| invalid("V23 posting quantization error overflows"))
    })?;
    let retained_mass_ppm =
        round_ratio_half_even(u128::from(retained) * 1_000_000, u128::from(total))?;
    let quantization_tv_ppm =
        round_ratio_half_even(error_numerator * 1_000_000, u128::from(total) * 65_535 * 2)?;
    Ok(V23PostingPrefixEvidence {
        retained_assignments: retained,
        retained_mass_ppm: u32::try_from(retained_mass_ppm)
            .map_err(|_| invalid("V23 posting retained mass ppm exceeds u32"))?,
        quantization_error_numerator: u64::try_from(error_numerator)
            .map_err(|_| invalid("V23 posting quantization error exceeds u64"))?,
        quantization_tv_ppm: u32::try_from(quantization_tv_ppm)
            .map_err(|_| invalid("V23 posting quantization TV ppm exceeds u32"))?,
    })
}

pub(crate) fn encode_posting_record(record: V23PostingRecord) -> Result<[u8; 8]> {
    if record.reserved != 0 {
        return Err(invalid("V23 posting record reserved bytes differ"));
    }
    let mut bytes = [0_u8; 8];
    bytes[..2].copy_from_slice(&record.leaf.to_le_bytes());
    bytes[2..6].copy_from_slice(&record.page.to_le_bytes());
    bytes[6..].copy_from_slice(&record.reserved.to_le_bytes());
    Ok(bytes)
}

pub(crate) fn decode_posting_record(bytes: &[u8]) -> Result<V23PostingRecord> {
    if bytes.len() != 8 {
        return Err(invalid("V23 posting record length differs"));
    }
    let record = V23PostingRecord {
        leaf: u16::from_le_bytes(bytes[..2].try_into().unwrap()),
        page: u32::from_le_bytes(bytes[2..6].try_into().unwrap()),
        reserved: u16::from_le_bytes(bytes[6..].try_into().unwrap()),
    };
    encode_posting_record(record)?;
    Ok(record)
}

fn canonical_source_ordinal(bytes: &[u8]) -> Result<u64> {
    let value = std::str::from_utf8(bytes)
        .ok()
        .filter(|value| {
            !value.is_empty()
                && value.bytes().all(|byte| byte.is_ascii_digit())
                && (*value == "0" || !value.starts_with('0'))
        })
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| value.to_string().as_bytes() == bytes)
        .ok_or_else(|| invalid("V23 posting source ordinal differs"))?;
    Ok(value)
}

fn decode_f16_row(code: &[u8]) -> Result<[f32; 96]> {
    if code.len() != 192 {
        return Err(invalid("V23 posting f16 row width differs"));
    }
    let mut vector = [0.0_f32; 96];
    for (output, bits) in vector.iter_mut().zip(code.as_chunks::<2>().0) {
        *output = f16::from_bits(u16::from_le_bytes([bits[0], bits[1]])).to_f32();
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V23 posting f16 row is non-finite"));
    }
    Ok(vector)
}

pub(crate) struct PagePostingRecords<'a> {
    tree: &'a V23IncidenceTree,
    page: &'a V23DecodedPage,
    arm: PostingAssignmentArm,
    row: usize,
    pending_leaf: Option<u16>,
}

impl Iterator for PagePostingRecords<'_> {
    type Item = Result<V23PostingRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(leaf) = self.pending_leaf.take() {
            return Some(Ok(V23PostingRecord {
                leaf,
                page: self.page.page_ordinal(),
                reserved: 0,
            }));
        }
        if self.row >= self.page.primary_rows() + self.page.replicated_rows() {
            return None;
        }
        let row = self.row;
        self.row += 1;
        let result = (|| {
            let source_ordinal = canonical_source_ordinal(
                self.page
                    .record_id(row)
                    .ok_or_else(|| invalid("V23 posting page record ID is absent"))?,
            )?;
            let vector = decode_f16_row(
                self.page
                    .code(row)
                    .ok_or_else(|| invalid("V23 posting page row is absent"))?,
            )?;
            let leaf = match self.arm {
                PostingAssignmentArm::OneLeaf => {
                    assign_one_leaf(self.tree, &vector, source_ordinal)?
                }
                PostingAssignmentArm::TwoBeamLeaves => {
                    let leaves = assign_two_beam_leaves(self.tree, &vector, source_ordinal)?.0;
                    self.pending_leaf = Some(leaves[1]);
                    leaves[0]
                }
            };
            Ok(V23PostingRecord {
                leaf,
                page: self.page.page_ordinal(),
                reserved: 0,
            })
        })();
        Some(result)
    }
}

pub(crate) fn page_posting_records<'a>(
    tree: &'a V23IncidenceTree,
    page: &'a V23DecodedPage,
    arm: PostingAssignmentArm,
) -> PagePostingRecords<'a> {
    PagePostingRecords {
        tree,
        page,
        arm,
        row: 0,
        pending_leaf: None,
    }
}

pub(crate) struct PagePostingRecordsBoth<'a> {
    tree: &'a V23IncidenceTree,
    page: &'a V23DecodedPage,
    row: usize,
}

impl Iterator for PagePostingRecordsBoth<'_> {
    type Item = Result<V23PostingArmRecords>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.page.primary_rows() + self.page.replicated_rows() {
            return None;
        }
        let row = self.row;
        self.row += 1;
        Some((|| {
            let source_ordinal = canonical_source_ordinal(
                self.page
                    .record_id(row)
                    .ok_or_else(|| invalid("V23 posting page record ID is absent"))?,
            )?;
            let vector = decode_f16_row(
                self.page
                    .code(row)
                    .ok_or_else(|| invalid("V23 posting page row is absent"))?,
            )?;
            let normalized = normalize_incidence_row(&vector)?;
            let one = assign_one_leaf_normalized(self.tree, &normalized, source_ordinal)?;
            let two = assign_two_beam_leaves_normalized(self.tree, &normalized, source_ordinal)?.0;
            let page = self.page.page_ordinal();
            Ok(V23PostingArmRecords {
                one: V23PostingRecord {
                    leaf: one,
                    page,
                    reserved: 0,
                },
                two: two.map(|leaf| V23PostingRecord {
                    leaf,
                    page,
                    reserved: 0,
                }),
            })
        })())
    }
}

pub(crate) fn page_posting_records_both<'a>(
    tree: &'a V23IncidenceTree,
    page: &'a V23DecodedPage,
) -> PagePostingRecordsBoth<'a> {
    PagePostingRecordsBoth { tree, page, row: 0 }
}

fn read_record(reader: &mut BufReader<File>) -> Result<Option<V23PostingRecord>> {
    let mut bytes = [0_u8; 8];
    let mut read = 0;
    while read < bytes.len() {
        match reader.read(&mut bytes[read..]) {
            Ok(0) if read == 0 => return Ok(None),
            Ok(0) => return Err(invalid("V23 posting run is truncated")),
            Ok(count) => read += count,
            Err(error) => return Err(io_error(Path::new("posting-run"), error)),
        }
    }
    decode_posting_record(&bytes).map(Some)
}

fn read_stream_u16(reader: &mut impl Read, path: &Path) -> Result<u16> {
    let mut bytes = [0_u8; 2];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| io_error(path, error))?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_stream_u32(reader: &mut impl Read, path: &Path) -> Result<u32> {
    let mut bytes = [0_u8; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| io_error(path, error))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_stream_u64(reader: &mut impl Read, path: &Path) -> Result<u64> {
    let mut bytes = [0_u8; 8];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| io_error(path, error))?;
    Ok(u64::from_le_bytes(bytes))
}

struct RunFiles {
    by_partition: Vec<Vec<PathBuf>>,
    paths: Vec<PathBuf>,
}

impl RunFiles {
    fn new() -> Self {
        Self {
            by_partition: vec![Vec::new(); V23_POSTING_PARTITIONS],
            paths: Vec::new(),
        }
    }

    fn cleanup(&mut self) -> Result<()> {
        while let Some(path) = self.paths.last().cloned() {
            fs::remove_file(&path).map_err(|error| io_error(&path, error))?;
            self.paths.pop();
        }
        Ok(())
    }
}

impl Drop for RunFiles {
    fn drop(&mut self) {
        while let Some(path) = self.paths.pop() {
            let _ = fs::remove_file(path);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RunEvidence {
    source_records: u64,
    maximum_resident_records: u64,
    scratch_bytes_peak: u64,
}

struct RunWriter<'a> {
    scratch: &'a Path,
    label: &'static str,
    run_records: usize,
    runs: RunFiles,
    chunk: Vec<V23PostingRecord>,
    evidence: RunEvidence,
    run_index: u64,
}

impl<'a> RunWriter<'a> {
    fn new(scratch: &'a Path, label: &'static str, run_records: usize) -> Self {
        Self {
            scratch,
            label,
            run_records,
            runs: RunFiles::new(),
            chunk: Vec::with_capacity(run_records),
            evidence: RunEvidence {
                source_records: 0,
                maximum_resident_records: 0,
                scratch_bytes_peak: 0,
            },
            run_index: 0,
        }
    }

    fn push(&mut self, record: V23PostingRecord) -> Result<()> {
        self.chunk.push(record);
        if self.chunk.len() == self.run_records {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        self.evidence.source_records = self
            .evidence
            .source_records
            .checked_add(self.chunk.len() as u64)
            .ok_or_else(|| invalid("V23 posting source record count overflows"))?;
        self.evidence.maximum_resident_records = self
            .evidence
            .maximum_resident_records
            .max(self.chunk.len() as u64);
        let mut partitions = vec![Vec::new(); V23_POSTING_PARTITIONS];
        for record in self.chunk.drain(..) {
            encode_posting_record(record)?;
            partitions[usize::from(record.leaf >> 8)].push(record);
        }
        for (partition, mut values) in partitions.into_iter().enumerate() {
            if values.is_empty() {
                continue;
            }
            values.sort_unstable_by_key(|record| (record.leaf, record.page));
            let path = self.scratch.join(format!(
                "{}-partition-{partition:03}-run-{:08}.bin",
                self.label, self.run_index
            ));
            let mut writer =
                BufWriter::new(File::create(&path).map_err(|error| io_error(&path, error))?);
            self.runs.paths.push(path.clone());
            for value in values {
                writer
                    .write_all(&encode_posting_record(value)?)
                    .map_err(|error| io_error(&path, error))?;
            }
            writer.flush().map_err(|error| io_error(&path, error))?;
            let bytes = writer
                .get_ref()
                .metadata()
                .map_err(|error| io_error(&path, error))?
                .len();
            self.evidence.scratch_bytes_peak = self
                .evidence
                .scratch_bytes_peak
                .checked_add(bytes)
                .ok_or_else(|| invalid("V23 posting scratch bytes overflow"))?;
            if self.evidence.scratch_bytes_peak > SCRATCH_CEILING_BYTES {
                return Err(invalid("V23 posting scratch ceiling exceeded"));
            }
            self.runs.by_partition[partition].push(path);
        }
        self.run_index += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<(RunFiles, RunEvidence)> {
        self.flush()?;
        if self.evidence.source_records == 0 {
            return Err(invalid("V23 posting source is empty"));
        }
        Ok((self.runs, self.evidence))
    }
}

fn write_runs<I>(records: I, scratch: &Path, run_records: usize) -> Result<(RunFiles, RunEvidence)>
where
    I: IntoIterator<Item = Result<V23PostingRecord>>,
{
    let mut writer = RunWriter::new(scratch, "single", run_records);
    for record in records {
        writer.push(record?)?;
    }
    writer.finish()
}

fn empty_posting_leaf() -> V23PostingLeaf {
    V23PostingLeaf {
        pages: Vec::new(),
        masses: Vec::new(),
        total_mass: 0,
        prefixes: [V23PostingPrefixEvidence::default(); 3],
    }
}

fn completed_posting_leaf(
    total_mass: u64,
    top: BinaryHeap<(Reverse<u64>, u32)>,
) -> Result<V23PostingLeaf> {
    let mut pages = top
        .into_iter()
        .map(|(Reverse(count), page)| Ok((page, normalized_mass(count, total_mass)?, count)))
        .collect::<Result<Vec<_>>>()?;
    pages.retain(|entry| entry.1 != 0);
    pages.sort_unstable_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));
    let prefixes = PREFIX_CAPS
        .map(|cap| prefix_evidence(&pages, total_mass, cap))
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .try_into()
        .map_err(|_| invalid("V23 posting prefix evidence count differs"))?;
    Ok(V23PostingLeaf {
        pages: pages.iter().map(|entry| entry.0).collect(),
        masses: pages.iter().map(|entry| entry.1).collect(),
        total_mass,
        prefixes,
    })
}

fn finish_posting_leaf(
    leaf: u16,
    total_mass: u64,
    top: BinaryHeap<(Reverse<u64>, u32)>,
    leaves: &mut [V23PostingLeaf],
) -> Result<()> {
    leaves[usize::from(leaf)] = completed_posting_leaf(total_mass, top)?;
    Ok(())
}

fn retain_top_posting(
    top: &mut BinaryHeap<(Reverse<u64>, u32)>,
    max_pages_per_leaf: usize,
    page: u32,
    count: u64,
) {
    let candidate = (Reverse(count), page);
    if top.len() < max_pages_per_leaf {
        top.push(candidate);
        return;
    }
    let worst = top.peek().copied().unwrap();
    if count > worst.0.0 || (count == worst.0.0 && page < worst.1) {
        top.pop();
        top.push(candidate);
    }
}

fn merge_partition(
    paths: &[PathBuf],
    max_pages_per_leaf: usize,
    leaves: &mut [V23PostingLeaf],
) -> Result<usize> {
    let mut readers = paths
        .iter()
        .map(|path| File::open(path).map_err(|error| io_error(path, error)))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(BufReader::new)
        .collect::<Vec<_>>();
    let mut records = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_record(reader)? {
            records.push(Reverse((record.leaf, record.page, index, record)));
        }
    }
    let mut active_leaf = None;
    let mut active_pair = None;
    let mut pair_count = 0_u64;
    let mut total_mass = 0_u64;
    let mut top = BinaryHeap::new();
    let mut maximum_merge_entries = 0;
    while let Some(Reverse((leaf, page, index, _))) = records.pop() {
        if active_pair != Some((leaf, page)) {
            if let Some((prior_leaf, prior_page)) = active_pair {
                if active_leaf != Some(prior_leaf) {
                    if let Some(completed_leaf) = active_leaf {
                        finish_posting_leaf(completed_leaf, total_mass, top, leaves)?;
                        top = BinaryHeap::new();
                        total_mass = 0;
                    }
                    active_leaf = Some(prior_leaf);
                }
                total_mass = total_mass
                    .checked_add(pair_count)
                    .ok_or_else(|| invalid("V23 posting total mass overflows"))?;
                retain_top_posting(&mut top, max_pages_per_leaf, prior_page, pair_count);
                maximum_merge_entries = maximum_merge_entries.max(top.len());
            }
            active_pair = Some((leaf, page));
            pair_count = 0;
        }
        pair_count = pair_count
            .checked_add(1)
            .ok_or_else(|| invalid("V23 posting mass overflows"))?;
        if let Some(record) = read_record(&mut readers[index])? {
            records.push(Reverse((record.leaf, record.page, index, record)));
        }
    }
    if let Some((leaf, page)) = active_pair {
        if active_leaf != Some(leaf) {
            if let Some(completed_leaf) = active_leaf {
                finish_posting_leaf(completed_leaf, total_mass, top, leaves)?;
                top = BinaryHeap::new();
                total_mass = 0;
            }
            active_leaf = Some(leaf);
        }
        total_mass = total_mass
            .checked_add(pair_count)
            .ok_or_else(|| invalid("V23 posting total mass overflows"))?;
        retain_top_posting(&mut top, max_pages_per_leaf, page, pair_count);
        maximum_merge_entries = maximum_merge_entries.max(top.len());
    }
    if let Some(leaf) = active_leaf {
        finish_posting_leaf(leaf, total_mass, top, leaves)?;
    }
    Ok(maximum_merge_entries)
}

fn merge_partition_stream(
    paths: &[PathBuf],
    max_pages_per_leaf: usize,
    emit: &mut impl FnMut(u16, V23PostingLeaf) -> Result<()>,
) -> Result<usize> {
    let mut readers = paths
        .iter()
        .map(|path| File::open(path).map_err(|error| io_error(path, error)))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(BufReader::new)
        .collect::<Vec<_>>();
    let mut records = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_record(reader)? {
            records.push(Reverse((record.leaf, record.page, index, record)));
        }
    }
    let mut active_leaf = None;
    let mut active_pair = None;
    let mut pair_count = 0_u64;
    let mut total_mass = 0_u64;
    let mut top = BinaryHeap::new();
    let mut maximum_merge_entries = 0;
    while let Some(Reverse((leaf, page, index, _))) = records.pop() {
        if active_pair != Some((leaf, page)) {
            if let Some((prior_leaf, prior_page)) = active_pair {
                if active_leaf != Some(prior_leaf) {
                    if let Some(completed_leaf) = active_leaf {
                        emit(completed_leaf, completed_posting_leaf(total_mass, top)?)?;
                        top = BinaryHeap::new();
                        total_mass = 0;
                    }
                    active_leaf = Some(prior_leaf);
                }
                total_mass = total_mass
                    .checked_add(pair_count)
                    .ok_or_else(|| invalid("V23 posting total mass overflows"))?;
                retain_top_posting(&mut top, max_pages_per_leaf, prior_page, pair_count);
                maximum_merge_entries = maximum_merge_entries.max(top.len());
            }
            active_pair = Some((leaf, page));
            pair_count = 0;
        }
        pair_count = pair_count
            .checked_add(1)
            .ok_or_else(|| invalid("V23 posting mass overflows"))?;
        if let Some(record) = read_record(&mut readers[index])? {
            records.push(Reverse((record.leaf, record.page, index, record)));
        }
    }
    if let Some((leaf, page)) = active_pair {
        if active_leaf != Some(leaf) {
            if let Some(completed_leaf) = active_leaf {
                emit(completed_leaf, completed_posting_leaf(total_mass, top)?)?;
                top = BinaryHeap::new();
                total_mass = 0;
            }
            active_leaf = Some(leaf);
        }
        total_mass = total_mass
            .checked_add(pair_count)
            .ok_or_else(|| invalid("V23 posting total mass overflows"))?;
        retain_top_posting(&mut top, max_pages_per_leaf, page, pair_count);
        maximum_merge_entries = maximum_merge_entries.max(top.len());
    }
    if let Some(leaf) = active_leaf {
        emit(leaf, completed_posting_leaf(total_mass, top)?)?;
    }
    Ok(maximum_merge_entries)
}

fn validate_posting_build_boundary(
    scratch: &Path,
    run_records: usize,
    max_pages_per_leaf: usize,
) -> Result<()> {
    if run_records == 0
        || run_records > MAX_RUN_RECORDS
        || !(1..=V23_POSTING_MAX_PAGES).contains(&max_pages_per_leaf)
        || !scratch.is_dir()
        || scratch
            .read_dir()
            .map_err(|error| io_error(scratch, error))?
            .next()
            .is_some()
    {
        return Err(invalid("V23 posting build boundary differs"));
    }
    Ok(())
}

fn merge_posting_runs(
    runs: &RunFiles,
    evidence: RunEvidence,
    arm: PostingAssignmentArm,
    max_pages_per_leaf: usize,
) -> Result<V23PostingPlane> {
    let mut leaves = vec![empty_posting_leaf(); LEAF_COUNT];
    let mut maximum_merge_entries = 0_usize;
    for paths in &runs.by_partition {
        maximum_merge_entries =
            maximum_merge_entries.max(merge_partition(paths, max_pages_per_leaf, &mut leaves)?);
    }
    Ok(V23PostingPlane {
        arm,
        max_pages_per_leaf: max_pages_per_leaf as u16,
        partition_count: V23_POSTING_PARTITIONS as u16,
        source_records: evidence.source_records,
        maximum_resident_records: evidence.maximum_resident_records,
        maximum_merge_entries: u32::try_from(maximum_merge_entries)
            .map_err(|_| invalid("V23 posting merge entries exceed u32"))?,
        scratch_bytes_peak: evidence.scratch_bytes_peak,
        leaves,
    })
}

pub(crate) fn build_posting_plane<I>(
    records: I,
    arm: PostingAssignmentArm,
    scratch: &Path,
    run_records: usize,
    max_pages_per_leaf: usize,
) -> Result<V23PostingPlane>
where
    I: IntoIterator<Item = Result<V23PostingRecord>>,
{
    validate_posting_build_boundary(scratch, run_records, max_pages_per_leaf)?;
    let (mut runs, evidence) = write_runs(records, scratch, run_records)?;
    let result = merge_posting_runs(&runs, evidence, arm, max_pages_per_leaf);
    let cleanup = runs.cleanup();
    match (result, cleanup) {
        (Ok(plane), Ok(())) => Ok(plane),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

#[cfg(test)]
pub(crate) fn build_both_posting_planes<I>(
    records: I,
    scratch: &Path,
    run_records: usize,
    max_pages_per_leaf: usize,
) -> Result<(V23PostingPlane, V23PostingPlane)>
where
    I: IntoIterator<Item = Result<V23PostingArmRecords>>,
{
    validate_posting_build_boundary(scratch, run_records, max_pages_per_leaf)?;
    let per_arm_run_records = run_records / 2;
    if per_arm_run_records == 0 {
        return Err(invalid("V23 posting combined run size differs"));
    }
    let mut one_writer = RunWriter::new(scratch, "one", per_arm_run_records);
    let mut two_writer = RunWriter::new(scratch, "two", per_arm_run_records);
    for records in records {
        let records = records?;
        one_writer.push(records.one)?;
        for record in records.two {
            two_writer.push(record)?;
        }
        let scratch_bytes = one_writer
            .evidence
            .scratch_bytes_peak
            .checked_add(two_writer.evidence.scratch_bytes_peak)
            .ok_or_else(|| invalid("V23 posting combined scratch bytes overflow"))?;
        if scratch_bytes > SCRATCH_CEILING_BYTES {
            return Err(invalid("V23 posting combined scratch ceiling exceeded"));
        }
    }
    let (mut one_runs, one_evidence) = one_writer.finish()?;
    let (mut two_runs, two_evidence) = two_writer.finish()?;
    if two_evidence.source_records
        != one_evidence
            .source_records
            .checked_mul(2)
            .ok_or_else(|| invalid("V23 posting combined source count overflows"))?
        || one_evidence
            .scratch_bytes_peak
            .checked_add(two_evidence.scratch_bytes_peak)
            .ok_or_else(|| invalid("V23 posting combined scratch bytes overflow"))?
            > SCRATCH_CEILING_BYTES
        || one_evidence
            .maximum_resident_records
            .checked_add(two_evidence.maximum_resident_records)
            .ok_or_else(|| invalid("V23 posting combined resident records overflow"))?
            > run_records as u64
    {
        return Err(invalid("V23 posting combined evidence differs"));
    }
    let result = (|| {
        Ok((
            merge_posting_runs(
                &one_runs,
                one_evidence,
                PostingAssignmentArm::OneLeaf,
                max_pages_per_leaf,
            )?,
            merge_posting_runs(
                &two_runs,
                two_evidence,
                PostingAssignmentArm::TwoBeamLeaves,
                max_pages_per_leaf,
            )?,
        ))
    })();
    let one_cleanup = one_runs.cleanup();
    let two_cleanup = two_runs.cleanup();
    match (result, one_cleanup, two_cleanup) {
        (Ok(planes), Ok(()), Ok(())) => Ok(planes),
        (Err(error), _, _) | (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
    }
}

fn write_posting_header(
    writer: &mut (impl Write + Seek),
    arm: PostingAssignmentArm,
    max_pages_per_leaf: usize,
    entry_count: u64,
    evidence: RunEvidence,
    maximum_merge_entries: usize,
) -> Result<()> {
    writer
        .seek(SeekFrom::Start(0))
        .map_err(|error| io_error(Path::new("posting-artifact"), error))?;
    writer
        .write_all(PLANE_MAGIC)
        .and_then(|_| {
            writer.write_all(
                &(match arm {
                    PostingAssignmentArm::OneLeaf => 1_u32,
                    PostingAssignmentArm::TwoBeamLeaves => 2_u32,
                })
                .to_le_bytes(),
            )
        })
        .and_then(|_| writer.write_all(&(LEAF_COUNT as u32).to_le_bytes()))
        .and_then(|_| writer.write_all(&(max_pages_per_leaf as u32).to_le_bytes()))
        .and_then(|_| writer.write_all(&(V23_POSTING_PARTITIONS as u32).to_le_bytes()))
        .and_then(|_| writer.write_all(&entry_count.to_le_bytes()))
        .and_then(|_| writer.write_all(&evidence.source_records.to_le_bytes()))
        .and_then(|_| writer.write_all(&evidence.maximum_resident_records.to_le_bytes()))
        .and_then(|_| writer.write_all(&(maximum_merge_entries as u32).to_le_bytes()))
        .and_then(|_| writer.write_all(&evidence.scratch_bytes_peak.to_le_bytes()))
        .map_err(|error| io_error(Path::new("posting-artifact"), error))
}

fn stream_posting_artifact(
    runs: &RunFiles,
    evidence: RunEvidence,
    arm: PostingAssignmentArm,
    max_pages_per_leaf: usize,
    output_directory: &Path,
) -> Result<V23PostingArtifact> {
    let label = match arm {
        PostingAssignmentArm::OneLeaf => "incidence-postings-one",
        PostingAssignmentArm::TwoBeamLeaves => "incidence-postings-two",
    };
    let temporary = output_directory.join(format!(".{label}-v2.tmp"));
    if temporary.exists() {
        return Err(invalid("V23 posting temporary output already exists"));
    }
    let mut renamed_path = None;
    let result = (|| {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| io_error(&temporary, error))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        writer
            .seek(SeekFrom::Start(PLANE_ENTRIES_OFFSET))
            .map_err(|error| io_error(&temporary, error))?;
        let mut counts = vec![0_u32; LEAF_COUNT];
        let mut totals = vec![0_u64; LEAF_COUNT];
        let mut prefixes = vec![[V23PostingPrefixEvidence::default(); 3]; LEAF_COUNT];
        let mut entry_count = 0_u64;
        let mut last_leaf = None;
        let mut maximum_merge_entries = 0_usize;
        for paths in &runs.by_partition {
            maximum_merge_entries = maximum_merge_entries.max(merge_partition_stream(
                paths,
                max_pages_per_leaf,
                &mut |leaf, posting| {
                    if last_leaf.is_some_and(|prior| leaf <= prior) {
                        return Err(invalid("V23 posting streamed leaf order differs"));
                    }
                    last_leaf = Some(leaf);
                    let index = usize::from(leaf);
                    counts[index] = u32::try_from(posting.pages.len())
                        .map_err(|_| invalid("V23 posting streamed leaf count overflows"))?;
                    totals[index] = posting.total_mass;
                    prefixes[index] = posting.prefixes;
                    for (&page, &mass) in posting.pages.iter().zip(&posting.masses) {
                        writer
                            .write_all(&page.to_le_bytes())
                            .and_then(|_| writer.write_all(&mass.to_le_bytes()))
                            .map_err(|error| io_error(&temporary, error))?;
                    }
                    entry_count = entry_count
                        .checked_add(posting.pages.len() as u64)
                        .ok_or_else(|| invalid("V23 posting streamed entry count overflows"))?;
                    Ok(())
                },
            )?);
        }
        if maximum_merge_entries == 0 {
            return Err(invalid("V23 posting streamed merge is empty"));
        }
        let body_bytes = PLANE_ENTRIES_OFFSET
            .checked_add(
                entry_count
                    .checked_mul(6)
                    .ok_or_else(|| invalid("V23 posting streamed length overflows"))?,
            )
            .ok_or_else(|| invalid("V23 posting streamed length overflows"))?;
        if writer
            .stream_position()
            .map_err(|error| io_error(&temporary, error))?
            != body_bytes
        {
            return Err(invalid("V23 posting streamed body length differs"));
        }
        write_posting_header(
            &mut writer,
            arm,
            max_pages_per_leaf,
            entry_count,
            evidence,
            maximum_merge_entries,
        )?;
        let mut offset = 0_u32;
        writer
            .write_all(&offset.to_le_bytes())
            .map_err(|error| io_error(&temporary, error))?;
        for count in counts {
            offset = offset
                .checked_add(count)
                .ok_or_else(|| invalid("V23 posting streamed offset overflows"))?;
            writer
                .write_all(&offset.to_le_bytes())
                .map_err(|error| io_error(&temporary, error))?;
        }
        if u64::from(offset) != entry_count {
            return Err(invalid("V23 posting streamed offsets differ"));
        }
        for (total, leaf_prefixes) in totals.into_iter().zip(prefixes) {
            writer
                .write_all(&total.to_le_bytes())
                .map_err(|error| io_error(&temporary, error))?;
            for prefix in leaf_prefixes {
                writer
                    .write_all(&prefix.retained_assignments.to_le_bytes())
                    .and_then(|_| writer.write_all(&prefix.retained_mass_ppm.to_le_bytes()))
                    .and_then(|_| {
                        writer.write_all(&prefix.quantization_error_numerator.to_le_bytes())
                    })
                    .and_then(|_| writer.write_all(&prefix.quantization_tv_ppm.to_le_bytes()))
                    .map_err(|error| io_error(&temporary, error))?;
            }
        }
        if writer
            .stream_position()
            .map_err(|error| io_error(&temporary, error))?
            != PLANE_ENTRIES_OFFSET
        {
            return Err(invalid("V23 posting streamed metadata length differs"));
        }
        writer
            .flush()
            .map_err(|error| io_error(&temporary, error))?;
        let mut file = writer
            .into_inner()
            .map_err(|error| io_error(&temporary, error.into_error()))?;
        file.sync_all()
            .map_err(|error| io_error(&temporary, error))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|error| io_error(&temporary, error))?;
        let mut hasher = blake3::Hasher::new();
        let mut remaining = body_bytes;
        let mut buffer = vec![0_u8; 1024 * 1024];
        while remaining != 0 {
            let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
            file.read_exact(&mut buffer[..wanted])
                .map_err(|error| io_error(&temporary, error))?;
            hasher.update(&buffer[..wanted]);
            remaining -= wanted as u64;
        }
        let internal_digest = hasher.finalize();
        file.seek(SeekFrom::End(0))
            .map_err(|error| io_error(&temporary, error))?;
        file.write_all(internal_digest.as_bytes())
            .map_err(|error| io_error(&temporary, error))?;
        file.sync_all()
            .map_err(|error| io_error(&temporary, error))?;
        hasher.update(internal_digest.as_bytes());
        let digest = hasher.finalize().to_hex().to_string();
        let final_path = output_directory.join(format!("{label}-{digest}.bin"));
        if final_path.exists() {
            return Err(invalid(
                "V23 posting content-addressed output already exists",
            ));
        }
        fs::rename(&temporary, &final_path).map_err(|error| io_error(&final_path, error))?;
        renamed_path = Some(final_path.clone());
        File::open(output_directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error(output_directory, error))?;
        let artifact = V23PostingArtifact {
            arm,
            max_pages_per_leaf: max_pages_per_leaf as u16,
            path: final_path,
            digest,
            encoded_bytes: body_bytes + 32,
            source_records: evidence.source_records,
            maximum_resident_records: evidence.maximum_resident_records,
            maximum_merge_entries: maximum_merge_entries as u32,
            scratch_bytes_peak: evidence.scratch_bytes_peak,
        };
        if let Err(error) = validate_posting_artifact(&artifact) {
            let _ = fs::remove_file(&artifact.path);
            return Err(error);
        }
        Ok(artifact)
    })();
    if result.is_err() && temporary.exists() {
        let _ = fs::remove_file(&temporary);
    }
    if result.is_err()
        && let Some(path) = renamed_path
    {
        let _ = fs::remove_file(path);
    }
    result
}

pub(crate) fn validate_posting_artifact(artifact: &V23PostingArtifact) -> Result<()> {
    let metadata = artifact
        .path
        .symlink_metadata()
        .map_err(|error| io_error(&artifact.path, error))?;
    let expected_scratch_bytes = artifact
        .source_records
        .checked_mul(POSTING_RECORD_BYTES)
        .ok_or_else(|| invalid("V23 posting artifact scratch bytes overflow"))?;
    if !metadata.file_type().is_file()
        || metadata.len() != artifact.encoded_bytes
        || artifact.encoded_bytes < PLANE_ENTRIES_OFFSET + 32
        || artifact.digest.len() != 64
        || !artifact
            .digest
            .bytes()
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value))
        || artifact.source_records == 0
        || artifact.maximum_resident_records == 0
        || artifact.maximum_resident_records > MAX_RUN_RECORDS as u64
        || artifact.maximum_merge_entries == 0
        || artifact.maximum_merge_entries > u32::from(artifact.max_pages_per_leaf)
        || !(1..=V23_POSTING_MAX_PAGES).contains(&usize::from(artifact.max_pages_per_leaf))
        || artifact.scratch_bytes_peak != expected_scratch_bytes
        || artifact.scratch_bytes_peak > SCRATCH_CEILING_BYTES
    {
        return Err(invalid("V23 posting artifact authority differs"));
    }
    let mut file = File::open(&artifact.path).map_err(|error| io_error(&artifact.path, error))?;
    let mut header = [0_u8; PLANE_HEADER_BYTES as usize];
    file.read_exact(&mut header)
        .map_err(|error| io_error(&artifact.path, error))?;
    let mut offset = 8;
    let arm = match read_u32(&header, &mut offset)? {
        1 => PostingAssignmentArm::OneLeaf,
        2 => PostingAssignmentArm::TwoBeamLeaves,
        _ => return Err(invalid("V23 posting artifact arm differs")),
    };
    let leaf_count = read_u32(&header, &mut offset)?;
    let max_pages_per_leaf = read_u32(&header, &mut offset)?;
    let partition_count = read_u32(&header, &mut offset)?;
    let entry_count = read_u64(&header, &mut offset)?;
    let source_records = read_u64(&header, &mut offset)?;
    let maximum_resident_records = read_u64(&header, &mut offset)?;
    let maximum_merge_entries = read_u32(&header, &mut offset)?;
    let scratch_bytes_peak = read_u64(&header, &mut offset)?;
    let expected_encoded_bytes = PLANE_ENTRIES_OFFSET
        .checked_add(
            entry_count
                .checked_mul(6)
                .ok_or_else(|| invalid("V23 posting artifact length overflows"))?,
        )
        .and_then(|value| value.checked_add(32))
        .ok_or_else(|| invalid("V23 posting artifact length overflows"))?;
    if &header[..8] != PLANE_MAGIC
        || arm != artifact.arm
        || leaf_count as usize != LEAF_COUNT
        || max_pages_per_leaf != u32::from(artifact.max_pages_per_leaf)
        || partition_count as usize != V23_POSTING_PARTITIONS
        || source_records != artifact.source_records
        || maximum_resident_records != artifact.maximum_resident_records
        || maximum_merge_entries != artifact.maximum_merge_entries
        || scratch_bytes_peak != artifact.scratch_bytes_peak
        || expected_encoded_bytes != artifact.encoded_bytes
    {
        return Err(invalid("V23 posting artifact header differs"));
    }
    let mut internal_hasher = blake3::Hasher::new();
    let mut object_hasher = blake3::Hasher::new();
    internal_hasher.update(&header);
    object_hasher.update(&header);
    let mut remaining = artifact.encoded_bytes - 32 - PLANE_HEADER_BYTES;
    let mut buffer = vec![0_u8; 1024 * 1024];
    while remaining != 0 {
        let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        file.read_exact(&mut buffer[..wanted])
            .map_err(|error| io_error(&artifact.path, error))?;
        internal_hasher.update(&buffer[..wanted]);
        object_hasher.update(&buffer[..wanted]);
        remaining -= wanted as u64;
    }
    let mut trailer = [0_u8; 32];
    file.read_exact(&mut trailer)
        .map_err(|error| io_error(&artifact.path, error))?;
    object_hasher.update(&trailer);
    if internal_hasher.finalize().as_bytes() != &trailer
        || object_hasher.finalize().to_hex().as_str() != artifact.digest
    {
        return Err(invalid("V23 posting artifact checksum differs"));
    }
    let label = match artifact.arm {
        PostingAssignmentArm::OneLeaf => "incidence-postings-one",
        PostingAssignmentArm::TwoBeamLeaves => "incidence-postings-two",
    };
    if artifact.path.file_name().and_then(|name| name.to_str())
        != Some(format!("{label}-{}.bin", artifact.digest).as_str())
    {
        return Err(invalid("V23 posting artifact path differs"));
    }
    let mut metadata_reader = BufReader::new(
        File::open(&artifact.path).map_err(|error| io_error(&artifact.path, error))?,
    );
    metadata_reader
        .seek(SeekFrom::Start(PLANE_HEADER_BYTES))
        .map_err(|error| io_error(&artifact.path, error))?;
    let mut offsets = Vec::with_capacity(LEAF_COUNT + 1);
    for _ in 0..=LEAF_COUNT {
        offsets.push(read_stream_u32(&mut metadata_reader, &artifact.path)? as usize);
    }
    if offsets[0] != 0
        || offsets[LEAF_COUNT] != entry_count as usize
        || offsets.windows(2).any(|pair| {
            pair[0] > pair[1] || pair[1] - pair[0] > usize::from(artifact.max_pages_per_leaf)
        })
    {
        return Err(invalid("V23 posting artifact offsets differ"));
    }
    let mut entry_reader = BufReader::new(
        File::open(&artifact.path).map_err(|error| io_error(&artifact.path, error))?,
    );
    entry_reader
        .seek(SeekFrom::Start(PLANE_ENTRIES_OFFSET))
        .map_err(|error| io_error(&artifact.path, error))?;
    let mut total_mass_sum = 0_u64;
    for leaf_index in 0..LEAF_COUNT {
        let total_mass = read_stream_u64(&mut metadata_reader, &artifact.path)?;
        total_mass_sum = total_mass_sum
            .checked_add(total_mass)
            .ok_or_else(|| invalid("V23 posting artifact total mass overflows"))?;
        let mut prefixes = [V23PostingPrefixEvidence::default(); 3];
        for prefix in &mut prefixes {
            *prefix = V23PostingPrefixEvidence {
                retained_assignments: read_stream_u64(&mut metadata_reader, &artifact.path)?,
                retained_mass_ppm: read_stream_u32(&mut metadata_reader, &artifact.path)?,
                quantization_error_numerator: read_stream_u64(
                    &mut metadata_reader,
                    &artifact.path,
                )?,
                quantization_tv_ppm: read_stream_u32(&mut metadata_reader, &artifact.path)?,
            };
        }
        let count = offsets[leaf_index + 1] - offsets[leaf_index];
        let mut pages = Vec::with_capacity(count);
        let mut masses = Vec::with_capacity(count);
        for _ in 0..count {
            pages.push(read_stream_u32(&mut entry_reader, &artifact.path)?);
            masses.push(read_stream_u16(&mut entry_reader, &artifact.path)?);
        }
        validate_posting_leaf_semantics(
            &V23PostingLeaf {
                pages,
                masses,
                total_mass,
                prefixes,
            },
            usize::from(artifact.max_pages_per_leaf),
        )?;
    }
    if total_mass_sum != artifact.source_records
        || metadata_reader
            .stream_position()
            .map_err(|error| io_error(&artifact.path, error))?
            != PLANE_ENTRIES_OFFSET
        || entry_reader
            .stream_position()
            .map_err(|error| io_error(&artifact.path, error))?
            != artifact.encoded_bytes - 32
    {
        return Err(invalid("V23 posting artifact streamed semantics differ"));
    }
    Ok(())
}

pub(crate) fn build_both_posting_plane_files<I>(
    records: I,
    scratch: &Path,
    output_directory: &Path,
    run_records: usize,
    max_pages_per_leaf: usize,
) -> Result<[V23PostingArtifact; 2]>
where
    I: IntoIterator<Item = Result<V23PostingArmRecords>>,
{
    validate_posting_build_boundary(scratch, run_records, max_pages_per_leaf)?;
    if output_directory == scratch || !output_directory.is_dir() {
        return Err(invalid("V23 posting streamed output boundary differs"));
    }
    let per_arm_run_records = run_records / 2;
    if per_arm_run_records == 0 {
        return Err(invalid("V23 posting combined run size differs"));
    }
    let mut one_writer = RunWriter::new(scratch, "one", per_arm_run_records);
    let mut two_writer = RunWriter::new(scratch, "two", per_arm_run_records);
    for records in records {
        let records = records?;
        one_writer.push(records.one)?;
        for record in records.two {
            two_writer.push(record)?;
        }
        if one_writer
            .evidence
            .scratch_bytes_peak
            .checked_add(two_writer.evidence.scratch_bytes_peak)
            .ok_or_else(|| invalid("V23 posting combined scratch bytes overflow"))?
            > SCRATCH_CEILING_BYTES
        {
            return Err(invalid("V23 posting combined scratch ceiling exceeded"));
        }
    }
    let (mut one_runs, one_evidence) = one_writer.finish()?;
    let (mut two_runs, two_evidence) = two_writer.finish()?;
    let evidence_valid = two_evidence.source_records
        == one_evidence
            .source_records
            .checked_mul(2)
            .ok_or_else(|| invalid("V23 posting combined source count overflows"))?
        && one_evidence
            .scratch_bytes_peak
            .checked_add(two_evidence.scratch_bytes_peak)
            .ok_or_else(|| invalid("V23 posting combined scratch bytes overflow"))?
            <= SCRATCH_CEILING_BYTES
        && one_evidence
            .maximum_resident_records
            .checked_add(two_evidence.maximum_resident_records)
            .ok_or_else(|| invalid("V23 posting combined resident records overflow"))?
            <= run_records as u64;
    let mut created = Vec::new();
    let result = if evidence_valid {
        (|| {
            let one = stream_posting_artifact(
                &one_runs,
                one_evidence,
                PostingAssignmentArm::OneLeaf,
                max_pages_per_leaf,
                output_directory,
            )?;
            created.push(one.path.clone());
            one_runs.cleanup()?;
            let two = stream_posting_artifact(
                &two_runs,
                two_evidence,
                PostingAssignmentArm::TwoBeamLeaves,
                max_pages_per_leaf,
                output_directory,
            )?;
            created.push(two.path.clone());
            two_runs.cleanup()?;
            Ok([one, two])
        })()
    } else {
        Err(invalid("V23 posting combined evidence differs"))
    };
    if result.is_err() {
        let _ = one_runs.cleanup();
        let _ = two_runs.cleanup();
        for path in created {
            let _ = fs::remove_file(path);
        }
    }
    result
}

pub(crate) fn posting_prefix_eligibility(plane: &V23PostingPlane, cap: usize) -> Result<bool> {
    let prefix_index = PREFIX_CAPS
        .iter()
        .position(|registered| *registered == cap)
        .ok_or_else(|| invalid("V23 posting prefix cap differs"))?;
    if usize::from(plane.max_pages_per_leaf) < cap
        || plane.leaves.len() != LEAF_COUNT
        || plane
            .leaves
            .iter()
            .any(|leaf| leaf.pages.len() != leaf.masses.len())
    {
        return Err(invalid("V23 posting prefix authority differs"));
    }
    let mut eligible = true;
    for leaf in plane.leaves.iter().filter(|leaf| leaf.total_mass != 0) {
        let evidence = &leaf.prefixes[prefix_index];
        let expected_retained_ppm = round_ratio_half_even(
            u128::from(evidence.retained_assignments) * 1_000_000,
            u128::from(leaf.total_mass),
        )?;
        if u64::from(evidence.retained_mass_ppm) != expected_retained_ppm {
            return Err(invalid("V23 posting prefix evidence differs"));
        }
        eligible &= u128::from(evidence.retained_assignments) * 1_000_000
            >= u128::from(leaf.total_mass) * u128::from(RETAINED_MASS_MINIMUM_PPM)
            && u128::from(evidence.quantization_error_numerator) * 1_000_000
                <= u128::from(leaf.total_mass)
                    * 65_535
                    * 2
                    * u128::from(QUANTIZATION_TV_MAXIMUM_PPM);
    }
    Ok(eligible)
}

pub(crate) fn validate_posting_prefix(plane: &V23PostingPlane, cap: usize) -> Result<()> {
    if !posting_prefix_eligibility(plane, cap)? {
        return Err(invalid("V23 posting prefix eligibility differs"));
    }
    Ok(())
}

pub(crate) fn validate_production_posting_plane(plane: &V23PostingPlane) -> Result<()> {
    validate_posting_plane_semantics(plane)?;
    let expected = match plane.arm {
        PostingAssignmentArm::OneLeaf => V23_POSTING_ONE_ARM_RECORDS,
        PostingAssignmentArm::TwoBeamLeaves => V23_POSTING_TWO_ARM_RECORDS,
    };
    if plane.source_records != expected
        || V23_POSTING_ONE_ARM_RECORDS + V23_POSTING_TWO_ARM_RECORDS != V23_POSTING_COMBINED_RECORDS
    {
        return Err(invalid("V23 production posting record count differs"));
    }
    Ok(())
}

fn validate_posting_plane_semantics(plane: &V23PostingPlane) -> Result<()> {
    let expected_scratch_bytes = plane
        .source_records
        .checked_mul(POSTING_RECORD_BYTES)
        .ok_or_else(|| invalid("V23 posting scratch bytes overflow"))?;
    if plane.leaves.len() != LEAF_COUNT
        || plane.partition_count as usize != V23_POSTING_PARTITIONS
        || !(1..=V23_POSTING_MAX_PAGES).contains(&usize::from(plane.max_pages_per_leaf))
        || plane.source_records == 0
        || plane.maximum_resident_records == 0
        || plane.maximum_resident_records > MAX_RUN_RECORDS as u64
        || plane.maximum_merge_entries == 0
        || plane.maximum_merge_entries > u32::from(plane.max_pages_per_leaf)
        || plane.scratch_bytes_peak != expected_scratch_bytes
        || plane.scratch_bytes_peak > SCRATCH_CEILING_BYTES
        || plane
            .leaves
            .iter()
            .try_fold(0_u64, |sum, leaf| sum.checked_add(leaf.total_mass))
            != Some(plane.source_records)
    {
        return Err(invalid("V23 posting plane shape differs"));
    }
    for leaf in &plane.leaves {
        validate_posting_leaf_semantics(leaf, usize::from(plane.max_pages_per_leaf))?;
    }
    Ok(())
}

fn validate_posting_leaf_semantics(leaf: &V23PostingLeaf, max_pages_per_leaf: usize) -> Result<()> {
    if leaf.pages.len() != leaf.masses.len()
        || leaf.pages.len() > max_pages_per_leaf
        || leaf.masses.contains(&0)
        || leaf.masses.windows(2).any(|pair| pair[0] < pair[1])
        || leaf
            .pages
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != leaf.pages.len()
        || (leaf.total_mass == 0 && !leaf.pages.is_empty())
    {
        return Err(invalid("V23 posting leaf shape differs"));
    }
    let mut prior_retained = 0;
    for evidence in &leaf.prefixes {
        if evidence.retained_assignments < prior_retained
            || evidence.retained_assignments > leaf.total_mass
            || (leaf.total_mass == 0
                && (evidence.retained_assignments != 0
                    || evidence.retained_mass_ppm != 0
                    || evidence.quantization_error_numerator != 0
                    || evidence.quantization_tv_ppm != 0))
            || (leaf.total_mass != 0
                && u64::from(evidence.retained_mass_ppm)
                    != round_ratio_half_even(
                        u128::from(evidence.retained_assignments) * 1_000_000,
                        u128::from(leaf.total_mass),
                    )?)
            || (leaf.total_mass != 0
                && u64::from(evidence.quantization_tv_ppm)
                    != round_ratio_half_even(
                        u128::from(evidence.quantization_error_numerator) * 1_000_000,
                        u128::from(leaf.total_mass) * 65_535 * 2,
                    )?)
            || evidence.quantization_tv_ppm > 500_000
        {
            return Err(invalid("V23 posting prefix evidence differs"));
        }
        prior_retained = evidence.retained_assignments;
    }
    Ok(())
}

pub(crate) fn encode_posting_plane(plane: &V23PostingPlane) -> Result<Vec<u8>> {
    validate_posting_plane_semantics(plane)?;
    let entry_count = plane
        .leaves
        .iter()
        .map(|leaf| leaf.pages.len())
        .sum::<usize>();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(PLANE_MAGIC);
    bytes.extend_from_slice(
        &(match plane.arm {
            PostingAssignmentArm::OneLeaf => 1_u32,
            PostingAssignmentArm::TwoBeamLeaves => 2_u32,
        })
        .to_le_bytes(),
    );
    bytes.extend_from_slice(&(LEAF_COUNT as u32).to_le_bytes());
    bytes.extend_from_slice(&u32::from(plane.max_pages_per_leaf).to_le_bytes());
    bytes.extend_from_slice(&u32::from(plane.partition_count).to_le_bytes());
    bytes.extend_from_slice(&(entry_count as u64).to_le_bytes());
    bytes.extend_from_slice(&plane.source_records.to_le_bytes());
    bytes.extend_from_slice(&plane.maximum_resident_records.to_le_bytes());
    bytes.extend_from_slice(&plane.maximum_merge_entries.to_le_bytes());
    bytes.extend_from_slice(&plane.scratch_bytes_peak.to_le_bytes());
    let mut offset = 0_u32;
    bytes.extend_from_slice(&offset.to_le_bytes());
    for leaf in &plane.leaves {
        offset = offset
            .checked_add(leaf.pages.len() as u32)
            .ok_or_else(|| invalid("V23 posting offset overflows"))?;
        bytes.extend_from_slice(&offset.to_le_bytes());
    }
    for leaf in &plane.leaves {
        bytes.extend_from_slice(&leaf.total_mass.to_le_bytes());
        for prefix in &leaf.prefixes {
            bytes.extend_from_slice(&prefix.retained_assignments.to_le_bytes());
            bytes.extend_from_slice(&prefix.retained_mass_ppm.to_le_bytes());
            bytes.extend_from_slice(&prefix.quantization_error_numerator.to_le_bytes());
            bytes.extend_from_slice(&prefix.quantization_tv_ppm.to_le_bytes());
        }
    }
    for leaf in &plane.leaves {
        for (&page, &mass) in leaf.pages.iter().zip(&leaf.masses) {
            if mass == 0 {
                return Err(invalid("V23 posting mass differs"));
            }
            bytes.extend_from_slice(&page.to_le_bytes());
            bytes.extend_from_slice(&mass.to_le_bytes());
        }
    }
    let digest = blake3::hash(&bytes);
    bytes.extend_from_slice(digest.as_bytes());
    Ok(bytes)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let value = bytes
        .get(*offset..*offset + 4)
        .ok_or_else(|| invalid("V23 posting plane is truncated"))?;
    *offset += 4;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let value = bytes
        .get(*offset..*offset + 8)
        .ok_or_else(|| invalid("V23 posting plane is truncated"))?;
    *offset += 8;
    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}

pub(crate) fn decode_posting_plane(bytes: &[u8]) -> Result<V23PostingPlane> {
    if bytes.len() < 64 || bytes.get(..8) != Some(PLANE_MAGIC) {
        return Err(invalid("V23 posting plane header differs"));
    }
    let (body, claimed_digest) = bytes.split_at(bytes.len() - 32);
    if blake3::hash(body).as_bytes() != claimed_digest {
        return Err(invalid("V23 posting plane checksum differs"));
    }
    let mut offset = 8;
    let arm = match read_u32(body, &mut offset)? {
        1 => PostingAssignmentArm::OneLeaf,
        2 => PostingAssignmentArm::TwoBeamLeaves,
        _ => return Err(invalid("V23 posting arm differs")),
    };
    if read_u32(body, &mut offset)? as usize != LEAF_COUNT {
        return Err(invalid("V23 posting leaf count differs"));
    }
    let max_pages_per_leaf = read_u32(body, &mut offset)?;
    if !(1..=V23_POSTING_MAX_PAGES as u32).contains(&max_pages_per_leaf) {
        return Err(invalid("V23 posting cap differs"));
    }
    let partition_count = read_u32(body, &mut offset)?;
    if partition_count as usize != V23_POSTING_PARTITIONS {
        return Err(invalid("V23 posting partition count differs"));
    }
    let entry_count = read_u64(body, &mut offset)? as usize;
    let source_records = read_u64(body, &mut offset)?;
    let maximum_resident_records = read_u64(body, &mut offset)?;
    let maximum_merge_entries = read_u32(body, &mut offset)?;
    let scratch_bytes_peak = read_u64(body, &mut offset)?;
    let expected_scratch_bytes = source_records
        .checked_mul(POSTING_RECORD_BYTES)
        .ok_or_else(|| invalid("V23 posting scratch bytes overflow"))?;
    if source_records == 0
        || maximum_resident_records == 0
        || maximum_resident_records > MAX_RUN_RECORDS as u64
        || maximum_merge_entries == 0
        || maximum_merge_entries > max_pages_per_leaf
        || scratch_bytes_peak != expected_scratch_bytes
        || scratch_bytes_peak > SCRATCH_CEILING_BYTES
    {
        return Err(invalid("V23 posting source record count differs"));
    }
    let mut offsets = Vec::with_capacity(LEAF_COUNT + 1);
    for _ in 0..=LEAF_COUNT {
        offsets.push(read_u32(body, &mut offset)? as usize);
    }
    if offsets[0] != 0
        || offsets[LEAF_COUNT] != entry_count
        || offsets
            .windows(2)
            .any(|pair| pair[0] > pair[1] || pair[1] - pair[0] > max_pages_per_leaf as usize)
    {
        return Err(invalid("V23 posting offsets differ"));
    }
    let mut totals = Vec::with_capacity(LEAF_COUNT);
    let mut prefix_evidence = Vec::<[V23PostingPrefixEvidence; 3]>::with_capacity(LEAF_COUNT);
    for _ in 0..LEAF_COUNT {
        totals.push(read_u64(body, &mut offset)?);
        let mut prefixes = Vec::with_capacity(PREFIX_CAPS.len());
        for _ in PREFIX_CAPS {
            prefixes.push(V23PostingPrefixEvidence {
                retained_assignments: read_u64(body, &mut offset)?,
                retained_mass_ppm: read_u32(body, &mut offset)?,
                quantization_error_numerator: read_u64(body, &mut offset)?,
                quantization_tv_ppm: read_u32(body, &mut offset)?,
            });
        }
        prefix_evidence.push(
            prefixes
                .try_into()
                .map_err(|_| invalid("V23 posting prefix evidence count differs"))?,
        );
    }
    let expected_end = offset
        .checked_add(
            entry_count
                .checked_mul(6)
                .ok_or_else(|| invalid("V23 posting length overflows"))?,
        )
        .ok_or_else(|| invalid("V23 posting length overflows"))?;
    if expected_end != body.len() {
        return Err(invalid("V23 posting encoded length differs"));
    }
    let mut leaves = Vec::with_capacity(LEAF_COUNT);
    for leaf in 0..LEAF_COUNT {
        let count = offsets[leaf + 1] - offsets[leaf];
        let mut pages = Vec::with_capacity(count);
        let mut masses = Vec::with_capacity(count);
        for _ in 0..count {
            let page = read_u32(body, &mut offset)?;
            let mass_bytes = body
                .get(offset..offset + 2)
                .ok_or_else(|| invalid("V23 posting mass is truncated"))?;
            offset += 2;
            let mass = u16::from_le_bytes(mass_bytes.try_into().unwrap());
            if mass == 0 {
                return Err(invalid("V23 posting mass differs"));
            }
            pages.push(page);
            masses.push(mass);
        }
        if masses.windows(2).any(|pair| pair[0] < pair[1])
            || pages
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != pages.len()
        {
            return Err(invalid("V23 posting order differs"));
        }
        for evidence in &prefix_evidence[leaf] {
            if evidence.retained_assignments > totals[leaf]
                || (totals[leaf] == 0
                    && (evidence.retained_assignments != 0
                        || evidence.retained_mass_ppm != 0
                        || evidence.quantization_error_numerator != 0
                        || evidence.quantization_tv_ppm != 0))
                || (totals[leaf] != 0
                    && u64::from(evidence.retained_mass_ppm)
                        != round_ratio_half_even(
                            u128::from(evidence.retained_assignments) * 1_000_000,
                            u128::from(totals[leaf]),
                        )?)
                || (totals[leaf] != 0
                    && u64::from(evidence.quantization_tv_ppm)
                        != round_ratio_half_even(
                            u128::from(evidence.quantization_error_numerator) * 1_000_000,
                            u128::from(totals[leaf]) * 65_535 * 2,
                        )?)
                || evidence.quantization_tv_ppm > 500_000
            {
                return Err(invalid("V23 posting retained mass differs"));
            }
        }
        leaves.push(V23PostingLeaf {
            pages,
            masses,
            total_mass: totals[leaf],
            prefixes: prefix_evidence[leaf],
        });
    }
    let plane = V23PostingPlane {
        arm,
        max_pages_per_leaf: max_pages_per_leaf as u16,
        partition_count: partition_count as u16,
        source_records,
        maximum_resident_records,
        maximum_merge_entries,
        scratch_bytes_peak,
        leaves,
    };
    validate_posting_plane_semantics(&plane)?;
    Ok(plane)
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::BTreeMap, fs};

    use bytes::Bytes;
    use half::f16;
    use tempfile::tempdir;

    use crate::{
        metric::VectorMetric,
        v23_diagnostic::{
            V23DecodedPage, V23PageInput, V23PageRef, V23PageRow, V23QuantizerFamily,
            decode_v23_page, encode_v23_page,
        },
        v23_incidence_tree::{
            V23IncidenceTrainingShape, V23IncidenceTree, V23TrainingWork, V23TreeLeaf, V23TreeNode,
        },
    };

    use super::{
        PostingAssignmentArm, V23PostingArmRecords, V23PostingRecord,
        build_both_posting_plane_files, build_both_posting_planes, build_posting_plane,
        decode_posting_plane, decode_posting_record, encode_posting_plane, encode_posting_record,
        normalized_mass, page_posting_records, page_posting_records_both,
        posting_prefix_eligibility, validate_posting_artifact, validate_posting_prefix,
        validate_production_posting_plane,
    };

    fn contributions() -> Vec<V23PostingRecord> {
        let mut records = Vec::new();
        for row in 0..4096_u32 {
            records.push(V23PostingRecord {
                leaf: ((row * 17) % 32) as u16,
                page: (row * 13) % 97,
                reserved: 0,
            });
            if row % 3 == 0 {
                records.push(V23PostingRecord {
                    leaf: ((row * 19 + 1) % 32) as u16,
                    page: (row * 7 + 5) % 97,
                    reserved: 0,
                });
            }
        }
        records
    }

    fn incidence_tree() -> V23IncidenceTree {
        let mut zero = [f16::ZERO; 96];
        let mut one = [f16::ZERO; 96];
        zero[0] = f16::ONE;
        one[1] = f16::ONE;
        V23IncidenceTree {
            shape: V23IncidenceTrainingShape {
                dimensions: 96,
                reservoir_rows: 2,
                depth: 1,
                lloyd_iterations: 4,
            },
            reservoir_seed: 7,
            work: V23TrainingWork {
                farthest_seed_dimensions: 0,
                lloyd_dimensions: 0,
                repartition_dimensions: 0,
                total_distance_dimensions: 0,
            },
            nodes: vec![V23TreeNode {
                child_zero: zero,
                child_one: one,
                child_zero_inverse_norm: 1.0,
                child_one_inverse_norm: 1.0,
                boundary_score_bits: 0.0_f32.to_bits(),
                boundary_source_ordinal: 0,
                child_zero_index: 1,
                child_one_index: 2,
            }],
            leaves: vec![
                V23TreeLeaf {
                    centroid: zero,
                    inverse_norm: 1.0,
                    population: 1,
                    mean_squared_residual: 0.0,
                },
                V23TreeLeaf {
                    centroid: one,
                    inverse_norm: 1.0,
                    population: 1,
                    mean_squared_residual: 0.0,
                },
            ],
        }
    }

    fn f16_code(first: f32, second: f32) -> Box<[u8]> {
        let mut bytes = Vec::with_capacity(192);
        for value in [first, second]
            .into_iter()
            .chain(std::iter::repeat_n(0.0, 94))
        {
            bytes.extend_from_slice(&f16::from_f32(value).to_bits().to_le_bytes());
        }
        bytes.into_boxed_slice()
    }

    fn decoded_page(record_ids: [&str; 3]) -> (V23PageRef, V23DecodedPage) {
        let input = V23PageInput {
            generation_checksum: [9; 32],
            page_ordinal: 17,
            metric: VectorMetric::Cosine,
            dimensions: 96,
            family: V23QuantizerFamily::F16Flat,
            code_width: 192,
            primary_rows: vec![
                V23PageRow {
                    canonical_record_id: record_ids[0].as_bytes().into(),
                    code: f16_code(1.0, 0.0),
                },
                V23PageRow {
                    canonical_record_id: record_ids[1].as_bytes().into(),
                    code: f16_code(0.0, 1.0),
                },
            ],
            replicated_rows: vec![V23PageRow {
                canonical_record_id: record_ids[2].as_bytes().into(),
                code: f16_code(1.0, 1.0),
            }],
        };
        let bytes = encode_v23_page(&input).unwrap();
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let page = V23PageRef {
            generation_checksum: input.generation_checksum,
            page_ordinal: input.page_ordinal,
            metric: input.metric.clone(),
            dimensions: input.dimensions,
            family: input.family,
            code_width: input.code_width,
            path: format!("pages/{checksum}"),
            checksum,
            encoded_bytes: bytes.len() as u64,
            primary_rows: 2,
            replicated_rows: 1,
        };
        let decoded = decode_v23_page(Bytes::copy_from_slice(&bytes), &page).unwrap();
        (page, decoded)
    }

    #[test]
    fn v23_incidence_postings_stream_authenticated_page_rows_into_each_arm() {
        let tree = incidence_tree();
        let (page, decoded) = decoded_page(["1", "2", "3"]);
        let one = page_posting_records(&tree, &decoded, PostingAssignmentArm::OneLeaf)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let two = page_posting_records(&tree, &decoded, PostingAssignmentArm::TwoBeamLeaves)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(one.len(), 3);
        assert_eq!(two.len(), 6);
        assert!(one.iter().chain(&two).all(|record| {
            record.page == page.page_ordinal && record.reserved == 0 && record.leaf < 2
        }));

        let (_, invalid) = decoded_page(["01", "2", "3"]);
        assert!(
            page_posting_records(&tree, &invalid, PostingAssignmentArm::OneLeaf)
                .next()
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn v23_incidence_postings_both_arms_share_one_page_stream() {
        let tree = incidence_tree();
        let (_, decoded) = decoded_page(["1", "2", "3"]);
        let bundles = page_posting_records_both(&tree, &decoded)
            .collect::<Result<Vec<V23PostingArmRecords>, _>>()
            .unwrap();
        assert_eq!(bundles.len(), 3);
        assert_eq!(
            bundles
                .iter()
                .map(|records| records.one)
                .collect::<Vec<_>>(),
            page_posting_records(&tree, &decoded, PostingAssignmentArm::OneLeaf)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        );
        assert_eq!(
            bundles
                .iter()
                .flat_map(|records| records.two)
                .collect::<Vec<_>>(),
            page_posting_records(&tree, &decoded, PostingAssignmentArm::TwoBeamLeaves)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        );

        let consumed = Cell::new(0_usize);
        let records = bundles.into_iter().map(|records| {
            consumed.set(consumed.get() + 1);
            Ok(records)
        });
        let temporary = tempdir().unwrap();
        let (one, two) = build_both_posting_planes(records, temporary.path(), 2, 32).unwrap();

        assert_eq!(consumed.get(), 3);
        assert_eq!(one.arm, PostingAssignmentArm::OneLeaf);
        assert_eq!(two.arm, PostingAssignmentArm::TwoBeamLeaves);
        assert_eq!(one.source_records, 3);
        assert_eq!(two.source_records, 6);
        assert!(one.maximum_resident_records <= 2);
        assert!(two.maximum_resident_records <= 2);
        assert!(one.maximum_resident_records + two.maximum_resident_records <= 2);
        assert!(one.leaves.iter().all(|leaf| leaf.pages.len() <= 32));
        assert!(two.leaves.iter().all(|leaf| leaf.pages.len() <= 32));
        assert!(temporary.path().read_dir().unwrap().next().is_none());
    }

    #[test]
    fn v23_incidence_postings_stream_v2_artifacts_without_materializing_both_planes() {
        let tree = incidence_tree();
        let (_, decoded) = decoded_page(["1", "2", "3"]);
        let bundles = page_posting_records_both(&tree, &decoded)
            .collect::<Result<Vec<V23PostingArmRecords>, _>>()
            .unwrap();
        let consumed = Cell::new(0_usize);
        let records = bundles.into_iter().map(|records| {
            consumed.set(consumed.get() + 1);
            Ok(records)
        });
        let scratch = tempdir().unwrap();
        let output = tempdir().unwrap();

        let artifacts =
            build_both_posting_plane_files(records, scratch.path(), output.path(), 2, 32).unwrap();

        assert_eq!(consumed.get(), 3);
        assert_eq!(artifacts[0].arm, PostingAssignmentArm::OneLeaf);
        assert_eq!(artifacts[1].arm, PostingAssignmentArm::TwoBeamLeaves);
        assert_eq!(artifacts[0].source_records, 3);
        assert_eq!(artifacts[1].source_records, 6);
        assert!(artifacts.iter().all(|artifact| {
            artifact.maximum_resident_records <= 2
                && artifact.maximum_merge_entries <= 32
                && artifact.encoded_bytes > 32
                && artifact.path.parent() == Some(output.path())
                && artifact
                    .path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains(&artifact.digest)
        }));
        let one_bytes = fs::read(&artifacts[0].path).unwrap();
        let two_bytes = fs::read(&artifacts[1].path).unwrap();
        let reference_scratch = tempdir().unwrap();
        let reference_records = page_posting_records_both(&tree, &decoded);
        let (reference_one, reference_two) =
            build_both_posting_planes(reference_records, reference_scratch.path(), 2, 32).unwrap();
        assert_eq!(one_bytes, encode_posting_plane(&reference_one).unwrap());
        assert_eq!(two_bytes, encode_posting_plane(&reference_two).unwrap());
        assert_eq!(&one_bytes[..8], b"BVIP\x02\0\0\0");
        assert_eq!(&two_bytes[..8], b"BVIP\x02\0\0\0");
        assert_eq!(decode_posting_plane(&one_bytes).unwrap().source_records, 3);
        assert_eq!(decode_posting_plane(&two_bytes).unwrap().source_records, 6);
        validate_posting_artifact(&artifacts[0]).unwrap();
        validate_posting_artifact(&artifacts[1]).unwrap();
        let mut changed_artifact = artifacts[0].clone();
        changed_artifact.scratch_bytes_peak ^= 8;
        assert!(validate_posting_artifact(&changed_artifact).is_err());
        let mut legacy = one_bytes;
        legacy[..8].copy_from_slice(b"BVIP\x01\0\0\0");
        let body_len = legacy.len() - 32;
        let digest = blake3::hash(&legacy[..body_len]);
        legacy[body_len..].copy_from_slice(digest.as_bytes());
        assert!(decode_posting_plane(&legacy).is_err());
        let mut corrupted = fs::read(&artifacts[0].path).unwrap();
        corrupted[super::PLANE_ENTRIES_OFFSET as usize] ^= 1;
        fs::write(&artifacts[0].path, corrupted).unwrap();
        assert!(validate_posting_artifact(&artifacts[0]).is_err());
        assert!(scratch.path().read_dir().unwrap().next().is_none());
    }

    #[test]
    fn v23_incidence_postings_streamed_build_unlinks_partial_inputs_and_outputs() {
        let records = vec![
            Ok(V23PostingArmRecords {
                one: V23PostingRecord {
                    leaf: 1,
                    page: 2,
                    reserved: 0,
                },
                two: [
                    V23PostingRecord {
                        leaf: 1,
                        page: 2,
                        reserved: 0,
                    },
                    V23PostingRecord {
                        leaf: 3,
                        page: 2,
                        reserved: 0,
                    },
                ],
            }),
            Err(super::invalid("injected posting input failure")),
        ];
        let scratch = tempdir().unwrap();
        let output = tempdir().unwrap();

        assert!(
            build_both_posting_plane_files(records, scratch.path(), output.path(), 2, 32).is_err()
        );
        assert!(scratch.path().read_dir().unwrap().next().is_none());
        assert!(output.path().read_dir().unwrap().next().is_none());
    }

    #[test]
    fn v23_incidence_postings_artifact_validation_recomputes_streamed_leaf_semantics() {
        let tree = incidence_tree();
        let (_, decoded) = decoded_page(["1", "2", "3"]);
        let scratch = tempdir().unwrap();
        let output = tempdir().unwrap();
        let [artifact, _] = build_both_posting_plane_files(
            page_posting_records_both(&tree, &decoded),
            scratch.path(),
            output.path(),
            2,
            32,
        )
        .unwrap();
        let mut bytes = fs::read(&artifact.path).unwrap();
        let plane = decode_posting_plane(&bytes).unwrap();
        let leaf = plane
            .leaves
            .iter()
            .position(|posting| posting.total_mass != 0)
            .unwrap();
        let total_offset =
            (super::PLANE_HEADER_BYTES + super::PLANE_OFFSET_BYTES) as usize + leaf * 80;
        let total = u64::from_le_bytes(bytes[total_offset..total_offset + 8].try_into().unwrap());
        bytes[total_offset..total_offset + 8].copy_from_slice(&(total + 1).to_le_bytes());
        let body_len = bytes.len() - 32;
        let internal = blake3::hash(&bytes[..body_len]);
        bytes[body_len..].copy_from_slice(internal.as_bytes());
        let digest = blake3::hash(&bytes).to_hex().to_string();
        let changed_path = output
            .path()
            .join(format!("incidence-postings-one-{digest}.bin"));
        fs::write(&changed_path, bytes).unwrap();
        let changed = super::V23PostingArtifact {
            path: changed_path,
            digest,
            ..artifact
        };

        assert!(validate_posting_artifact(&changed).is_err());
    }

    #[test]
    fn v23_incidence_postings_wire_records_are_exactly_eight_little_endian_bytes() {
        let record = V23PostingRecord {
            leaf: 0x1234,
            page: 0x89ab_cdef,
            reserved: 0,
        };
        let encoded = encode_posting_record(record).unwrap();
        assert_eq!(encoded, [0x34, 0x12, 0xef, 0xcd, 0xab, 0x89, 0, 0]);
        assert_eq!(decode_posting_record(&encoded).unwrap(), record);

        let mut changed = encoded;
        changed[6] = 1;
        assert!(decode_posting_record(&changed).is_err());
        assert!(decode_posting_record(&encoded[..7]).is_err());
        assert_eq!(normalized_mass(1, 2).unwrap(), 32_768);
        assert_eq!(normalized_mass(1, 6).unwrap(), 10_922);
    }

    #[test]
    fn v23_incidence_postings_match_reference_across_bounded_run_sizes() {
        let records = contributions();
        let mut reference = BTreeMap::<(u16, u32), u64>::new();
        for record in &records {
            *reference.entry((record.leaf, record.page)).or_default() += 1;
        }
        let mut by_leaf = BTreeMap::<u16, Vec<(u32, u64)>>::new();
        for ((leaf, page), mass) in reference {
            by_leaf.entry(leaf).or_default().push((page, mass));
        }
        let mut reference = BTreeMap::new();
        for (leaf, mut pages) in by_leaf {
            let total = pages.iter().map(|entry| entry.1).sum();
            pages.sort_unstable_by(|left, right| {
                right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
            });
            for (page, count) in pages.into_iter().take(32) {
                reference.insert((leaf, page), normalized_mass(count, total).unwrap());
            }
        }

        let temporary = tempdir().unwrap();
        let small = build_posting_plane(
            records.iter().copied().map(Ok),
            PostingAssignmentArm::OneLeaf,
            temporary.path(),
            128,
            32,
        )
        .unwrap();
        let large = build_posting_plane(
            records.iter().copied().map(Ok),
            PostingAssignmentArm::OneLeaf,
            temporary.path(),
            4096,
            32,
        )
        .unwrap();
        assert_eq!(small.arm, large.arm);
        assert_eq!(small.max_pages_per_leaf, large.max_pages_per_leaf);
        assert_eq!(small.partition_count, large.partition_count);
        assert_eq!(small.source_records, large.source_records);
        assert_eq!(small.scratch_bytes_peak, large.scratch_bytes_peak);
        assert_eq!(small.leaves, large.leaves);
        assert_eq!(small.entries_as_map(), reference);
        assert!(small.leaves.iter().all(|leaf| leaf.pages.len() <= 32));
        assert!(small.maximum_resident_records <= 128);
        assert!(large.maximum_resident_records <= 4096);
        assert!(small.maximum_merge_entries <= 32);
        assert!(large.maximum_merge_entries <= 32);
        assert_eq!(small.partition_count, 256);
        assert_eq!(small.source_records, records.len() as u64);
        assert!(validate_production_posting_plane(&small).is_err());
        assert!(temporary.path().read_dir().unwrap().next().is_none());
    }

    #[test]
    fn v23_incidence_postings_prefixes_bind_mass_quantization_and_canonical_bytes() {
        let temporary = tempdir().unwrap();
        let plane = build_posting_plane(
            contributions().into_iter().map(Ok),
            PostingAssignmentArm::TwoBeamLeaves,
            temporary.path(),
            257,
            2048,
        )
        .unwrap();
        for cap in [512, 1024, 2048] {
            validate_posting_prefix(&plane, cap).unwrap();
        }
        let encoded = encode_posting_plane(&plane).unwrap();
        assert_eq!(decode_posting_plane(&encoded).unwrap(), plane);

        let mut changed = encoded.clone();
        changed[0] ^= 1;
        assert!(decode_posting_plane(&changed).is_err());
        let mut changed = encoded.clone();
        changed.push(0);
        assert!(decode_posting_plane(&changed).is_err());
        let mut changed = encoded;
        let last = changed.len() - 1;
        changed[last] ^= 1;
        assert!(decode_posting_plane(&changed).is_err());

        let mut changed = plane.clone();
        changed.leaves[0].prefixes[2].retained_mass_ppm ^= 1;
        assert!(encode_posting_plane(&changed).is_err());

        let temporary = tempdir().unwrap();
        let wide = build_posting_plane(
            (0..600_u32).map(|page| {
                Ok(V23PostingRecord {
                    leaf: 0,
                    page,
                    reserved: 0,
                })
            }),
            PostingAssignmentArm::OneLeaf,
            temporary.path(),
            73,
            2048,
        )
        .unwrap();
        assert!(wide.leaves[0].masses.iter().all(|mass| *mass == 109));
        assert!(!posting_prefix_eligibility(&wide, 512).unwrap());
        assert!(validate_posting_prefix(&wide, 512).is_err());
        assert!(posting_prefix_eligibility(&wide, 1024).unwrap());
        validate_posting_prefix(&wide, 1024).unwrap();
        validate_posting_prefix(&wide, 2048).unwrap();
    }

    #[test]
    fn v23_incidence_postings_failure_unlinks_every_partial_run() {
        let mut records = contributions();
        records[1024].reserved = 1;
        let temporary = tempdir().unwrap();
        assert!(
            build_posting_plane(
                records.into_iter().map(Ok),
                PostingAssignmentArm::OneLeaf,
                temporary.path(),
                127,
                2048,
            )
            .is_err()
        );
        assert!(temporary.path().read_dir().unwrap().next().is_none());
    }
}
