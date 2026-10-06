//! UNVERIFIED source-only virtual layout. No serving rewrite or quality claim.
//!
//! Frozen rule: SHA256("borsuk-co-selection-anchor-v1\0" || raw canonical
//! SHA256 || LE u64 logical ID), then logical ID/canonical physical ordinal; first
//! 4096 train, next 512 held. C=min(64,16MiB/(32*16*(D+12))). One original-ID
//! sweep; eight relaxed-gain destinations (block-ID ties), every full partner;
//! strictly positive objective reduction only (block-ID/partner-ID ties).
//! Intact 16-row groups, short tail last, 128 blocks/object, no padding/copies.
//! Jaccard uses integer ratios and logical-ID/ordinal ties. Expansion follows
//! first nomination position. Cover cuts largest internal gaps across objects,
//! ties object ID then offset; all bridge rows count. Original nomination and
//! SQ8 arithmetic remain unchanged. Output is evidence, never execution authority.

use crate::{
    budgeted_page_rank::cover_pages,
    fine_sq8_groups::{
        CoSelectionSources, FineBuildReceipt, FineSq8Index, ResidentLimits, ResourceReceipt,
    },
    hierarchical_semantic_cells::{Artifact, BuildConfig, Result},
    resident_vector_graph::PqGraphIdentity,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

pub const CONFIG_SCHEMA: &str = "borsuk-co-selection-config-v1";
pub const REPORT_SCHEMA: &str = "borsuk-co-selection-diagnostic-v1";
const MAP_SCHEMA: &str = "borsuk-co-selection-map-v1";
const ANCHOR_DOMAIN: &[u8] = b"borsuk-co-selection-anchor-v1\0";
const TRAIN: usize = 4096;
const HELD: usize = 512;
const GROUP: usize = 16;
const OBJECT_BLOCKS: usize = 128;
const TOTAL_BYTES: usize = 16 * 1024 * 1024;
const MEMORY: usize = 512 * 1024 * 1024;
const SOURCE_BYTES: usize = 1024 * 1024 * 1024;
const CONSTRUCTION_OPERATIONS: u64 = 512_000_000_000;
const REPLAY_OPERATIONS: u64 = 20_000_000_000;
const OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const ROOT_BYTES: usize = 65536;
const TERMINAL_RESERVE: usize = 8192;
const FIXED_MEMORY: usize = 64 * 1024 * 1024;
const PREFIX_BYTES: usize = 1_665_668;
const PREFIX_SHA: &str = "7abcf7830e10d214999b26fb499cebf925e16b8a88a931ddb9f981ad3d28d025";
const SEAL_SHA: &str = "374cbd0e8700c85c2b4229468c3a9fa20283deb969b661386fb0078024b9de62";

fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn digest(value: &str) -> Result<[u8; 32]> {
    require(
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "co-selection lowercase SHA256",
    )?;
    let mut result = [0; 32];
    for (i, b) in result.iter_mut().enumerate() {
        *b = u8::from_str_radix(&value[2 * i..2 * i + 2], 16)?;
    }
    Ok(result)
}
fn same(a: &Artifact, b: &Artifact) -> bool {
    a.path == b.path && a.bytes == b.bytes && a.sha256 == b.sha256
}
fn sum(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(0usize, |a, b| {
        a.checked_add(*b)
            .ok_or_else(|| "co-selection size overflow".into())
    })
}
fn reserved<T>(n: usize) -> Result<Vec<T>> {
    require(
        n.checked_mul(std::mem::size_of::<T>())
            .is_some_and(|b| b <= MEMORY),
        "co-selection allocation cap",
    )?;
    let mut v = Vec::new();
    v.try_reserve_exact(n)?;
    require(v.capacity() == n, "co-selection unexpected owned capacity")?;
    Ok(v)
}
fn filled<T: Clone>(n: usize, value: T) -> Result<Vec<T>> {
    let mut v = reserved(n)?;
    v.resize(n, value);
    Ok(v)
}
fn sort_charge(n: usize) -> u64 {
    (n as u64).saturating_mul((usize::BITS - n.max(1).leading_zeros()) as u64 * 2 + 1)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Caps {
    pub memory_bytes: usize,
    pub caller_pinned_bytes: usize,
    pub source_auth_bytes: usize,
    pub construction_operations: u64,
    pub replay_operations: u64,
    pub output_bytes: usize,
    pub deadline_seconds: u64,
    pub cpu_threads: usize,
    pub swap_bytes: usize,
}
impl Caps {
    fn validate(&self) -> Result<()> {
        require(
            (FIXED_MEMORY..=MEMORY).contains(&self.memory_bytes)
                && self
                    .caller_pinned_bytes
                    .checked_add(FIXED_MEMORY)
                    .is_some_and(|n| n <= self.memory_bytes)
                && (1..=SOURCE_BYTES).contains(&self.source_auth_bytes)
                && (1..=CONSTRUCTION_OPERATIONS).contains(&self.construction_operations)
                && (1..=REPLAY_OPERATIONS).contains(&self.replay_operations)
                && (TERMINAL_RESERVE..=OUTPUT_BYTES).contains(&self.output_bytes)
                && (1..=86400).contains(&self.deadline_seconds)
                && self.cpu_threads == 1
                && self.swap_bytes == 0,
            "co-selection explicit resource caps",
        )
    }
}
/// Checked work accounting; external supervisor still owns CPU/RSS/no-swap.
pub struct WorkBudget {
    caps: Caps,
    start: Instant,
    operations: u64,
    construction_operations: u64,
    replay_operations: u64,
    replay: bool,
    source_bytes: usize,
    peak_modeled_bytes: usize,
    retained_bytes: usize,
    next_poll: u64,
}
impl WorkBudget {
    pub fn new(caps: Caps) -> Result<Self> {
        caps.validate()?;
        let retained_bytes = caps.caller_pinned_bytes;
        Ok(Self {
            caps,
            start: Instant::now(),
            operations: 0,
            construction_operations: 0,
            replay_operations: 0,
            replay: false,
            source_bytes: 0,
            peak_modeled_bytes: retained_bytes + FIXED_MEMORY,
            retained_bytes,
            next_poll: 0,
        })
    }
    fn reserve_work(&self, count: u64) -> Result<()> {
        let (used, cap) = self.phase_budget();
        require(
            used.checked_add(count).is_some_and(|n| n <= cap)
                && self.start.elapsed() <= Duration::from_secs(self.caps.deadline_seconds),
            "co-selection operations/deadline",
        )
    }
    fn tick(&mut self, count: u64) -> Result<()> {
        let next = self
            .operations
            .checked_add(count)
            .ok_or("co-selection operation overflow")?;
        let (used, cap) = self.phase_budget();
        let phase = used
            .checked_add(count)
            .ok_or("co-selection phase operation overflow")?;
        require(phase <= cap, "co-selection phase operation cap")?;
        if count == 0 || next >= self.next_poll {
            require(
                self.start.elapsed() <= Duration::from_secs(self.caps.deadline_seconds),
                "co-selection deadline",
            )?;
            self.next_poll = next.saturating_add(4096);
        }
        self.operations = next;
        if self.replay {
            self.replay_operations = phase;
        } else {
            self.construction_operations = phase;
        }
        Ok(())
    }
    fn phase_budget(&self) -> (u64, u64) {
        if self.replay {
            (self.replay_operations, self.caps.replay_operations)
        } else {
            (
                self.construction_operations,
                self.caps.construction_operations,
            )
        }
    }
    /// One explicit transition; no counter, deadline, source or retained-memory
    /// reset. Construction/held and metadata replay have distinct ledgers.
    pub fn begin_replay(&mut self) -> Result<()> {
        require(!self.replay, "co-selection replay phase already entered")?;
        self.tick(0)?;
        self.replay = true;
        Ok(())
    }
    fn source(&mut self, count: usize) -> Result<()> {
        let next = sum(&[self.source_bytes, count])?;
        require(
            next <= self.caps.source_auth_bytes,
            "co-selection source authentication bytes",
        )?;
        self.tick(count as u64)?;
        self.source_bytes = next;
        Ok(())
    }
    fn memory(&mut self, values: &[usize]) -> Result<()> {
        let required = sum(&[sum(values)?, self.retained_bytes])?;
        require(
            required <= self.caps.memory_bytes,
            "co-selection aggregate owned memory admission",
        )?;
        self.peak_modeled_bytes = self.peak_modeled_bytes.max(required);
        Ok(())
    }
    fn serialization(&mut self, bound: usize) -> Result<()> {
        // One bounded encoded buffer plus conservative Value/tree/string
        // storage. FIXED_MEMORY retains the <=2MiB prefix, config, pins,
        // decoder/plan scratch and emergency terminal report concurrently.
        self.memory(&[
            FIXED_MEMORY,
            bound
                .checked_mul(17)
                .ok_or("co-selection serialization size overflow")?,
        ])?;
        self.tick(bound as u64)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Panel {
    pub dataset: String,
    pub root: Artifact,
    pub identity: PqGraphIdentity,
    pub canonical: Artifact,
    pub source_order: Artifact,
    pub fine_order: Artifact,
    pub pq: Artifact,
    pub graph: Artifact,
}
/// No request-vector or ground-truth descriptors are admitted by this schema.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema: String,
    pub panels: [Panel; 2],
    pub original_seal: Artifact,
    pub prefix: Artifact,
    pub caps: Caps,
    pub prior_reads: ReadBudget,
}
/// Costs already consumed in the SAME total envelope, not additional allowance.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadBudget {
    pub operations: usize,
    pub bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
struct Anchor {
    logical_id: i64,
    original_ordinal: u32,
    canonical_ordinal: u32,
    query_sha256: [u8; 32],
    nominees: Vec<u32>,
    // Original group ID and first nomination position, nomination order.
    groups: Vec<(u32, u16)>,
}
/// Only source authentication and unchanged native nomination can construct this.
/// In particular there is no Deserialize or public precomputed-selection API.
pub struct SourceSelections {
    panel: Panel,
    sources: CoSelectionSources,
    anchors: Arc<Vec<Anchor>>,
    training: usize,
    ids: Arc<Vec<i64>>,
    group_hashes: Vec<u8>,
    capacity_bytes: usize,
    nomination_resources: ResourceReceipt,
}

#[derive(Debug, Serialize)]
pub struct CoSelectionLayout {
    rows: usize,
    dimensions: usize,
    capacity: usize,
    root_sha256: String,
    source_sha256: String,
    old_to_new: Vec<u32>,
    new_to_old: Vec<u32>,
    #[serde(serialize_with = "decimal_u128")]
    objective_before: u128,
    #[serde(serialize_with = "decimal_u128")]
    objective_after: u128,
    swaps: Vec<Swap>,
    #[serde(skip)]
    anchors: Arc<Vec<Anchor>>,
    #[serde(skip)]
    training: usize,
    #[serde(skip)]
    ids: Arc<Vec<i64>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Swap {
    group: u32,
    destination: u32,
    partner: u32,
    #[serde(serialize_with = "decimal_u128")]
    gain: u128,
}

// C64 objectives exceed JSON's u64 integer range. Never round through f64 or
// let json! panic on a perfectly valid exact u128 objective.
fn decimal_u128<S: serde::Serializer>(
    value: &u128,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

fn block_capacity(d: usize) -> usize {
    64.min(TOTAL_BYTES / (32 * GROUP * (d + 12)))
}
fn anchor_key(source: &[u8; 32], id: u64) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(ANCHOR_DOMAIN);
    h.update(source);
    h.update(id.to_le_bytes());
    h.finalize().into()
}
fn contribution(c: usize, n: usize) -> Result<u128> {
    require(n <= c + 1, "co-selection block occupancy")?;
    Ok((1u128 << (c + 1)) - (1u128 << (c + 1 - n)))
}
struct Incidence {
    offsets: Vec<usize>,
    anchors: Vec<u16>,
}
impl Incidence {
    fn at(&self, g: usize) -> &[u16] {
        &self.anchors[self.offsets[g]..self.offsets[g + 1]]
    }
    fn build(source: &SourceSelections, work: &mut WorkBudget) -> Result<Self> {
        let groups = source.panel.identity.rows.div_ceil(GROUP);
        let mut offsets = filled(groups + 1, 0usize)?;
        for a in source.anchors.iter().take(source.training) {
            for &(g, _) in &a.groups {
                work.tick(1)?;
                offsets[g as usize + 1] += 1;
            }
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let mut anchors = filled(offsets[groups], 0u16)?;
        let mut cursor = reserved(groups)?;
        cursor.extend_from_slice(&offsets[..groups]);
        for (a, anchor) in source.anchors.iter().take(source.training).enumerate() {
            for &(g, _) in &anchor.groups {
                work.tick(1)?;
                anchors[cursor[g as usize]] = a as u16;
                cursor[g as usize] += 1;
            }
        }
        Ok(Self { offsets, anchors })
    }
}
struct FitState {
    c: usize,
    blocks: Vec<Vec<u32>>,
    block_of: Vec<usize>,
    counts: Vec<u8>,
    incidence: Incidence,
}
impl FitState {
    fn gain(&self, group: usize, partner: usize, work: &mut WorkBudget) -> Result<i128> {
        let s = self.block_of[group];
        let t = self.block_of[partner];
        let b = self.blocks.len();
        let left = self.incidence.at(group);
        let right = self.incidence.at(partner);
        let (mut i, mut j) = (0, 0);
        let mut gain = 0i128;
        while i < left.len() || j < right.len() {
            work.tick(1)?;
            if i < left.len() && j < right.len() && left[i] == right[j] {
                i += 1;
                j += 1;
                continue;
            }
            let forward = j == right.len() || (i < left.len() && left[i] < right[j]);
            let a = if forward {
                let a = left[i] as usize;
                i += 1;
                a
            } else {
                let a = right[j] as usize;
                j += 1;
                a
            };
            let (from, to) = if forward { (s, t) } else { (t, s) };
            let n = self.counts[a * b + from] as usize;
            let m = self.counts[a * b + to] as usize;
            require(n > 0, "co-selection incidence count")?;
            let before = contribution(self.c, n)? + contribution(self.c, m)?;
            let after = contribution(self.c, n - 1)? + contribution(self.c, m + 1)?;
            gain = gain
                .checked_add(before as i128 - after as i128)
                .ok_or("co-selection signed gain overflow")?;
        }
        Ok(gain)
    }
    fn apply(&mut self, g: usize, p: usize, work: &mut WorkBudget) -> Result<()> {
        let s = self.block_of[g];
        let t = self.block_of[p];
        let b = self.blocks.len();
        for (&group, from, to) in [(&g, s, t), (&p, t, s)] {
            for &a in self.incidence.at(group) {
                work.tick(1)?;
                self.counts[a as usize * b + from] = self.counts[a as usize * b + from]
                    .checked_sub(1)
                    .ok_or("co-selection count underflow")?;
                self.counts[a as usize * b + to] = self.counts[a as usize * b + to]
                    .checked_add(1)
                    .ok_or("co-selection count overflow")?;
            }
        }
        work.tick((self.blocks[s].len() + self.blocks[t].len()) as u64)?;
        let si = self.blocks[s]
            .iter()
            .position(|&v| v == g as u32)
            .ok_or("co-selection source member")?;
        let ti = self.blocks[t]
            .iter()
            .position(|&v| v == p as u32)
            .ok_or("co-selection target member")?;
        self.blocks[s][si] = p as u32;
        self.blocks[t][ti] = g as u32;
        self.block_of.swap(g, p);
        Ok(())
    }
    fn objective(&self, work: &mut WorkBudget) -> Result<u128> {
        self.counts.iter().try_fold(0u128, |total, &n| {
            work.tick(1)?;
            total
                .checked_add(contribution(self.c, n as usize)?)
                .ok_or_else(|| "co-selection objective overflow".into())
        })
    }
}
impl CoSelectionLayout {
    pub fn fit(source: &SourceSelections, work: &mut WorkBudget) -> Result<Self> {
        require(
            !work.replay,
            "co-selection fitting outside construction phase",
        )?;
        let n = source.panel.identity.rows;
        let d = source.panel.identity.dimensions;
        require(
            (2..=100_000).contains(&n)
                && (1..=768).contains(&d)
                && source.training > 0
                && source.training <= TRAIN,
            "co-selection fit geometry",
        )?;
        let groups = n.div_ceil(GROUP);
        let full = n / GROUP;
        let c = block_capacity(d);
        let b = groups.div_ceil(c);
        work.tick((groups * 16 + source.training * b * 2) as u64)?;
        let incidences: usize = source
            .anchors
            .iter()
            .take(source.training)
            .map(|a| a.groups.len())
            .sum();
        work.memory(&[
            FIXED_MEMORY,
            source.capacity_bytes,
            groups * 128,
            incidences * 2,
            source.training * b,
            b * 128,
        ])?;
        let mut blocks = reserved(b)?;
        let mut block_of = filled(groups, 0usize)?;
        for block in 0..b {
            let end = ((block + 1) * c).min(groups);
            let mut members = reserved(end - block * c)?;
            for (g, owner) in block_of.iter_mut().enumerate().take(end).skip(block * c) {
                *owner = block;
                members.push(g as u32);
            }
            blocks.push(members);
        }
        let mut counts = filled(source.training * b, 0u8)?;
        for (a, anchor) in source.anchors.iter().take(source.training).enumerate() {
            for &(g, _) in &anchor.groups {
                work.tick(1)?;
                counts[a * b + block_of[g as usize]] += 1;
            }
        }
        let mut state = FitState {
            c,
            blocks,
            block_of,
            counts,
            incidence: Incidence::build(source, work)?,
        };
        let before = state.objective(work)?;
        let mut objective = before;
        let mut swaps = reserved(full)?;
        let mut candidates = reserved(b)?;
        for g in 0..full {
            let s = state.block_of[g];
            candidates.clear();
            for t in 0..b {
                if t != s {
                    work.tick(1)?;
                    let mut shares = false;
                    let mut gain = 0i128;
                    for &a in state.incidence.at(g) {
                        work.tick(1)?;
                        let ns = state.counts[a as usize * b + s] as usize;
                        let nt = state.counts[a as usize * b + t] as usize;
                        shares |= nt > 0;
                        gain = gain
                            .checked_add(
                                (contribution(c, ns)? + contribution(c, nt)?) as i128
                                    - (contribution(c, ns - 1)? + contribution(c, nt + 1)?) as i128,
                            )
                            .ok_or("co-selection move gain overflow")?;
                    }
                    if shares {
                        candidates.push((gain, t));
                    }
                }
            }
            work.tick(sort_charge(candidates.len()))?;
            candidates.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            let mut best: Option<(i128, usize, usize)> = None;
            for &(_, t) in candidates.iter().take(8) {
                for &p in &state.blocks[t] {
                    work.tick(1)?;
                    let p = p as usize;
                    if p >= full {
                        continue;
                    }
                    let gain = state.gain(g, p, work)?;
                    if gain > 0
                        && best.is_none_or(|(old, bt, bp)| {
                            gain > old || (gain == old && (t, p) < (bt, bp))
                        })
                    {
                        best = Some((gain, t, p));
                    }
                }
            }
            if let Some((gain, destination, p)) = best {
                state.apply(g, p, work)?;
                objective = objective
                    .checked_sub(gain as u128)
                    .ok_or("co-selection objective underflow")?;
                swaps.push(Swap {
                    group: g as u32,
                    destination: destination as u32,
                    partner: p as u32,
                    gain: gain as u128,
                });
            }
        }
        require(
            state.objective(work)? == objective && objective <= before,
            "co-selection objective recomputation",
        )?;
        let mut new_to_old = reserved(groups)?;
        for block in &mut state.blocks {
            work.tick(sort_charge(block.len()))?;
            block.sort_unstable();
            new_to_old.extend_from_slice(block);
        }
        let mut old_to_new = filled(groups, u32::MAX)?;
        for (new, &old) in new_to_old.iter().enumerate() {
            require(
                (old as usize) < groups && old_to_new[old as usize] == u32::MAX,
                "co-selection full bijection",
            )?;
            old_to_new[old as usize] = new as u32;
        }
        require(
            n % GROUP == 0 || old_to_new[full] == full as u32,
            "co-selection tail last",
        )?;
        Ok(Self {
            rows: n,
            dimensions: d,
            capacity: c,
            root_sha256: source.panel.root.sha256.clone(),
            source_sha256: source.panel.canonical.sha256.clone(),
            old_to_new,
            new_to_old,
            objective_before: before,
            objective_after: objective,
            swaps,
            anchors: source.anchors.clone(),
            training: source.training,
            ids: source.ids.clone(),
        })
    }
    fn group_range(&self, old: usize) -> ObjectRange {
        let new = self.old_to_new[old] as usize;
        let per = self.capacity * OBJECT_BLOCKS;
        let object = new / per;
        let first = (new % per) * GROUP;
        let rows = GROUP.min(self.rows - old * GROUP);
        ObjectRange {
            object: object as u32,
            bytes: first * (self.dimensions + 12)..(first + rows) * (self.dimensions + 12),
        }
    }
    fn cover(
        &self,
        selected: &[bool],
        requests: usize,
        work: &mut WorkBudget,
    ) -> Result<Option<Vec<ObjectRange>>> {
        work.tick((selected.len() * 3) as u64)?;
        let mut ranges = reserved(selected.iter().filter(|v| **v).count())?;
        for &old in &self.new_to_old {
            if selected[old as usize] {
                ranges.push(self.group_range(old as usize));
            }
        }
        minimum_cover(&ranges, requests, work)
    }
    /// Complete nominal set survives or the whole plan is rejected. No partial
    /// cover is returned as success, even when prior costs exhaust the envelope.
    pub fn plan(
        &self,
        nominees: &[usize],
        prior: ReadBudget,
        work: &mut WorkBudget,
    ) -> Result<LayoutPlan> {
        let started = work.operations;
        let g = self.old_to_new.len();
        require(
            !nominees.is_empty() && nominees.len() <= 1024,
            "co-selection nominee count",
        )?;
        work.tick((self.rows + g * 6 + self.anchors.len() + nominees.len() * 32) as u64)?;
        work.memory(&[
            FIXED_MEMORY,
            self.owned_capacity_bytes(),
            g * 256,
            self.rows,
            nominees.len() * 64,
        ])?;
        let mut selected = filled(g, false)?;
        let mut logical_ids = reserved(nominees.len())?;
        let mut seen = filled(self.rows, false)?;
        for &n in nominees {
            work.tick(1)?;
            require(
                n < self.rows && !std::mem::replace(&mut seen[n], true),
                "co-selection nominee ordinal/uniqueness",
            )?;
            require(
                self.old_to_new
                    .get(n / GROUP)
                    .and_then(|&new| self.new_to_old.get(new as usize))
                    == Some(&((n / GROUP) as u32)),
                "co-selection mandatory nominee inverse-map invariant",
            )?;
            selected[n / GROUP] = true;
            logical_ids.push(self.ids[n]);
        }
        let mut mandatory = reserved(g)?;
        for (i, &v) in selected.iter().enumerate() {
            if v {
                mandatory.push(i as u32);
            }
        }
        let mut expansion = reserved(g)?;
        let mut best: Option<(usize, usize, usize)> = None;
        for (a, anchor) in self.anchors.iter().take(self.training).enumerate() {
            work.tick(anchor.groups.len() as u64 + 1)?;
            let intersection = anchor
                .groups
                .iter()
                .filter(|(g, _)| selected[*g as usize])
                .count();
            if intersection == 0 {
                continue;
            }
            let union = mandatory.len() + anchor.groups.len() - intersection;
            let improve = best.is_none_or(|(i, bi, bu)| {
                let old = &self.anchors[i];
                intersection * bu > bi * union
                    || (intersection * bu == bi * union
                        && (anchor.logical_id, anchor.canonical_ordinal)
                            < (old.logical_id, old.canonical_ordinal))
            });
            if improve {
                best = Some((a, intersection, union));
            }
        }
        let requests = 32usize.saturating_sub(prior.operations);
        let allowance = TOTAL_BYTES.saturating_sub(prior.bytes);
        let mut ranges = self.cover(&selected, requests, work)?;
        let request_cap_feasible = ranges.is_some();
        if ranges.is_none() {
            // A J>K rejection still retains a complete covering witness, one
            // span per object. It is explicitly over budget, never a partial
            // plan. Nominee loss is always an invariant error (INVALID).
            let required = self
                .new_to_old
                .chunks(self.capacity * OBJECT_BLOCKS)
                .filter(|part| part.iter().any(|&old| selected[old as usize]))
                .count();
            ranges = self.cover(&selected, required, work)?;
        }
        let cost = |r: &Option<Vec<ObjectRange>>| {
            r.as_ref()
                .map(|v| v.iter().map(|r| r.bytes.len()).sum::<usize>())
        };
        let fits = prior.operations <= 32
            && prior.bytes <= TOTAL_BYTES
            && request_cap_feasible
            && cost(&ranges).is_some_and(|n| n <= allowance);
        let mandatory_cover = CoverSummary {
            ranges: ranges
                .as_ref()
                .ok_or("co-selection complete mandatory cover invariant")?
                .clone(),
            payload_bytes: cost(&ranges).ok_or("co-selection mandatory cost invariant")?,
            request_cap_feasible,
            fits,
        };
        let mut skipped = 0usize;
        if fits {
            if let Some((a, _, _)) = best {
                // Stored order is the first actual nomination position, including
                // the deterministic group-ID tie domain (positions are unique).
                for &(group, _) in &self.anchors[a].groups {
                    let group = group as usize;
                    if selected[group] {
                        continue;
                    }
                    selected[group] = true;
                    let next = self.cover(&selected, requests, work)?;
                    if cost(&next).is_some_and(|n| n <= allowance) {
                        expansion.push(group as u32);
                        ranges = next;
                    } else {
                        selected[group] = false;
                        skipped += 1;
                    }
                }
            }
        }
        let mut bridges = reserved(g)?;
        if let Some(rs) = &ranges {
            for &old in &self.new_to_old {
                work.tick(rs.len() as u64)?;
                let r = self.group_range(old as usize);
                if !selected[old as usize]
                    && rs.iter().any(|v| {
                        v.object == r.object
                            && v.bytes.start <= r.bytes.start
                            && v.bytes.end >= r.bytes.end
                    })
                {
                    bridges.push(old);
                }
            }
        }
        let retained = ranges.as_ref().is_some_and(|rs| {
            nominees.iter().all(|&n| {
                let mut r = self.group_range(n / GROUP);
                r.bytes.start += (n % GROUP) * (self.dimensions + 12);
                r.bytes.end = r.bytes.start + self.dimensions + 12;
                rs.iter().any(|v| {
                    v.object == r.object
                        && v.bytes.start <= r.bytes.start
                        && v.bytes.end >= r.bytes.end
                })
            })
        });
        require(
            retained,
            "co-selection mandatory nominee retention invariant",
        )?;
        work.tick((g + nominees.len() * ranges.as_ref().map_or(0, Vec::len)) as u64)?;
        let payload_bytes = cost(&ranges);
        let range_count = ranges.as_ref().map_or(0, Vec::len);
        let rows = |groups: &[u32]| {
            groups
                .iter()
                .map(|&g| GROUP.min(self.rows - g as usize * GROUP))
                .sum::<usize>()
        };
        Ok(LayoutPlan {
            fits,
            all_nominees_retained: retained,
            mandatory_cover,
            root_sha256: self.root_sha256.clone(),
            source_sha256: self.source_sha256.clone(),
            logical_nominees: logical_ids,
            mandatory_rows: rows(&mandatory),
            expansion_rows: rows(&expansion),
            bridge_rows: rows(&bridges),
            mandatory_groups: mandatory,
            expansion_groups: expansion,
            bridge_groups: bridges,
            ranges: ranges.unwrap_or_default(),
            payload_bytes,
            total_bytes: payload_bytes.and_then(|n| n.checked_add(prior.bytes)),
            total_operations: prior.operations.checked_add(range_count),
            prior,
            anchor: best.map(|(a, i, u)| AnchorMatch {
                logical_id: self.anchors[a].logical_id,
                original_ordinal: self.anchors[a].original_ordinal,
                canonical_ordinal: self.anchors[a].canonical_ordinal,
                intersection: i,
                union: u,
            }),
            expansion_skipped: skipped,
            counted_operations: work.operations - started,
            cover_ties: "largest gap first; object ID then byte offset",
            anchor_ties: "logical ID then canonical physical ordinal",
        })
    }
    fn owned_capacity_bytes(&self) -> usize {
        self.anchors.capacity() * std::mem::size_of::<Anchor>()
            + self
                .anchors
                .iter()
                .map(|a| {
                    a.nominees.capacity() * 4
                        + a.groups.capacity() * std::mem::size_of::<(u32, u16)>()
                })
                .sum::<usize>()
            + self.ids.capacity() * 8
            + self.old_to_new.capacity() * 4
            + self.new_to_old.capacity() * 4
            + self.swaps.capacity() * std::mem::size_of::<Swap>()
            + ROOT_BYTES * 16
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ObjectRange {
    pub object: u32,
    pub bytes: Range<usize>,
}
/// Inputs are sorted disjoint selected intervals. A missing cross-object cover
/// is a valid locality rejection, not an execution error.
fn minimum_cover(
    selected: &[ObjectRange],
    requests: usize,
    work: &mut WorkBudget,
) -> Result<Option<Vec<ObjectRange>>> {
    if selected.is_empty() {
        return Ok(Some(Vec::new()));
    }
    work.tick((selected.len() * 4) as u64)?;
    let mut gaps = reserved(selected.len())?;
    let mut objects = 1;
    for (i, pair) in selected.windows(2).enumerate() {
        let (a, b) = (&pair[0], &pair[1]);
        require(
            a.bytes.start < a.bytes.end
                && b.bytes.start < b.bytes.end
                && (a.object < b.object || (a.object == b.object && a.bytes.end <= b.bytes.start)),
            "co-selection ordered interval input",
        )?;
        if a.object != b.object {
            objects += 1;
        } else if a.bytes.end < b.bytes.start {
            gaps.push((b.bytes.start - a.bytes.end, a.object, a.bytes.end, i));
        }
    }
    require(
        selected[0].bytes.start < selected[0].bytes.end,
        "co-selection nonempty interval",
    )?;
    if objects > requests {
        return Ok(None);
    }
    work.tick(sort_charge(gaps.len()))?;
    gaps.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let mut cuts = filled(selected.len(), false)?;
    for &(_, _, _, i) in gaps.iter().take(requests - objects) {
        cuts[i] = true;
    }
    let mut result = reserved(selected.len().min(requests))?;
    let mut current = selected[0].clone();
    for i in 1..selected.len() {
        if selected[i].object != current.object || cuts[i - 1] {
            result.push(current);
            current = selected[i].clone();
        } else {
            current.bytes.end = selected[i].bytes.end;
        }
    }
    result.push(current);
    Ok(Some(result))
}
#[derive(Debug, Serialize)]
struct AnchorMatch {
    logical_id: i64,
    original_ordinal: u32,
    canonical_ordinal: u32,
    intersection: usize,
    union: usize,
}
#[derive(Debug, Serialize)]
struct CoverSummary {
    ranges: Vec<ObjectRange>,
    payload_bytes: usize,
    request_cap_feasible: bool,
    fits: bool,
}
#[derive(Debug, Serialize)]
pub struct LayoutPlan {
    pub fits: bool,
    pub all_nominees_retained: bool,
    mandatory_cover: CoverSummary,
    root_sha256: String,
    source_sha256: String,
    logical_nominees: Vec<i64>,
    mandatory_groups: Vec<u32>,
    expansion_groups: Vec<u32>,
    bridge_groups: Vec<u32>,
    mandatory_rows: usize,
    expansion_rows: usize,
    bridge_rows: usize,
    pub ranges: Vec<ObjectRange>,
    pub payload_bytes: Option<usize>,
    total_bytes: Option<usize>,
    total_operations: Option<usize>,
    prior: ReadBudget,
    anchor: Option<AnchorMatch>,
    expansion_skipped: usize,
    counted_operations: u64,
    cover_ties: &'static str,
    anchor_ties: &'static str,
}

#[cfg(test)]
thread_local! {
    static OPENED:std::cell::RefCell<Vec<PathBuf>>=const {std::cell::RefCell::new(Vec::new())};
}
fn secure_open(path: &Path, flags: rustix::fs::OFlags) -> Result<File> {
    use rustix::fs::{Mode, OFlags as F};
    require(
        path.is_absolute() && path.as_os_str().len() <= 4096,
        "co-selection absolute bounded path",
    )?;
    require(
        path.components()
            .skip(1)
            .all(|c| matches!(c, Component::Normal(_))),
        "co-selection normal path components",
    )?;
    let mut fd = File::from(rustix::fs::open(
        "/",
        F::RDONLY | F::DIRECTORY | F::CLOEXEC,
        Mode::empty(),
    )?);
    let mut parts = path.components().skip(1).peekable();
    let mut count = 0;
    while let Some(Component::Normal(name)) = parts.next() {
        count += 1;
        require(count <= 128, "co-selection path depth")?;
        fd = File::from(rustix::fs::openat(
            &fd,
            name,
            (if parts.peek().is_none() {
                flags
            } else {
                F::RDONLY | F::DIRECTORY
            }) | F::NOFOLLOW
                | F::NONBLOCK
                | F::CLOEXEC,
            Mode::empty(),
        )?);
    }
    #[cfg(test)]
    OPENED.with(|v| v.borrow_mut().push(path.into()));
    Ok(fd)
}
fn input(a: &Artifact, cap: usize, prefix: bool) -> Result<File> {
    digest(&a.sha256)?;
    require(
        a.bytes > 0 && a.bytes <= cap,
        "co-selection input admission before open",
    )?;
    let f = secure_open(&a.path, rustix::fs::OFlags::RDONLY)?;
    let m = f.metadata()?;
    require(
        m.is_file()
            && if prefix {
                m.len() >= a.bytes as u64
            } else {
                m.len() == a.bytes as u64
            },
        "co-selection regular input length",
    )?;
    Ok(f)
}
fn read_pinned(a: &Artifact, cap: usize, prefix: bool, work: &mut WorkBudget) -> Result<Vec<u8>> {
    require(a.bytes <= cap, "co-selection read cap before allocation")?;
    work.source(a.bytes)?;
    let mut f = input(a, cap, prefix)?;
    let mut body = filled(a.bytes, 0u8)?;
    for part in body.chunks_mut(65536) {
        work.tick(0)?;
        f.read_exact(part)?;
    }
    // A historical prefix may share a file with forbidden query results/GT.
    // Never probe EOF or prefetch beyond that exact frozen prefix boundary.
    require(
        (prefix || f.read(&mut [0])? == 0) && hash(&body) == a.sha256,
        "co-selection input EOF/SHA256",
    )?;
    Ok(body)
}
fn authenticate(a: &Artifact, cap: usize, work: &mut WorkBudget) -> Result<()> {
    work.source(a.bytes)?;
    let mut file = input(a, cap, false)?;
    let mut h = Sha256::new();
    let mut remaining = a.bytes;
    let mut buf = [0; 65536];
    while remaining > 0 {
        work.tick(0)?;
        let count = remaining.min(buf.len());
        file.read_exact(&mut buf[..count])?;
        h.update(&buf[..count]);
        remaining -= count;
    }
    require(
        file.read(&mut [0])? == 0 && format!("{:x}", h.finalize()) == a.sha256,
        "co-selection streamed EOF/SHA256",
    )
}
fn nomination_work(evaluations: usize, visits: usize, d: usize, n: usize) -> u64 {
    // Count 64 PQ subspace additions, bounded heap/sort comparisons per score,
    // maximum 256 examined base edges per visit, table coordinates and planner
    // set/cover scans. Opaque call reserved at its frozen maximum before entry.
    (evaluations as u64) * 128
        + (visits as u64) * 256
        + (256 * d) as u64
        + sort_charge(4096.min(n))
        + 65536
}
impl SourceSelections {
    pub fn generate(panel: &Panel, work: &mut WorkBudget) -> Result<Self> {
        Self::generate_counts(panel, TRAIN, HELD, work)
    }
    fn generate_counts(
        panel: &Panel,
        training: usize,
        held: usize,
        work: &mut WorkBudget,
    ) -> Result<Self> {
        require(
            !work.replay,
            "co-selection source generation outside construction phase",
        )?;
        let n = panel.identity.rows;
        let d = panel.identity.dimensions;
        let count = training + held;
        require(
            (2..=100_000).contains(&n)
                && (1..=768).contains(&d)
                && count <= n
                && training > 0
                && training <= TRAIN
                && held <= HELD,
            "co-selection exact source anchor geometry",
        )?;
        let groups = n.div_ceil(GROUP);
        // Worst owned capacities: every nominee is in a distinct group. Arc
        // shares the immutable selections with the fitted map, never double
        // counts one allocation or claims sharing for the two distinct panels.
        let selections = count
            * (std::mem::size_of::<Anchor>() + 1024 * (4 + std::mem::size_of::<(u32, u16)>()))
            + n * 8
            + groups * 32;
        let vectors = count * (d * 4 + std::mem::size_of::<Vec<f32>>());
        let scratch = n * 96 + groups * 32 + ROOT_BYTES * 32;
        work.memory(&[
            FIXED_MEMORY,
            selections,
            vectors,
            scratch,
            n * 1024,
            4 * 1024 * 1024,
        ])?;
        let _root = read_pinned(&panel.root, ROOT_BYTES, false, work)?;
        work.source(panel.root.bytes)?;
        let sources = FineSq8Index::co_selection_sources(&panel.root)?;
        require(
            sources.identity == panel.identity
                && same(&sources.original.canonical, &panel.canonical)
                && same(&sources.original.order, &panel.source_order)
                && same(&sources.order, &panel.fine_order)
                && same(&sources.pq, &panel.pq)
                && same(&sources.graph, &panel.graph)
                && panel.canonical.bytes == n * (8 + 4 * d)
                && panel.source_order.bytes == n * 8
                && panel.fine_order.bytes == n * 8,
            "co-selection independently pinned source/root/PQ/graph/order bindings",
        )?;
        let primary = read_pinned(&sources.primary_root, ROOT_BYTES, false, work)?;
        let primary: Value = serde_json::from_slice(&primary)?;
        let original: BuildConfig = serde_json::from_value(primary["input"].clone())?;
        require(
            serde_json::to_vec(&original)? == serde_json::to_vec(&sources.original)?
                && primary["rows"] == n
                && primary["dimensions"] == d,
            "co-selection original primary source identity",
        )?;
        // These independent pins are authenticated before the unchanged index
        // opens them again. The second reads are explicitly admitted below.
        authenticate(&panel.pq, 24 + 64 * 256 * d.div_ceil(64) * 4 + n * 64, work)?;
        authenticate(&panel.graph, n * 512, work)?;
        let order_body = read_pinned(&panel.source_order, n * 8, false, work)?;
        let fine_body = read_pinned(&panel.fine_order, n * 8, false, work)?;
        let mut source_ids = reserved(n)?;
        let mut canonical_position = filled(n, u32::MAX)?;
        for (ordinal, word) in order_body.chunks_exact(8).enumerate() {
            work.tick(1)?;
            let id = usize::try_from(u64::from_le_bytes(word.try_into()?))?;
            require(
                id < n && canonical_position[id] == u32::MAX,
                "co-selection source roster bijection",
            )?;
            canonical_position[id] = ordinal as u32;
            source_ids.push(id as i64);
        }
        let mut ids = reserved(n)?;
        let mut seen = filled(n, false)?;
        for word in fine_body.chunks_exact(8) {
            work.tick(1)?;
            let id = usize::try_from(u64::from_le_bytes(word.try_into()?))?;
            require(
                id < n && !std::mem::replace(&mut seen[id], true),
                "co-selection fine roster bijection",
            )?;
            ids.push(id as i64);
        }
        let source = digest(&panel.canonical.sha256)?;
        let mut ordered = reserved(n)?;
        for (ordinal, &id) in ids.iter().enumerate() {
            work.tick((ANCHOR_DOMAIN.len() + 40) as u64)?;
            ordered.push((
                anchor_key(&source, u64::try_from(id)?),
                id,
                canonical_position[id as usize],
                ordinal as u32,
            ));
        }
        work.tick(sort_charge(n))?;
        ordered.sort_unstable();
        let mut selected = filled(n, u32::MAX)?;
        let mut anchor_vectors = reserved(count)?;
        for (slot, &(_, id, _, _)) in ordered.iter().take(count).enumerate() {
            selected[canonical_position[id as usize] as usize] = slot as u32;
            anchor_vectors.push(reserved::<f32>(d)?);
        }
        // One exact authenticated stream, collecting ONLY selected source
        // vectors. Do not call nomination until the final EOF/hash succeeds.
        work.source(panel.canonical.bytes)?;
        let mut canonical = input(&panel.canonical, n * (8 + 4 * d), false)?;
        let mut sha = Sha256::new();
        let mut row = filled(8 + 4 * d, 0u8)?;
        for ordinal in 0..n {
            work.tick(1)?;
            canonical.read_exact(&mut row)?;
            sha.update(&row);
            require(
                i64::from_le_bytes(row[..8].try_into()?) == source_ids[ordinal],
                "co-selection canonical source ID/order",
            )?;
            let slot = selected[ordinal];
            if slot != u32::MAX {
                let vector = &mut anchor_vectors[slot as usize];
                for word in row[8..].chunks_exact(4) {
                    vector.push(f32::from_le_bytes(word.try_into()?));
                }
                crate::sq8_source::cosine_vector(vector)?;
            }
        }
        require(
            canonical.read(&mut [0])? == 0
                && format!("{:x}", sha.finalize()) == panel.canonical.sha256,
            "co-selection canonical complete EOF/SHA256",
        )?;
        let group_hashes = read_pinned(&sources.groups, groups * 32, false, work)?;
        let runtime = sum(&[
            FIXED_MEMORY,
            work.retained_bytes,
            selections,
            vectors,
            scratch,
        ])?;
        let limits = ResidentLimits {
            max_peak_payload_bytes: work.caps.memory_bytes,
            pinned_generation_bytes: 0,
            active_queries: 1,
            delta_bytes: 0,
            maintenance_bytes: 0,
            runtime_bytes: runtime,
        };
        // admission + open_remote admission + its manifest read; graph/PQ,
        // order and group digest table. No records handle is ever acquired.
        work.source(sum(&[
            3 * panel.root.bytes,
            panel.pq.bytes,
            panel.graph.bytes,
            panel.fine_order.bytes,
            sources.groups.bytes,
        ])?)?;
        let admitted = FineSq8Index::admission(&panel.root, &limits)?;
        work.memory(&[admitted
            .admitted_peak_bytes
            .saturating_sub(work.retained_bytes)])?;
        work.tick((n * 1024 + panel.pq.bytes * 4 + panel.graph.bytes * 66) as u64)?;
        let index = FineSq8Index::open_remote(&panel.root, &limits)?;
        require(
            index.co_selection_logical_ids() == ids,
            "co-selection reopened index order parity",
        )?;
        let mut workspace = index.new_workspace()?;
        let mut anchors = reserved(count)?;
        for (slot, vector) in anchor_vectors.into_iter().enumerate() {
            work.tick((n + groups + 4096 + d * 4) as u64)?;
            let upper = nomination_work(65536, n.min(65536), d, n);
            work.reserve_work(upper)?;
            let plan = match index.plan(&vector, &mut workspace) {
                Ok(p) => p,
                Err(e) => {
                    work.tick(upper)?;
                    return Err(e);
                }
            };
            let (evaluations, visits) = plan.co_selection_work();
            require(
                evaluations <= 65536 && visits <= n.min(evaluations),
                "co-selection opaque nomination work bound invariant",
            )?;
            work.tick(nomination_work(evaluations, visits, d, n))?;
            require(
                !plan.nominees().is_empty() && plan.nominees().len() <= 1024,
                "co-selection actual source nominees",
            )?;
            let mut nominees = reserved(plan.nominees().len())?;
            let mut selected_groups = reserved(plan.nominees().len())?;
            let mut group_seen = filled(groups, false)?;
            let mut row_seen = filled(n, false)?;
            for (position, &ordinal) in plan.nominees().iter().enumerate() {
                require(
                    ordinal < n && !std::mem::replace(&mut row_seen[ordinal], true),
                    "co-selection source nominee bijection",
                )?;
                nominees.push(ordinal as u32);
                let g = ordinal / GROUP;
                if !std::mem::replace(&mut group_seen[g], true) {
                    selected_groups.push((g as u32, position as u16));
                }
            }
            let mut h = Sha256::new();
            for x in &vector {
                h.update(x.to_le_bytes());
            }
            let (_, id, canonical_ordinal, ordinal) = ordered[slot];
            anchors.push(Anchor {
                logical_id: id,
                original_ordinal: ordinal,
                canonical_ordinal,
                query_sha256: h.finalize().into(),
                nominees,
                groups: selected_groups,
            });
        }
        let capacity_bytes = anchors.capacity() * std::mem::size_of::<Anchor>()
            + anchors
                .iter()
                .map(|a| {
                    a.nominees.capacity() * 4
                        + a.groups.capacity() * std::mem::size_of::<(u32, u16)>()
                })
                .sum::<usize>()
            + ids.capacity() * 8
            + group_hashes.capacity()
            + ROOT_BYTES * 16;
        require(
            capacity_bytes <= selections + ROOT_BYTES * 16,
            "co-selection actual selection allocation bound",
        )?;
        Ok(Self {
            panel: panel.clone(),
            sources,
            anchors: Arc::new(anchors),
            training,
            ids: Arc::new(ids),
            group_hashes,
            capacity_bytes,
            nomination_resources: index.resources.clone(),
        })
    }
}

fn source_hashes() -> Value {
    json!({
        "co_selection_layout.rs":hash(include_bytes!("co_selection_layout.rs")),
        "fine_sq8_groups.rs":hash(include_bytes!("fine_sq8_groups.rs")),
        "pq64_nominee.rs":hash(include_bytes!("pq64_nominee.rs")),
        "resident_vector_graph.rs":hash(include_bytes!("resident_vector_graph.rs")),
        "hierarchical_semantic_cells.rs":hash(include_bytes!("hierarchical_semantic_cells.rs")),
        "budgeted_page_rank.rs":hash(include_bytes!("budgeted_page_rank.rs")),
        "sq8_page_authority.rs":hash(include_bytes!("sq8_page_authority.rs")),
        "returned_sq8.rs":hash(include_bytes!("returned_sq8.rs")),
        "exact_sq8_nominee.rs":hash(include_bytes!("exact_sq8_nominee.rs")),
        "centroid_hnsw.rs":hash(include_bytes!("centroid_hnsw.rs")),
        "sq8_source.rs":hash(include_bytes!("sq8_source.rs")),
        "bin/hierarchical_semantic_cells.rs":hash(include_bytes!("bin/hierarchical_semantic_cells.rs")),
        "lib.rs":hash(include_bytes!("lib.rs"))
    })
}
fn source_code_bytes() -> usize {
    include_bytes!("co_selection_layout.rs").len()
        + include_bytes!("fine_sq8_groups.rs").len()
        + include_bytes!("pq64_nominee.rs").len()
        + include_bytes!("resident_vector_graph.rs").len()
        + include_bytes!("hierarchical_semantic_cells.rs").len()
        + include_bytes!("budgeted_page_rank.rs").len()
        + include_bytes!("sq8_page_authority.rs").len()
        + include_bytes!("returned_sq8.rs").len()
        + include_bytes!("exact_sq8_nominee.rs").len()
        + include_bytes!("centroid_hnsw.rs").len()
        + include_bytes!("sq8_source.rs").len()
        + include_bytes!("bin/hierarchical_semantic_cells.rs").len()
        + include_bytes!("lib.rs").len()
}
struct Outputs {
    path: PathBuf,
    parent: File,
    report: File,
    cap: usize,
    bytes: usize,
    published_bytes: usize,
    #[cfg(test)]
    fail_sync_at: Option<usize>,
    #[cfg(test)]
    sync_count: usize,
}
impl Outputs {
    fn create(path: &Path) -> Result<Self> {
        let parent = secure_open(
            path.parent().ok_or("co-selection output parent")?,
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY,
        )?;
        let report = Self::create_file(&parent, path)?;
        Ok(Self {
            path: path.into(),
            parent,
            report,
            cap: OUTPUT_BYTES,
            bytes: 0,
            published_bytes: 0,
            #[cfg(test)]
            fail_sync_at: None,
            #[cfg(test)]
            sync_count: 0,
        })
    }
    fn create_file(parent: &File, path: &Path) -> Result<File> {
        use rustix::fs::{Mode, OFlags as F};
        Ok(File::from(rustix::fs::openat(
            parent,
            path.file_name().ok_or("co-selection basename")?,
            F::WRONLY | F::CREATE | F::EXCL | F::NOFOLLOW | F::NONBLOCK | F::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?))
    }
    fn companion(&self, extension: &str) -> Result<(PathBuf, File)> {
        let path = self.path.with_extension(extension);
        require(path != self.path, "co-selection output companion collision")?;
        let file = Self::create_file(&self.parent, &path)?;
        Ok((path, file))
    }
    fn write(&mut self, file: &mut File, body: &[u8]) -> Result<()> {
        require(
            sum(&[self.bytes, body.len(), TERMINAL_RESERVE])? <= self.cap,
            "co-selection output cap before write",
        )?;
        // Charge attempted bytes before I/O, retaining failed/partial charges.
        self.bytes += body.len();
        file.write_all(body)?;
        Ok(())
    }
    fn sync(&mut self, file: &File) -> Result<()> {
        #[cfg(test)]
        {
            self.sync_count += 1;
            require(
                self.fail_sync_at != Some(self.sync_count),
                "co-selection injected file fsync",
            )?;
        }
        file.sync_all()?;
        #[cfg(test)]
        {
            self.sync_count += 1;
            require(
                self.fail_sync_at != Some(self.sync_count),
                "co-selection injected directory fsync",
            )?;
        }
        self.parent.sync_all()?;
        Ok(())
    }
    fn publish(&mut self, extension: &str, body: &[u8]) -> Result<Artifact> {
        let (path, mut file) = self.companion(extension)?;
        self.write(&mut file, body)?;
        self.sync(&file)?;
        self.published_bytes += body.len();
        Ok(Artifact {
            path,
            bytes: body.len(),
            sha256: hash(body),
        })
    }
    fn finish(&mut self, report: &Value) -> Result<()> {
        let body = json_bytes(report, TERMINAL_RESERVE, false)?;
        require(
            body.len() <= TERMINAL_RESERVE && sum(&[self.bytes, body.len()])? <= self.cap,
            "co-selection terminal reserve",
        )?;
        self.report.write_all(&body)?;
        let file = self.report.try_clone()?;
        self.sync(&file)
    }
    fn invalidate(
        &mut self,
        config_sha: &str,
        error: &dyn std::fmt::Display,
        work: Option<&WorkBudget>,
    ) {
        let report = terminal(
            config_sha,
            "INVALID",
            false,
            0,
            json!({"error":error.to_string().chars().take(512).collect::<String>(),
            "attempted_output_bytes":self.bytes,"published_output_bytes":self.published_bytes,
            "work":work.map(|w|json!({"caps":w.caps,"construction_operations":w.construction_operations,
                "replay_operations":w.replay_operations,"total_operations":w.operations,"source_auth_bytes":w.source_bytes,
                "retained_capacity_bytes":w.retained_bytes,"modeled_peak_owned_bytes":w.peak_modeled_bytes}))}),
        );
        if let Ok(body) = json_bytes(&report, TERMINAL_RESERVE, false) {
            // Rewrite only this run's CREATE_EXCL report inode. A failed fsync
            // or partial write is never promoted by a copied report body.
            let _ = self.report.set_len(0);
            let _ = self.report.seek(SeekFrom::Start(0));
            let _ = self.report.write_all(&body);
            let _ = self.report.sync_all();
            let _ = self.parent.sync_all();
        }
    }
}
fn terminal(
    config_sha: &str,
    status: &str,
    complete: bool,
    queries: usize,
    details: Value,
) -> Value {
    json!({"schema":REPORT_SCHEMA,"status":status,"complete":complete,"queries":queries,"config_sha256":config_sha,
        "diagnostic_source_sha256":source_hashes(),"standalone_authority":false,"requires_matching_supervisor_exit_receipt":true,
        "quality_or_performance_claim":false,"truth_opened":false,"request_vectors_opened":false,"sq8_bodies_opened":false,
        "source_only_anchor_vectors":true,"details":details})
}
// A slice writer cannot grow past admission, including the JSONL newline.
// The caller admits both this buffer and the Value before constructing either.
fn json_bytes(value: &Value, bound: usize, newline: bool) -> Result<Vec<u8>> {
    let mut body = filled(bound, 0u8)?;
    let end = bound
        .checked_sub(usize::from(newline))
        .ok_or("co-selection JSON bound")?;
    let mut writer = Cursor::new(&mut body[..end]);
    serde_json::to_writer(&mut writer, value)?;
    let written = writer.position() as usize;
    body.truncate(written);
    if newline {
        body.push(b'\n');
    }
    Ok(body)
}
fn selection_bytes(source: &SourceSelections, work: &mut WorkBudget) -> Result<Vec<u8>> {
    // Fixed header: magic, six raw descriptor hashes, train/held u32 counts.
    // Then hash-order anchors: logical i64, fine u32, canonical u32, query raw32,
    // nominee count u32 and ordered nominee u32s. Group/first positions are
    // uniquely reconstructible from these actual unchanged nominee outputs.
    let bytes = 8
        + 6 * 32
        + 8
        + source
            .anchors
            .iter()
            .map(|a| 52 + a.nominees.len() * 4)
            .sum::<usize>();
    work.tick(work_product(4, bytes as u64)?)?;
    work.memory(&[FIXED_MEMORY, bytes])?;
    let mut body = reserved(bytes)?;
    body.extend_from_slice(b"BORSCS01");
    for a in [
        &source.panel.root,
        &source.panel.canonical,
        &source.panel.source_order,
        &source.panel.fine_order,
        &source.panel.pq,
        &source.panel.graph,
    ] {
        body.extend_from_slice(&digest(&a.sha256)?);
    }
    body.extend_from_slice(&(source.training as u32).to_le_bytes());
    body.extend_from_slice(&((source.anchors.len() - source.training) as u32).to_le_bytes());
    for a in source.anchors.iter() {
        body.extend_from_slice(&a.logical_id.to_le_bytes());
        body.extend_from_slice(&a.original_ordinal.to_le_bytes());
        body.extend_from_slice(&a.canonical_ordinal.to_le_bytes());
        body.extend_from_slice(&a.query_sha256);
        body.extend_from_slice(&(a.nominees.len() as u32).to_le_bytes());
        for &n in &a.nominees {
            body.extend_from_slice(&n.to_le_bytes());
        }
    }
    require(
        body.len() == bytes,
        "co-selection selection pin exact length",
    )?;
    Ok(body)
}
fn map_bytes(
    source: &SourceSelections,
    layout: &CoSelectionLayout,
    selection: &Artifact,
    config_sha: &str,
    work: &mut WorkBudget,
    ceiling: &WorkCeiling,
    output_ceiling: &OutputCeiling,
) -> Result<Vec<u8>> {
    work.serialization(output_ceiling.map_per_panel)?;
    let groups = layout.new_to_old.len();
    let per = layout.capacity * OBJECT_BLOCKS;
    let mut objects = reserved(groups.div_ceil(per))?;
    for (id, chunk) in layout.new_to_old.chunks(per).enumerate() {
        let mut h = Sha256::new();
        h.update(b"borsuk-co-selection-virtual-object-v1\0");
        h.update(digest(&source.panel.root.sha256)?);
        h.update((id as u32).to_le_bytes());
        let mut bytes = 0;
        for &old in chunk {
            h.update(old.to_le_bytes());
            h.update(&source.group_hashes[old as usize * 32..(old as usize + 1) * 32]);
            bytes += GROUP.min(layout.rows - old as usize * GROUP) * (layout.dimensions + 12);
        }
        objects.push(
            json!({"object":id,"first_new_group":id*per,"groups":chunk.len(),"bytes":bytes,
            "placement_metadata_sha256":format!("{:x}",h.finalize()),"payload_materialized":false,
            "payload_sha256":null,"payload_etag":null}),
        );
    }
    json_bytes(
        &json!({"schema":MAP_SCHEMA,"config_sha256":config_sha,"diagnostic_source_sha256":source_hashes(),
        "source_selections":selection,"source":source.panel,"original_sources":source.sources,
        "nomination_resources":source.nomination_resources,"selection_owned_capacity_bytes":source.capacity_bytes,
        "group_hashes_hex":source.group_hashes.chunks_exact(32).map(|s|s.iter().map(|b|format!("{b:02x}")).collect::<String>()).collect::<Vec<_>>(),
        "layout":layout,"objects":objects,"held_anchor_count":source.anchors.len()-source.training,"held_used_for_fitting":false,
        "caps":work.caps,"work_preflight":ceiling,"output_preflight":output_ceiling,"phase_work_before_map_serialization":{
            "construction_operations":work.construction_operations,"replay_operations":work.replay_operations,"total_operations":work.operations},
        "group_rows":GROUP,"blocks_per_object":OBJECT_BLOCKS,
        "replication_factor":1,"short_tail_last":true,"within_group_order":"unchanged","query_blind":true,
        "anchor_domain_hex":ANCHOR_DOMAIN.iter().map(|b|format!("{b:02x}")).collect::<String>(),
        "tie_contract":"hash/logical ID/canonical physical ordinal; relaxed gain then block ID; positive swap gain then destination/partner; Jaccard then logical ID/canonical ordinal; gap bytes descending then object/offset"}),
        output_ceiling.map_per_panel,
        false,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalPanel {
    dataset: String,
    root: Artifact,
    requests: Artifact,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalSeal {
    schema: String,
    config_sha256: String,
    source_identity_sha256: String,
    prefix_bytes: usize,
    prefix_sha256: String,
    plans_per_panel: usize,
    panels: [OriginalPanel; 2],
    truth_opened: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenPlan {
    root_sha256: String,
    query_sha256: String,
    revision: u64,
    mutation_sha256: String,
    nominees: Vec<usize>,
    ranges: Vec<Range<usize>>,
    planned_bytes: usize,
    feasible: bool,
    exhausted: bool,
    converged: bool,
    actual_shortlist_rows: usize,
    evaluations: usize,
    base_visits: usize,
    old_page_gets: usize,
    old_page_bytes: usize,
}
#[derive(Deserialize)]
#[serde(tag = "phase", deny_unknown_fields)]
enum FrozenEvent {
    #[serde(rename = "startup")]
    Startup {
        dataset: String,
        root: Artifact,
        resources: ResourceReceipt,
        build: FineBuildReceipt,
        truth_opened: bool,
        wall_ns: u64,
        process_cpu_ns: i64,
    },
    #[serde(rename = "fine_plan")]
    Plan {
        dataset: String,
        ordinal: usize,
        plan: FrozenPlan,
        truth_opened: bool,
        wall_ns: u64,
        process_cpu_ns: i64,
    },
}
struct Protocol {
    rows: usize,
    dimensions: usize,
    training: usize,
    held: usize,
    prefix_bytes: usize,
    prefix_sha: String,
    seal_sha: String,
}
#[derive(Debug, Serialize)]
struct WorkCeiling {
    nomination: u64,
    source_setup: u64,
    fitting: u64,
    held: u64,
    authentication_and_serialization: u64,
    construction: u64,
    replay: u64,
    per_plan: u64,
}
#[derive(Debug, Serialize)]
struct OutputCeiling {
    selection_per_panel: usize,
    map_per_panel: usize,
    held_line: usize,
    replay_line: usize,
    prefix: usize,
    terminal: usize,
    total: usize,
    serialization_peak_extra_bytes: usize,
}
fn artifact_output_bound(path: &Path) -> Result<usize> {
    // Exact upper bound for JSON string escaping, plus descriptor keys,
    // SHA256, a maximum usize decimal byte count and punctuation.
    let text = path.to_str().ok_or("co-selection output UTF8 path")?;
    let escaped = text.bytes().try_fold(2usize, |n, b| {
        n.checked_add(match b {
            0..=31 => 6,
            b'"' | b'\\' => 2,
            _ => 1,
        })
        .ok_or("co-selection escaped path size overflow")
    })?;
    sum(&[escaped, 160])
}
fn preflight_output(c: &Config, p: &Protocol, output: &Path) -> Result<OutputCeiling> {
    let output_pin = artifact_output_bound(&output.with_extension("relaion.selections.bin"))?;
    let root_pin = c
        .panels
        .iter()
        .map(|p| artifact_output_bound(&p.root.path))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .ok_or("co-selection panels")?;
    let groups = p.rows.div_ceil(GROUP);
    let selection_per_panel = usize::try_from(work_sum(&[
        208,
        work_product(sum(&[p.training, p.held])? as u64, 52 + 4 * 1024)?,
    ])?)?;
    // Per group: 67 digest bytes, two <=6-byte ordinals and <=128-byte
    // accepted-swap record. Four bounded root documents cover descriptors,
    // scalar metadata, objects and source/config/work identity fields.
    let map_per_panel = sum(&[
        4 * ROOT_BYTES,
        groups.checked_mul(256).ok_or("co-selection map size")?,
    ])?;
    // Each cover has <=32 ranges (even the complete J>K witness has <=2
    // objects in this geometry), <=80 JSON bytes/range. Group categories are
    // disjoint, so all three replay arrays together contain <=G ordinals.
    let held_line = sum(&[2048, 2 * 32 * 80, output_pin, output_pin])?;
    let replay_line = sum(&[
        4096,
        2 * 32 * 80,
        groups.checked_mul(6).ok_or("co-selection group size")?,
        1024 * (20 + 6),
        output_pin,
        output_pin,
        root_pin,
    ])?;
    let total = usize::try_from(work_sum(&[
        work_product(2, selection_per_panel as u64)?,
        work_product(2, map_per_panel as u64)?,
        work_product(work_product(2, p.held as u64)?, held_line as u64)?,
        work_product(128, replay_line as u64)?,
        p.prefix_bytes as u64,
        TERMINAL_RESERVE as u64,
    ])?)?;
    require(
        total <= c.caps.output_bytes,
        "co-selection full frozen batch output ceiling",
    )?;
    let serialization_peak_extra_bytes = usize::try_from(work_product(
        17,
        map_per_panel
            .max(held_line)
            .max(replay_line)
            .max(TERMINAL_RESERVE) as u64,
    )?)?
    .max(selection_per_panel);
    Ok(OutputCeiling {
        selection_per_panel,
        map_per_panel,
        held_line,
        replay_line,
        prefix: p.prefix_bytes,
        terminal: TERMINAL_RESERVE,
        total,
        serialization_peak_extra_bytes,
    })
}
fn work_sum(values: &[u64]) -> Result<u64> {
    values.iter().try_fold(0u64, |sum, &n| {
        sum.checked_add(n)
            .ok_or_else(|| "co-selection work ceiling overflow".into())
    })
}
fn work_product(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| "co-selection work ceiling overflow".into())
}
fn preflight_work(c: &Config, p: &Protocol) -> Result<WorkCeiling> {
    // Algebraic maxima of the loops below, evaluated with checked arithmetic.
    // I=A*min(1024,G); every swap union <= A, not 2A. Every plan tries <=1024
    // missing anchor groups; selected groups <=min(G,2048). Two extra covers
    // allow the mandatory check and its complete over-budget witness.
    let n = p.rows as u64;
    let g = p.rows.div_ceil(GROUP) as u64;
    let capacity = block_capacity(p.dimensions) as u64;
    let b = g.div_ceil(capacity);
    let a = p.training as u64;
    let h = p.held as u64;
    let anchors = work_sum(&[a, h])?;
    let incidence = work_product(a, g.min(1024))?;
    let selected = g.min(2048);
    let cover = work_sum(&[
        work_product(3, g)?,
        work_product(4, selected)?,
        sort_charge(selected as usize),
    ])?;
    let per_plan = work_sum(&[
        n,
        work_product(8, g)?,
        anchors,
        131072,
        incidence,
        a,
        work_product(1026, cover)?,
        work_product(32, g)?,
        work_product(2, sort_charge(1024))?,
    ])?;
    let one_fit = work_sum(&[
        work_product(20, g)?,
        work_product(4, work_product(a, b)?)?,
        work_product(3, incidence)?,
        work_product(b.saturating_sub(1), work_sum(&[incidence, g])?)?,
        work_product(g, sort_charge(b as usize))?,
        work_product(
            work_product(work_product(g, b.saturating_sub(1).min(8))?, capacity)?,
            work_sum(&[a, 1])?,
        )?,
        work_product(
            g,
            work_sum(&[work_product(2, a)?, work_product(2, capacity)?])?,
        )?,
        work_product(b, sort_charge(capacity as usize))?,
    ])?;
    let nomination = work_product(
        work_product(2, anchors)?,
        nomination_work(65536, p.rows.min(65536), p.dimensions, p.rows),
    )?;
    let mut source_setup = 0;
    for panel in &c.panels {
        source_setup = work_sum(&[
            source_setup,
            work_product(n, (1024 + ANCHOR_DOMAIN.len() + 40 + 4) as u64)?,
            sort_charge(p.rows),
            work_product(panel.pq.bytes as u64, 4)?,
            work_product(panel.graph.bytes as u64, 66)?,
            work_product(
                anchors,
                work_sum(&[n, g, 4096, work_product(p.dimensions as u64, 4)?])?,
            )?,
        ])?;
    }
    // Byte units include bounded hashing/parsing/encoding traversals; the
    // 5*output covers admitted buffer initialization, map/plan conversion,
    // encoding/hashing and failed writes.
    // Compiled source hashing is charged separately by its exact byte length.
    let authentication_and_serialization = work_sum(&[
        c.caps.source_auth_bytes as u64,
        work_product(5, c.caps.output_bytes as u64)?,
        work_product(8, source_code_bytes() as u64)?,
    ])?;
    let fitting = work_product(2, one_fit)?;
    let held = work_product(work_product(2, h)?, per_plan)?;
    let construction = work_sum(&[
        nomination,
        source_setup,
        fitting,
        held,
        authentication_and_serialization,
    ])?;
    let replay = work_sum(&[
        work_product(128, work_sum(&[per_plan, 65536 + ROOT_BYTES as u64])?)?,
        authentication_and_serialization,
    ])?;
    require(
        construction <= c.caps.construction_operations && replay <= c.caps.replay_operations,
        "co-selection full frozen batch exceeds declared phase operation ceilings",
    )?;
    Ok(WorkCeiling {
        nomination,
        source_setup,
        fitting,
        held,
        authentication_and_serialization,
        construction,
        replay,
        per_plan,
    })
}
impl Protocol {
    fn frozen() -> Self {
        Self {
            rows: 100_000,
            dimensions: 768,
            training: TRAIN,
            held: HELD,
            prefix_bytes: PREFIX_BYTES,
            prefix_sha: PREFIX_SHA.into(),
            seal_sha: SEAL_SHA.into(),
        }
    }
    fn validate(&self, c: &Config) -> Result<()> {
        c.caps.validate()?;
        require(
            c.schema == CONFIG_SCHEMA
                && c.panels[0].dataset == "relaion"
                && c.panels[1].dataset == "cohere"
                && c.prefix.bytes == self.prefix_bytes
                && c.prefix.sha256 == self.prefix_sha
                && c.original_seal.sha256 == self.seal_sha
                && c.original_seal.bytes <= 4096
                && self.prefix_bytes <= 2 * 1024 * 1024
                && c.prior_reads.operations <= 32
                && c.prior_reads.bytes <= TOTAL_BYTES,
            "co-selection frozen config/prefix/seal/total budget",
        )?;
        for p in &c.panels {
            require(
                p.identity.rows == self.rows
                    && p.identity.dimensions == self.dimensions
                    && p.root.bytes <= ROOT_BYTES
                    && p.canonical.bytes == self.rows * (8 + 4 * self.dimensions)
                    && p.source_order.bytes == self.rows * 8
                    && p.fine_order.bytes == self.rows * 8
                    && p.graph.bytes <= self.rows * 512
                    && p.pq.bytes
                        == 24 + 64 * 256 * self.dimensions.div_ceil(64) * 4 + self.rows * 64,
                "co-selection panel pins/caps",
            )?;
            for a in [
                &p.root,
                &p.canonical,
                &p.source_order,
                &p.fine_order,
                &p.graph,
                &p.pq,
            ] {
                digest(&a.sha256)?;
            }
        }
        Ok(())
    }
}

impl CoSelectionLayout {
    fn replay_frozen_plans(
        body: &[u8],
        c: &Config,
        layouts: &[Self],
        map_pins: &[Artifact],
        selection_pins: &[Artifact],
        output_ceiling: &OutputCeiling,
        outputs: &mut Outputs,
        work: &mut WorkBudget,
    ) -> Result<(Artifact, [usize; 2])> {
        require(
            body.last() == Some(&b'\n'),
            "co-selection complete metadata prefix",
        )?;
        let mut lines = std::str::from_utf8(body)?.lines();
        for p in &c.panels {
            let line = lines.next().ok_or("co-selection missing startup")?;
            require(line.len() <= ROOT_BYTES, "co-selection startup cap")?;
            let event: FrozenEvent = serde_json::from_str(line)?;
            let FrozenEvent::Startup {
                dataset,
                root,
                resources,
                build,
                truth_opened,
                wall_ns,
                process_cpu_ns,
            } = event
            else {
                return Err("co-selection startup shape".into());
            };
            let _ = (resources, wall_ns);
            require(
                dataset == p.dataset
                    && same(&root, &p.root)
                    && !truth_opened
                    && build.source_rows == p.identity.rows
                    && process_cpu_ns >= 0,
                "co-selection frozen startup bindings",
            )?;
        }
        let (path, mut plans) = outputs.companion("plans.jsonl")?;
        let mut h = Sha256::new();
        let mut bytes = 0;
        let mut passed = [0; 2];
        for (panel, p) in c.panels.iter().enumerate() {
            for expected in 0..64 {
                let line = lines
                    .next()
                    .ok_or("co-selection incomplete128 metadata prefix")?;
                require(line.len() <= ROOT_BYTES, "co-selection plan decoder cap")?;
                work.tick(line.len() as u64)?;
                let event: FrozenEvent = serde_json::from_str(line)?;
                let FrozenEvent::Plan {
                    dataset,
                    ordinal,
                    plan,
                    truth_opened,
                    wall_ns,
                    process_cpu_ns,
                } = event
                else {
                    return Err("co-selection frozen plan shape".into());
                };
                let _ = wall_ns;
                digest(&plan.query_sha256)?;
                let unique = plan.nominees.iter().copied().collect::<BTreeSet<_>>();
                require(
                    dataset == p.dataset
                        && ordinal == expected
                        && !truth_opened
                        && process_cpu_ns >= 0
                        && plan.root_sha256 == p.root.sha256
                        && plan.revision == 0
                        && plan.mutation_sha256.is_empty()
                        && !unique.is_empty()
                        && plan.nominees.len() <= 1024
                        && unique.len() == plan.nominees.len()
                        && unique.last().is_some_and(|n| *n < p.identity.rows)
                        && plan.actual_shortlist_rows == plan.nominees.len()
                        && plan.converged != plan.exhausted
                        && plan.evaluations <= 65536
                        && plan.base_visits <= p.identity.rows.min(plan.evaluations),
                    "co-selection frozen identity/ordinal/query/nominee binding",
                )?;
                let n = p.identity.rows;
                let d = p.identity.dimensions;
                let fine = unique.iter().map(|n| n / GROUP).collect();
                let coarse = unique.iter().map(|n| n / 256).collect();
                work.tick(65536)?;
                let (ranges, oldbytes) = cover_pages(&fine, n, d + 12, GROUP, 256)
                    .map_err(|e| format!("co-selection historical fine cover: {e:?}"))?;
                let (old, coarsebytes) = cover_pages(&coarse, n, d + 12, 256, 256)
                    .map_err(|e| format!("co-selection historical coarse cover: {e:?}"))?;
                require(
                    plan.ranges == ranges
                        && plan.planned_bytes == oldbytes
                        && plan.feasible == (oldbytes <= TOTAL_BYTES)
                        && plan.old_page_gets == old.len()
                        && plan.old_page_bytes == coarsebytes,
                    "co-selection historical exact nominee cover",
                )?;
                let result = layouts[panel].plan(&plan.nominees, c.prior_reads, work)?;
                passed[panel] += usize::from(result.fits);
                let mut nominee_hasher = Sha256::new();
                for &n in &plan.nominees {
                    nominee_hasher.update((n as u64).to_le_bytes());
                }
                let nominee_sha = format!("{:x}", nominee_hasher.finalize());
                work.serialization(output_ceiling.replay_line)?;
                let row = json_bytes(
                    &json!({"phase":"co_selection_plan","dataset":dataset,"ordinal":ordinal,
                "root":p.root,"map":map_pins[panel],"source_selections":selection_pins[panel],
                "query_sha256":plan.query_sha256,"original_nominees":plan.nominees,
                "nominee_sha256_ordered_le_u64":nominee_sha,
                "plan":result,"truth_opened":false,"request_vectors_opened":false,"sq8_bodies_opened":false}),
                    output_ceiling.replay_line,
                    true,
                )?;
                work.tick(work_product(4, row.len() as u64)?)?;
                outputs.write(&mut plans, &row)?;
                h.update(&row);
                bytes += row.len();
                // A valid budget rejection does NOT terminate replay. Subsequent
                // malformed input must still invalidate the entire execution.
            }
        }
        require(
            lines.next().is_none(),
            "co-selection prefix has exactly128 plans",
        )?;
        outputs.sync(&plans)?;
        outputs.published_bytes += bytes;
        Ok((
            Artifact {
                path,
                bytes,
                sha256: format!("{:x}", h.finalize()),
            },
            passed,
        ))
    }
}
fn run(
    c: &Config,
    config_sha: &str,
    protocol: &Protocol,
    outputs: &mut Outputs,
    work: &mut WorkBudget,
    before_prefix: &mut impl FnMut(&[Artifact], &[Artifact]) -> Result<()>,
) -> Result<Value> {
    protocol.validate(c)?;
    outputs.cap = c.caps.output_bytes;
    let ceiling = preflight_work(c, protocol)?;
    let output_ceiling = preflight_output(c, protocol, &outputs.path)?;
    work.memory(&[FIXED_MEMORY])?;
    let seal_body = read_pinned(&c.original_seal, 4096, false, work)?;
    let seal: OriginalSeal = serde_json::from_slice(&seal_body)?;
    digest(&seal.config_sha256)?;
    digest(&seal.source_identity_sha256)?;
    require(
        seal.schema == "borsuk-fine-sq8-seal-v1"
            && !seal.truth_opened
            && seal.plans_per_panel == 64
            && seal.prefix_bytes == c.prefix.bytes
            && seal.prefix_sha256 == c.prefix.sha256,
        "co-selection original seal binding",
    )?;
    for (p, s) in c.panels.iter().zip(&seal.panels) {
        digest(&s.requests.sha256)?;
        require(
            p.dataset == s.dataset && same(&p.root, &s.root),
            "co-selection sealed panel roots",
        )?;
    }
    let mut layouts = reserved(2)?;
    let mut maps = reserved(2)?;
    let mut selections = reserved(2)?;
    for panel in &c.panels {
        let source =
            SourceSelections::generate_counts(panel, protocol.training, protocol.held, work)?;
        let layout = CoSelectionLayout::fit(&source, work)?;
        // Retain precisely the shared selections/IDs plus maps, before planning
        // held diagnostics or admitting the next panel's independent index.
        let retained = sum(&[
            source.capacity_bytes,
            layout.old_to_new.capacity() * 4,
            layout.new_to_old.capacity() * 4,
            layout.swaps.capacity() * std::mem::size_of::<Swap>(),
        ])?;
        work.retained_bytes = sum(&[work.retained_bytes, retained])?;
        work.memory(&[FIXED_MEMORY])?;
        let selection = outputs.publish(
            &format!("{}.selections.bin", panel.dataset),
            &selection_bytes(&source, work)?,
        )?;
        work.tick(source_code_bytes() as u64)?;
        work.reserve_work(work_sum(&[
            work_product(5, c.caps.output_bytes as u64)?,
            source_code_bytes() as u64,
        ])?)?;
        let map_body = map_bytes(
            &source,
            &layout,
            &selection,
            config_sha,
            work,
            &ceiling,
            &output_ceiling,
        )?;
        work.tick(work_product(4, map_body.len() as u64)?)?;
        let map = outputs.publish(&format!("{}.map.json", panel.dataset), &map_body)?;
        selections.push(selection);
        maps.push(map);
        layouts.push(layout);
    }
    // Both maps are now immutable and fsynced. Held source queries are purely
    // descriptive and cannot participate in fit, destinations or dictionary.
    let (held_path, mut held_file) = outputs.companion("held.jsonl")?;
    let mut held_sha = Sha256::new();
    let mut held_bytes = 0;
    for (panel, layout) in layouts.iter().enumerate() {
        for (held_ordinal, anchor) in layout.anchors.iter().skip(layout.training).enumerate() {
            let mut nominees = reserved(anchor.nominees.len())?;
            nominees.extend(anchor.nominees.iter().map(|v| *v as usize));
            let result = layout.plan(&nominees, c.prior_reads, work)?;
            let blocks = result
                .mandatory_groups
                .iter()
                .map(|&g| layout.old_to_new[g as usize] as usize / layout.capacity)
                .collect::<BTreeSet<_>>();
            let objects = blocks
                .iter()
                .map(|b| b / OBJECT_BLOCKS)
                .collect::<BTreeSet<_>>();
            work.serialization(output_ceiling.held_line)?;
            let line = json_bytes(
                &json!({"dataset":c.panels[panel].dataset,"held_ordinal":held_ordinal,
                "logical_id":anchor.logical_id,"canonical_ordinal":anchor.canonical_ordinal,"fine_ordinal":anchor.original_ordinal,
                "map":maps[panel],"source_selections":selections[panel],"dictionary_overlap":result.anchor,
                "mandatory_blocks":blocks.len(),"mandatory_objects":objects.len(),"mandatory_cover":result.mandatory_cover,
                "expanded_ranges":result.ranges,"expanded_bytes":result.payload_bytes,"fits":result.fits,
                "counted_operations":result.counted_operations,"used_for_fitting":false,"policy_tuning":false}),
                output_ceiling.held_line,
                true,
            )?;
            work.tick(work_sum(&[
                work_product(4, line.len() as u64)?,
                sort_charge(result.mandatory_groups.len()) * 2,
            ])?)?;
            outputs.write(&mut held_file, &line)?;
            held_sha.update(&line);
            held_bytes += line.len();
        }
    }
    outputs.sync(&held_file)?;
    outputs.published_bytes += held_bytes;
    let held_pin = Artifact {
        path: held_path,
        bytes: held_bytes,
        sha256: format!("{:x}", held_sha.finalize()),
    };
    // Both map and actual selection pins have file+directory fsync receipts
    // before opening (not merely parsing) the request nomination prefix.
    before_prefix(&maps, &selections)?;
    require(
        work.construction_operations <= ceiling.construction,
        "co-selection construction work ceiling invariant",
    )?;
    work.begin_replay()?;
    let prefix = read_pinned(&c.prefix, 2 * 1024 * 1024, true, work)?;
    work.tick(work_product(2, prefix.len() as u64)?)?;
    let prefix_pin = outputs.publish("prefix.jsonl", &prefix)?;
    let (plans, passed) = CoSelectionLayout::replay_frozen_plans(
        &prefix,
        c,
        &layouts,
        &maps,
        &selections,
        &output_ceiling,
        outputs,
        work,
    )?;
    work.tick(work_sum(&[
        source_code_bytes() as u64,
        (4 * TERMINAL_RESERVE) as u64,
    ])?)?;
    work.serialization(TERMINAL_RESERVE)?;
    require(
        work.replay_operations <= ceiling.replay,
        "co-selection replay work ceiling invariant",
    )?;
    Ok(terminal(
        config_sha,
        if passed == [64, 64] {
            "SURVIVED_NECESSARY_LOCALITY"
        } else {
            "REJECT"
        },
        true,
        128,
        json!({"maps":maps,"source_selections":selections,"nomination_prefix":prefix_pin,"plans":plans,"per_panel_fits":passed,"held_diagnostics":held_pin,
            "original_seal":c.original_seal,"original_trace_source_identity_sha256":seal.source_identity_sha256,
            "caps":c.caps,"prior_reads":c.prior_reads,"counted_operations":work.operations,"source_auth_bytes":work.source_bytes,
            "construction_operations":work.construction_operations,"replay_operations":work.replay_operations,"work_preflight":ceiling,"output_preflight":output_ceiling,
            "modeled_peak_owned_bytes":work.peak_modeled_bytes,"retained_capacity_bytes":work.retained_bytes,
            "wall_ms":work.start.elapsed().as_millis(),"delta_bytes":0,"additional_attempts":0,
            "operation_units":"authenticated bytes; sort comparison bounds; incidence/count/cover scans; PQ64 score+heap bound, 256 base edges per visit, table coordinates",
            "scope":"metadata-only virtual layout; no payload rewrite, SQ8 ranking or quality qualification"}),
    ))
}
/// Strict CONFIG SHA NEW_OUTPUT CLI. Production geometry and the already
/// frozen metadata prefix cannot be overridden through serialized config.
pub fn check_co_selection_layout(
    config_path: &Path,
    config_sha: &str,
    output: &Path,
) -> Result<()> {
    check_with_protocol(
        config_path,
        config_sha,
        output,
        &Protocol::frozen(),
        &mut |_, _| Ok(()),
        None,
    )
}
fn check_with_protocol(
    config_path: &Path,
    config_sha: &str,
    output: &Path,
    protocol: &Protocol,
    before_prefix: &mut impl FnMut(&[Artifact], &[Artifact]) -> Result<()>,
    fail_sync: Option<usize>,
) -> Result<()> {
    let mut outputs = Outputs::create(output)?;
    #[cfg(test)]
    {
        outputs.fail_sync_at = fail_sync;
    }
    #[cfg(not(test))]
    let _ = fail_sync;
    let mut ledger = None;
    let result = (|| {
        digest(config_sha)?;
        let mut file = secure_open(config_path, rustix::fs::OFlags::RDONLY)?;
        let metadata = file.metadata()?;
        require(
            metadata.is_file() && metadata.len() > 0 && metadata.len() <= ROOT_BYTES as u64,
            "co-selection config regular/cap",
        )?;
        let mut body = filled(metadata.len() as usize, 0u8)?;
        file.read_exact(&mut body)?;
        require(
            file.read(&mut [0])? == 0 && hash(&body) == config_sha,
            "co-selection config EOF/SHA256",
        )?;
        let config: Config = serde_json::from_slice(&body)?;
        protocol.validate(&config)?;
        let worker_ok = crate::configured_cpu_threads() == 1;
        #[cfg(test)]
        let worker_ok = worker_ok || protocol.rows != 100_000;
        require(worker_ok, "co-selection requires BORSUK_CPU_THREADS=1")?;
        ledger = Some(WorkBudget::new(config.caps.clone())?);
        let work = ledger.as_mut().ok_or("co-selection work ledger")?;
        work.source(body.len())?;
        let report = run(
            &config,
            config_sha,
            protocol,
            &mut outputs,
            work,
            before_prefix,
        )?;
        outputs.finish(&report)?;
        work.tick(0)
    })();
    if let Err(error) = &result {
        outputs.invalidate(config_sha, error, ledger.as_ref());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn artifact(path: &Path, body: &[u8]) -> Artifact {
        fs::write(path, body).unwrap();
        Artifact {
            path: path.into(),
            bytes: body.len(),
            sha256: hash(body),
        }
    }
    fn absent(path: &Path, bytes: usize) -> Artifact {
        Artifact {
            path: path.into(),
            bytes,
            sha256: hash(b"intentionally inaccessible"),
        }
    }
    fn original(canonical: Artifact, order: Artifact) -> BuildConfig {
        let missing = absent(&canonical.path.with_extension("forbidden"), 1);
        BuildConfig {
            schema: crate::hierarchical_semantic_cells::BUILD_SCHEMA.into(),
            generation: missing.clone(),
            plane: missing.clone(),
            canonical,
            order,
            records: missing.clone(),
            mean: missing.clone(),
            sq8: missing,
            cell_rows: 16,
            sample_rows: 16,
            max_depth: 24,
            max_build_payload_bytes: MEMORY,
            max_output_bytes: OUTPUT_BYTES,
        }
    }
    fn pure_source(n: usize, d: usize, edges: &[Vec<usize>]) -> SourceSelections {
        let missing = absent(Path::new("/co-selection-pure-fixture"), 1);
        let identity = PqGraphIdentity {
            generation: 1,
            rows: n,
            dimensions: d,
            source: [1; 32],
            layout: [2; 32],
            pq: [3; 32],
        };
        let panel = Panel {
            dataset: "synthetic".into(),
            root: missing.clone(),
            identity: identity.clone(),
            canonical: missing.clone(),
            source_order: missing.clone(),
            fine_order: missing.clone(),
            pq: missing.clone(),
            graph: missing.clone(),
        };
        let sources = CoSelectionSources {
            identity,
            original: original(missing.clone(), missing.clone()),
            primary_root: missing.clone(),
            pq: missing.clone(),
            graph: missing.clone(),
            order: missing.clone(),
            groups: missing.clone(),
            records: missing,
        };
        let anchors = edges
            .iter()
            .enumerate()
            .map(|(a, edge)| Anchor {
                logical_id: a as i64,
                original_ordinal: a as u32,
                canonical_ordinal: a as u32,
                query_sha256: [0; 32],
                nominees: edge.iter().map(|g| (g * GROUP) as u32).collect(),
                groups: edge
                    .iter()
                    .enumerate()
                    .map(|(p, &g)| (g as u32, p as u16))
                    .collect(),
            })
            .collect::<Vec<_>>();
        SourceSelections {
            panel,
            sources,
            anchors: Arc::new(anchors),
            training: edges.len(),
            ids: Arc::new((0..n as i64).rev().collect()),
            group_hashes: vec![0; n.div_ceil(GROUP) * 32],
            capacity_bytes: 1024 * 1024,
            nomination_resources: ResourceReceipt::default(),
        }
    }
    fn fixture_panel(dir: &Path, dataset: &str) -> Panel {
        fs::create_dir(dir).unwrap();
        let n = 35usize;
        let d = 2usize;
        let ids = (0..n).rev().collect::<Vec<_>>();
        let source_order = artifact(
            &dir.join("source-order.bin"),
            &(0..n as u64).flat_map(u64::to_le_bytes).collect::<Vec<_>>(),
        );
        let mut canonical = Vec::new();
        for id in 0..n {
            canonical.extend_from_slice(&(id as i64).to_le_bytes());
            canonical.extend_from_slice(&1f32.to_le_bytes());
            canonical.extend_from_slice(&0f32.to_le_bytes());
        }
        let canonical = artifact(&dir.join("canonical.bin"), &canonical);
        let original = original(canonical.clone(), source_order.clone());
        let primary = artifact(
            &dir.join("primary.json"),
            &serde_json::to_vec(&json!({"input":original,"rows":n,"dimensions":d})).unwrap(),
        );
        let fine_order = artifact(
            &dir.join("order.bin"),
            &ids.iter()
                .flat_map(|&i| (i as u64).to_le_bytes())
                .collect::<Vec<_>>(),
        );
        // Real PQ64 encoding, decoded by FineSq8Index::open_remote. Its two
        // nonempty subspaces reconstruct [1,0] for every actual source row.
        let mut books = vec![0f32; 64 * 256];
        for w in 0..256 {
            books[31 * 256 + w] = 1.;
        }
        let mut pq_body = b"BORSPQ01".to_vec();
        pq_body.extend((n as u64).to_le_bytes());
        pq_body.extend((d as u64).to_le_bytes());
        pq_body.extend(books.iter().flat_map(|v| v.to_le_bytes()));
        pq_body.extend(vec![0u8; n * 64]);
        let pq = artifact(&dir.join("pq.bin"), &pq_body);
        let mut records = Vec::new();
        for &id in &ids {
            records.extend((id as i64).to_le_bytes());
            records.extend(1f32.to_le_bytes());
            records.extend([1u8, 0]);
        }
        let records_pin = Artifact {
            path: dir.join("records.bin"),
            bytes: records.len(),
            sha256: hash(&records),
        };
        let groups = artifact(
            &dir.join("groups.bin"),
            &records
                .chunks(16 * (d + 12))
                .flat_map(|r| Sha256::digest(r).to_vec())
                .collect::<Vec<_>>(),
        );
        let layout = Sha256::digest(
            serde_json::to_vec(&(
                primary.sha256.as_str(),
                records_pin.sha256.as_str(),
                fine_order.sha256.as_str(),
                vec![0u32, 0],
                vec![1f32.to_bits(), 1f32.to_bits()],
            ))
            .unwrap(),
        )
        .into();
        let identity = PqGraphIdentity {
            generation: 1,
            rows: n,
            dimensions: d,
            source: digest(&canonical.sha256).unwrap(),
            layout,
            pq: digest(&pq.sha256).unwrap(),
        };
        let mut graph_body = b"BORSVG02".to_vec();
        for word in [1, n as u64, d as u64] {
            graph_body.extend(word.to_le_bytes());
        }
        for h in [identity.source, identity.layout, identity.pq] {
            graph_body.extend(h);
        }
        graph_body.extend(0u32.to_le_bytes());
        for i in 0..n {
            graph_body.push(1);
            graph_body.extend(1u16.to_le_bytes());
            graph_body.extend((((i + 1) % n) as u32).to_le_bytes());
        }
        let graph = artifact(&dir.join("graph.bin"), &graph_body);
        let root=artifact(&dir.join("manifest.json"),&serde_json::to_vec(&json!({"schema":crate::fine_sq8_groups::SCHEMA,
            "primary_root":primary,"original":original,"identity":identity,"low":[0.,0.],"step":[1.,1.],"pq":pq,"graph":graph,
            "records":records_pin,"groups":groups,"order":fine_order,"build":FineBuildReceipt {modeled_build_payload_bytes:1,
                modeled_output_and_staging_bytes:1,actual_graph_capacity_bytes:1,construction_graph_capacity_bytes:1,
                actual_pq_capacity_bytes:1,source_rows:n,sq8_body_bytes:n*(d+12)}})).unwrap());
        Panel {
            dataset: dataset.into(),
            root,
            identity,
            canonical,
            source_order,
            fine_order,
            pq,
            graph,
        }
    }
    fn fixture(dir: &Path) -> (Config, Protocol) {
        let panels = [
            fixture_panel(&dir.join("relaion"), "relaion"),
            fixture_panel(&dir.join("cohere"), "cohere"),
        ];
        let mut body = Vec::new();
        for p in &panels {
            let root: Value = serde_json::from_slice(&fs::read(&p.root.path).unwrap()).unwrap();
            body.extend(serde_json::to_vec(&json!({"phase":"startup","dataset":p.dataset,"root":p.root,"resources":ResourceReceipt::default(),
                "build":root["build"],"truth_opened":false,"wall_ns":1,"process_cpu_ns":1})).unwrap());
            body.push(b'\n');
        }
        for p in &panels {
            for ordinal in 0..64 {
                let nominees = vec![ordinal % 35];
                let selected = nominees.iter().map(|n| n / 16).collect();
                let coarse = nominees.iter().map(|n| n / 256).collect();
                let (ranges, bytes) = cover_pages(&selected, 35, 14, 16, 256).unwrap();
                let (old, oldbytes) = cover_pages(&coarse, 35, 14, 256, 256).unwrap();
                body.extend(serde_json::to_vec(&json!({"phase":"fine_plan","dataset":p.dataset,"ordinal":ordinal,"truth_opened":false,
                "wall_ns":1,"process_cpu_ns":1,"plan":{"root_sha256":p.root.sha256,"query_sha256":hash(&[ordinal as u8]),
                "revision":0,"mutation_sha256":"","nominees":nominees,"ranges":ranges,"planned_bytes":bytes,"feasible":true,
                "exhausted":false,"converged":true,"actual_shortlist_rows":1,"evaluations":35,"base_visits":35,
                "old_page_gets":old.len(),"old_page_bytes":oldbytes}})).unwrap());
                body.push(b'\n');
            }
        }
        let prefix = artifact(&dir.join("frozen.jsonl"), &body);
        let original_seal=artifact(&dir.join("seal.json"),&serde_json::to_vec(&json!({"schema":"borsuk-fine-sq8-seal-v1",
            "config_sha256":hash(b"original config"),"source_identity_sha256":hash(b"original source"),"prefix_bytes":prefix.bytes,
            "prefix_sha256":prefix.sha256,"plans_per_panel":64,"truth_opened":false,
            "panels":panels.iter().map(|p|json!({"dataset":p.dataset,"root":p.root,"requests":absent(&dir.join("forbidden-requests"),1)})).collect::<Vec<_>>()})).unwrap());
        let protocol = Protocol {
            rows: 35,
            dimensions: 2,
            training: 4,
            held: 2,
            prefix_bytes: prefix.bytes,
            prefix_sha: prefix.sha256.clone(),
            seal_sha: original_seal.sha256.clone(),
        };
        (
            Config {
                schema: CONFIG_SCHEMA.into(),
                panels,
                original_seal,
                prefix,
                caps: caps(),
                prior_reads: ReadBudget {
                    operations: 0,
                    bytes: 0,
                },
            },
            protocol,
        )
    }
    fn run_fixture(
        c: &Config,
        p: &Protocol,
        output: &Path,
        before: &mut impl FnMut(&[Artifact], &[Artifact]) -> Result<()>,
        fail_sync: Option<usize>,
    ) -> Result<()> {
        let config = artifact(
            &output.with_extension("config.json"),
            &serde_json::to_vec(c).unwrap(),
        );
        check_with_protocol(&config.path, &config.sha256, output, p, before, fail_sync)
    }

    fn caps() -> Caps {
        Caps {
            memory_bytes: MEMORY,
            caller_pinned_bytes: 0,
            source_auth_bytes: SOURCE_BYTES,
            construction_operations: CONSTRUCTION_OPERATIONS,
            replay_operations: REPLAY_OPERATIONS,
            output_bytes: OUTPUT_BYTES,
            deadline_seconds: 300,
            cpu_threads: 1,
            swap_bytes: 0,
        }
    }
    fn work() -> WorkBudget {
        WorkBudget::new(caps()).unwrap()
    }

    // Independent interval DP: partitions each ordered object selection, then
    // knapsacks request allocations across objects. Does not use gap sorting.
    fn interval_dp(objects: &[Vec<Range<usize>>], requests: usize) -> Option<usize> {
        let mut global = vec![usize::MAX; requests + 1];
        global[0] = 0;
        for selected in objects.iter().filter(|v| !v.is_empty()) {
            let n = selected.len();
            let mut dp = vec![vec![usize::MAX; requests + 1]; n + 1];
            dp[0][0] = 0;
            for end in 1..=n {
                for k in 1..=requests {
                    for start in 0..end {
                        if dp[start][k - 1] != usize::MAX {
                            dp[end][k] = dp[end][k].min(
                                dp[start][k - 1] + selected[end - 1].end - selected[start].start,
                            );
                        }
                    }
                }
            }
            let mut next = vec![usize::MAX; requests + 1];
            for total in 1..=requests {
                for here in 1..=total {
                    if global[total - here] != usize::MAX && dp[n][here] != usize::MAX {
                        next[total] = next[total].min(global[total - here] + dp[n][here]);
                    }
                }
            }
            global = next;
        }
        global.into_iter().min().filter(|n| *n != usize::MAX)
    }
    fn exhaustive_cover(objects: &[Vec<Range<usize>>], requests: usize) -> Option<usize> {
        let gaps = objects
            .iter()
            .map(|r| r.len().saturating_sub(1))
            .sum::<usize>();
        let mut best = None;
        for mask in 0usize..(1usize << gaps) {
            let mut bit = 0;
            let mut cost = 0;
            let mut used = 0;
            for object in objects.iter().filter(|r| !r.is_empty()) {
                let mut start = object[0].start;
                used += 1;
                for pair in object.windows(2) {
                    if mask & (1 << bit) != 0 {
                        cost += pair[0].end - start;
                        start = pair[1].start;
                        used += 1;
                    }
                    bit += 1;
                }
                cost += object.last().unwrap().end - start;
            }
            if used <= requests {
                best = Some(best.map_or(cost, |old: usize| old.min(cost)));
            }
        }
        best
    }

    #[test]
    fn co_selection_cover_matches_independent_interval_dp_and_exhaustive_masks() {
        for mask in 1usize..256 {
            let mut objects = [Vec::new(), Vec::new()];
            for i in 0..8 {
                if mask & (1 << i) != 0 {
                    objects[i / 4].push((i % 4 * 3)..(i % 4 * 3 + 1));
                }
            }
            for k in 0..=8 {
                let ranges = objects
                    .iter()
                    .enumerate()
                    .flat_map(|(object, rs)| {
                        rs.iter().cloned().map(move |bytes| ObjectRange {
                            object: object as u32,
                            bytes,
                        })
                    })
                    .collect::<Vec<_>>();
                let cover = minimum_cover(&ranges, k, &mut work()).unwrap();
                assert_eq!(
                    cover
                        .as_ref()
                        .map(|v| v.iter().map(|r| r.bytes.len()).sum()),
                    interval_dp(&objects, k)
                );
                assert_eq!(interval_dp(&objects, k), exhaustive_cover(&objects, k));
            }
        }
    }
    #[test]
    fn co_selection_anchor_hash_domain_and_caps_are_frozen() {
        let source = [9; 32];
        let mut h = Sha256::new();
        h.update(b"borsuk-co-selection-anchor-v1\0");
        h.update(source);
        h.update(17u64.to_le_bytes());
        assert_eq!(anchor_key(&source, 17), <[u8; 32]>::from(h.finalize()));
        assert_eq!(
            anchor_key(&[0; 32], 1)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            "c80f8eaa165ccce90907df27ed0975522d81196a87700ed8ce25d8ca16ed7728"
        );
        assert_eq!(block_capacity(768), 42);
        assert_eq!(block_capacity(1), 64);
        let mut c = caps();
        c.source_auth_bytes += 1;
        assert!(WorkBudget::new(c).is_err());
    }

    // Independent full recomputation over an explicit assignment; no fitter
    // incidence, cached counts or contribution/delta routine is reused.
    fn objective_oracle(edges: &[Vec<usize>], assignment: &[usize], c: usize) -> u128 {
        let blocks = assignment.iter().max().unwrap() + 1;
        edges
            .iter()
            .map(|edge| {
                (0..blocks)
                    .map(|block| {
                        let count = edge.iter().filter(|&&g| assignment[g] == block).count();
                        if count == 0 {
                            0
                        } else {
                            2u128.pow((c + 1) as u32) - 2u128.pow((c + 1 - count) as u32)
                        }
                    })
                    .sum::<u128>()
            })
            .sum()
    }
    fn fit_oracle(edges: &[Vec<usize>], n: usize, c: usize) -> (Vec<usize>, Vec<Swap>, u128) {
        let mut assignment = (0..n.div_ceil(16)).map(|g| g / c).collect::<Vec<_>>();
        let mut swaps = Vec::new();
        for g in 0..n / 16 {
            let old = objective_oracle(edges, &assignment, c);
            let from = assignment[g];
            let blocks = *assignment.iter().max().unwrap() + 1;
            let mut destinations = Vec::new();
            for to in 0..blocks {
                if to == from
                    || !edges
                        .iter()
                        .any(|edge| edge.contains(&g) && edge.iter().any(|&p| assignment[p] == to))
                {
                    continue;
                }
                let mut moved = assignment.clone();
                moved[g] = to;
                destinations.push((old as i128 - objective_oracle(edges, &moved, c) as i128, to));
            }
            destinations.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            let mut candidates = Vec::new();
            for &(_, to) in destinations.iter().take(8) {
                for partner in 0..n / 16 {
                    if assignment[partner] != to {
                        continue;
                    }
                    let mut swapped = assignment.clone();
                    swapped.swap(g, partner);
                    let next = objective_oracle(edges, &swapped, c);
                    if next < old {
                        candidates.push((old - next, to, partner));
                    }
                }
            }
            candidates.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
            if let Some(&(gain, to, partner)) = candidates.first() {
                assignment.swap(g, partner);
                swaps.push(Swap {
                    group: g as u32,
                    destination: to as u32,
                    partner: partner as u32,
                    gain,
                });
            }
        }
        let objective = objective_oracle(edges, &assignment, c);
        (assignment, swaps, objective)
    }
    #[test]
    fn co_selection_objective_swaps_and_all_ties_match_independent_recomputation() {
        let edges = vec![
            vec![0, 43, 86],
            vec![1, 44],
            vec![0, 44],
            vec![2, 85],
            vec![87],
        ];
        let n = 16 * 87 + 3;
        let source = pure_source(n, 768, &edges);
        let layout = CoSelectionLayout::fit(&source, &mut work()).unwrap();
        let (assignment, swaps, objective) = fit_oracle(&edges, n, 42);
        assert!(!swaps.is_empty());
        assert_eq!(layout.swaps, swaps);
        assert_eq!(layout.objective_after, objective);
        assert!(layout.objective_after < layout.objective_before);
        for (g, &block) in assignment.iter().enumerate() {
            assert_eq!(layout.old_to_new[g] as usize / 42, block);
        }
        assert_eq!(layout.old_to_new[87], 87);
        let again = CoSelectionLayout::fit(&source, &mut work()).unwrap();
        assert_eq!(again.old_to_new, layout.old_to_new);
        // A single already co-located edge has only zero/negative-gain swaps.
        let source = pure_source(n, 768, &[vec![0, 1]]);
        assert!(
            CoSelectionLayout::fit(&source, &mut work())
                .unwrap()
                .swaps
                .is_empty()
        );
        let shared = FitState {
            c: 2,
            blocks: vec![vec![0], vec![1]],
            block_of: vec![0, 1],
            counts: vec![1, 1],
            incidence: Incidence {
                offsets: vec![0, 1, 2],
                anchors: vec![0, 0],
            },
        };
        assert_eq!(
            shared.gain(0, 1, &mut work()).unwrap(),
            0,
            "identical-incidence partners cancel"
        );
        let source = pure_source(35, 2, &[vec![0, 1, 2]]);
        let layout = CoSelectionLayout::fit(&source, &mut work()).unwrap();
        assert!(layout.objective_before > u64::MAX as u128);
        assert_eq!(
            serde_json::to_value(&layout).unwrap()["objective_before"],
            layout.objective_before.to_string()
        );
    }
    #[test]
    fn co_selection_bijection_short_tail_objects_nominees_and_total_budget() {
        let n = 90_003;
        let source = pure_source(n, 768, &[vec![0, 1, 5376]]);
        let layout = CoSelectionLayout::fit(&source, &mut work()).unwrap();
        let all = layout.new_to_old.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(all.len(), n.div_ceil(16));
        for (new, &old) in layout.new_to_old.iter().enumerate() {
            assert_eq!(layout.old_to_new[old as usize], new as u32);
        }
        let per = 42 * 128;
        let first = layout.new_to_old[per - 1] as usize;
        let second = layout.new_to_old[per] as usize;
        assert_eq!(layout.group_range(first).object, 0);
        assert_eq!(layout.group_range(first).bytes.end, 67_092_480);
        assert_eq!(layout.group_range(second).object, 1);
        assert_eq!(layout.group_range(second).bytes.start, 0);
        assert_eq!(layout.group_range(n / 16).bytes.len(), 3 * 780);
        let nominees = [first * 16, second * 16, n - 1];
        let rejected = layout
            .plan(
                &nominees,
                ReadBudget {
                    operations: 31,
                    bytes: 0,
                },
                &mut work(),
            )
            .unwrap();
        assert!(!rejected.fits);
        assert!(rejected.all_nominees_retained);
        assert!(!rejected.mandatory_cover.request_cap_feasible);
        assert_eq!(rejected.ranges.len(), 2);
        let plan = layout
            .plan(
                &nominees,
                ReadBudget {
                    operations: 0,
                    bytes: 0,
                },
                &mut work(),
            )
            .unwrap();
        assert!(plan.fits && plan.all_nominees_retained);
        assert_eq!(
            plan.logical_nominees,
            nominees
                .iter()
                .map(|&i| (n - 1 - i) as i64)
                .collect::<Vec<_>>()
        );
        assert!(plan.total_operations.unwrap() <= 32 && plan.total_bytes.unwrap() <= TOTAL_BYTES);
        assert_eq!(
            plan.payload_bytes.unwrap(),
            (plan.mandatory_rows + plan.expansion_rows + plan.bridge_rows) * 780
        );
        let mut broken = layout;
        broken.old_to_new[first] = broken.old_to_new[second];
        assert!(
            broken
                .plan(
                    &nominees,
                    ReadBudget {
                        operations: 0,
                        bytes: 0
                    },
                    &mut work()
                )
                .is_err()
        );
    }
    #[test]
    fn co_selection_expansion_uses_one_anchor_and_skips_unaffordable_groups() {
        // Equal Jaccard dictionaries deliberately in reverse logical-ID order.
        let mut source = pure_source(16 * 4, 768, &[vec![0, 1, 2], vec![0, 2, 3]]);
        let anchors = Arc::get_mut(&mut source.anchors).unwrap();
        anchors[0].logical_id = 7;
        anchors[1].logical_id = 3;
        let layout = CoSelectionLayout::fit(&source, &mut work()).unwrap();
        let plan = layout
            .plan(
                &[0],
                ReadBudget {
                    operations: 0,
                    bytes: TOTAL_BYTES - 32 * 780,
                },
                &mut work(),
            )
            .unwrap();
        assert!(plan.fits);
        assert_eq!(plan.anchor.as_ref().unwrap().logical_id, 3);
        assert_eq!(plan.expansion_groups, vec![2]);
        assert_eq!(plan.expansion_skipped, 1);
        assert_eq!(plan.expansion_rows, 16);
        assert_eq!(plan.bridge_rows, 0);
        let zero = layout
            .plan(
                &[3 * 16],
                ReadBudget {
                    operations: 32,
                    bytes: 0,
                },
                &mut work(),
            )
            .unwrap();
        assert!(!zero.fits);
        let no_overlap = CoSelectionLayout::fit(&pure_source(16 * 4, 768, &[vec![0]]), &mut work())
            .unwrap()
            .plan(
                &[3 * 16],
                ReadBudget {
                    operations: 0,
                    bytes: 0,
                },
                &mut work(),
            )
            .unwrap();
        assert!(no_overlap.anchor.is_none() && no_overlap.expansion_groups.is_empty());
        let gap = minimum_cover(
            &[
                ObjectRange {
                    object: 0,
                    bytes: 0..1,
                },
                ObjectRange {
                    object: 0,
                    bytes: 3..4,
                },
                ObjectRange {
                    object: 1,
                    bytes: 0..1,
                },
                ObjectRange {
                    object: 1,
                    bytes: 3..4,
                },
            ],
            3,
            &mut work(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            gap,
            vec![
                ObjectRange {
                    object: 0,
                    bytes: 0..1
                },
                ObjectRange {
                    object: 0,
                    bytes: 3..4
                },
                ObjectRange {
                    object: 1,
                    bytes: 0..4
                }
            ]
        );
        let short = CoSelectionLayout::fit(&pure_source(33, 768, &[vec![0, 1, 2]]), &mut work())
            .unwrap()
            .plan(
                &[0],
                ReadBudget {
                    operations: 0,
                    bytes: TOTAL_BYTES - 17 * 780,
                },
                &mut work(),
            )
            .unwrap();
        assert!(short.fits);
        assert_eq!(short.expansion_groups, vec![2]);
        assert_eq!(short.expansion_skipped, 1);
        let bridge = CoSelectionLayout::fit(&pure_source(80, 768, &[vec![0, 4, 2]]), &mut work())
            .unwrap()
            .plan(
                &[0, 64],
                ReadBudget {
                    operations: 31,
                    bytes: TOTAL_BYTES - 80 * 780,
                },
                &mut work(),
            )
            .unwrap();
        assert!(bridge.fits);
        assert_eq!(bridge.expansion_groups, vec![2]);
        assert_eq!(bridge.bridge_groups, vec![1, 3]);
        assert_eq!(
            bridge.payload_bytes,
            Some(bridge.mandatory_cover.payload_bytes)
        );
        let tied = minimum_cover(
            &[
                ObjectRange {
                    object: 0,
                    bytes: 0..1,
                },
                ObjectRange {
                    object: 0,
                    bytes: 2..3,
                },
                ObjectRange {
                    object: 0,
                    bytes: 4..5,
                },
            ],
            2,
            &mut work(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            tied,
            vec![
                ObjectRange {
                    object: 0,
                    bytes: 0..1
                },
                ObjectRange {
                    object: 0,
                    bytes: 2..5
                }
            ]
        );
    }
    #[test]
    fn co_selection_source_anchor_purity_actual_nominees_and_identity_tamper() {
        let tmp = tempfile::tempdir().unwrap();
        let p = fixture_panel(&tmp.path().join("source"), "relaion");
        OPENED.with(|o| o.borrow_mut().clear());
        let source = SourceSelections::generate_counts(&p, 4, 2, &mut work()).unwrap();
        assert_eq!(source.anchors.len(), 6);
        let mut independent = (0..35u64)
            .map(|id| {
                (
                    anchor_key(&digest(&p.canonical.sha256).unwrap(), id),
                    id as i64,
                    (34 - id) as i64,
                )
            })
            .collect::<Vec<_>>();
        independent.sort_unstable();
        let limits = ResidentLimits {
            max_peak_payload_bytes: MEMORY,
            pinned_generation_bytes: 0,
            active_queries: 1,
            delta_bytes: 0,
            maintenance_bytes: 0,
            runtime_bytes: 0,
        };
        let index = FineSq8Index::open_remote(&p.root, &limits).unwrap();
        let mut workspace = index.new_workspace().unwrap();
        for (anchor, &(_, id, ordinal)) in source.anchors.iter().zip(&independent) {
            assert_eq!(
                (
                    anchor.logical_id,
                    anchor.original_ordinal as i64,
                    anchor.canonical_ordinal as i64
                ),
                (id, ordinal, id)
            );
            let actual = index.plan(&[1., 0.], &mut workspace).unwrap();
            assert_eq!(
                anchor.nominees,
                actual
                    .nominees()
                    .iter()
                    .map(|n| *n as u32)
                    .collect::<Vec<_>>()
            );
        }
        assert!(!source.sources.records.path.exists());
        assert!(!OPENED.with(|o| o.borrow().contains(&source.sources.records.path)));
        let mut wrong = p.clone();
        wrong.graph.sha256 = "0".repeat(64);
        assert!(SourceSelections::generate_counts(&wrong, 4, 2, &mut work()).is_err());
        let body = fs::read(&p.canonical.path).unwrap();
        let mut tampered = body.clone();
        tampered[9] ^= 1;
        fs::write(&p.canonical.path, &tampered).unwrap();
        assert!(SourceSelections::generate_counts(&p, 4, 2, &mut work()).is_err());
        fs::write(&p.canonical.path, [body.as_slice(), b"tail"].concat()).unwrap();
        assert!(SourceSelections::generate_counts(&p, 4, 2, &mut work()).is_err());
        // Production source interface cannot silently reduce its anchor count.
        assert!(SourceSelections::generate(&p, &mut work()).is_err());
    }
    #[test]
    fn co_selection_actual_cli128_seals_before_prefix_and_never_reads_payload_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let (c, p) = fixture(tmp.path());
        let output = tmp.path().join("result.json");
        // Deliberately unparseable forbidden tail is outside the frozen prefix.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&c.prefix.path)
            .unwrap();
        file.write_all(b"FORBIDDEN SQ8/TRUTH TAIL").unwrap();
        OPENED.with(|o| o.borrow_mut().clear());
        let mut observed = false;
        run_fixture(
            &c,
            &p,
            &output,
            &mut |maps, selections| {
                assert_eq!((maps.len(), selections.len()), (2, 2));
                assert!(!OPENED.with(|o| o.borrow().contains(&c.prefix.path)));
                for pin in maps.iter().chain(selections) {
                    assert_eq!(hash(&fs::read(&pin.path)?), pin.sha256);
                }
                observed = true;
                Ok(())
            },
            None,
        )
        .unwrap();
        assert!(observed);
        let report: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(report["status"], "SURVIVED_NECESSARY_LOCALITY");
        assert_eq!(report["queries"], 128);
        assert_eq!(report["standalone_authority"], false);
        let plans: Artifact = serde_json::from_value(report["details"]["plans"].clone()).unwrap();
        let body = fs::read(plans.path).unwrap();
        assert_eq!(hash(&body), plans.sha256);
        let rows = std::str::from_utf8(&body)
            .unwrap()
            .lines()
            .map(|r| serde_json::from_str::<Value>(r).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 128);
        assert!(
            rows.iter()
                .all(|r| r["plan"]["all_nominees_retained"] == true && r["plan"]["fits"] == true)
        );
        let first = fs::read(&output).unwrap();
        assert!(run_fixture(&c, &p, &output, &mut |_, _| Ok(()), None).is_err());
        assert_eq!(first, fs::read(&output).unwrap());
    }
    #[test]
    fn co_selection_cli_complete_reject_does_not_mask_late_invalid_or_hash_tamper() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut c, mut p) = fixture(tmp.path());
        c.prior_reads.operations = 32;
        let rejected = tmp.path().join("rejected.json");
        run_fixture(&c, &p, &rejected, &mut |_, _| Ok(()), None).unwrap();
        let value: Value = serde_json::from_slice(&fs::read(&rejected).unwrap()).unwrap();
        assert_eq!(value["status"], "REJECT");
        assert_eq!(value["queries"], 128);
        let plans = fs::read_to_string(rejected.with_extension("plans.jsonl")).unwrap();
        assert_eq!(plans.lines().count(), 128);
        let mut lines = fs::read_to_string(&c.prefix.path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let mut late: Value = serde_json::from_str(lines.last().unwrap()).unwrap();
        late["ordinal"] = json!(62);
        *lines.last_mut().unwrap() = late.to_string();
        c.prefix = artifact(&c.prefix.path, format!("{}\n", lines.join("\n")).as_bytes());
        p.prefix_bytes = c.prefix.bytes;
        p.prefix_sha = c.prefix.sha256.clone();
        let mut seal: Value =
            serde_json::from_slice(&fs::read(&c.original_seal.path).unwrap()).unwrap();
        seal["prefix_bytes"] = json!(c.prefix.bytes);
        seal["prefix_sha256"] = json!(c.prefix.sha256);
        c.original_seal = artifact(&c.original_seal.path, &serde_json::to_vec(&seal).unwrap());
        p.seal_sha = c.original_seal.sha256.clone();
        let invalid = tmp.path().join("late-invalid.json");
        assert!(run_fixture(&c, &p, &invalid, &mut |_, _| Ok(()), None).is_err());
        let value: Value = serde_json::from_slice(&fs::read(&invalid).unwrap()).unwrap();
        assert_eq!(value["status"], "INVALID");
        assert_eq!(value["complete"], false);
        assert_eq!(
            fs::read_to_string(invalid.with_extension("plans.jsonl"))
                .unwrap()
                .lines()
                .count(),
            127
        );
        let output = tmp.path().join("late-hash.json");
        assert!(
            run_fixture(
                &c,
                &p,
                &output,
                &mut |_, _| {
                    let mut bytes = fs::read(&c.prefix.path)?;
                    let last = bytes.len() - 3;
                    bytes[last] ^= 1;
                    fs::write(&c.prefix.path, bytes)?;
                    Ok(())
                },
                None
            )
            .is_err()
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(output).unwrap()).unwrap()["status"],
            "INVALID"
        );
    }
    #[test]
    fn co_selection_cli_output_fsync_resource_failures_and_strict_config_are_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        let (c, p) = fixture(tmp.path());
        for (name, mut cfg, fail) in [
            ("fsync-file", c.clone(), Some(1)),
            ("fsync-dir", c.clone(), Some(2)),
            ("fsync-second-map", c.clone(), Some(8)),
            ("output", c.clone(), None),
            ("operations", c.clone(), None),
            ("source-cap", c.clone(), None),
        ] {
            if name == "output" {
                cfg.caps.output_bytes = TERMINAL_RESERVE;
            }
            if name == "operations" {
                cfg.caps.construction_operations = 1;
            }
            if name == "source-cap" {
                cfg.caps.source_auth_bytes = 1;
            }
            let output = tmp.path().join(format!("{name}.json"));
            OPENED.with(|o| o.borrow_mut().clear());
            assert!(
                run_fixture(&cfg, &p, &output, &mut |_, _| Ok(()), fail).is_err(),
                "{name}"
            );
            assert_eq!(
                serde_json::from_slice::<Value>(&fs::read(&output).unwrap()).unwrap()["status"],
                "INVALID",
                "{name}"
            );
            assert!(
                !OPENED.with(|o| o.borrow().contains(&c.prefix.path)),
                "{name}"
            );
        }
        for key in ["truth", "requests", "geometry", "training_selections"] {
            let mut value = serde_json::to_value(&c).unwrap();
            value[key] = json!("forbidden");
            assert!(serde_json::from_value::<Config>(value).is_err());
        }
        let output = tmp.path().join("companion.json");
        let companion = output.with_extension("relaion.selections.bin");
        fs::write(&companion, b"keep").unwrap();
        assert!(run_fixture(&c, &p, &output, &mut |_, _| Ok(()), None).is_err());
        assert_eq!(fs::read(companion).unwrap(), b"keep");
    }
    #[test]
    fn co_selection_phase_ledgers_and_full_batch_work_preflight_are_checked() {
        let mut c = caps();
        c.construction_operations = 100;
        c.replay_operations = 40;
        let mut budget = WorkBudget::new(c).unwrap();
        budget.tick(70).unwrap();
        budget.retained_bytes = 77;
        budget.begin_replay().unwrap();
        budget.tick(30).unwrap();
        assert_eq!(
            (
                budget.operations,
                budget.construction_operations,
                budget.replay_operations
            ),
            (100, 70, 30)
        );
        assert_eq!(budget.retained_bytes, 77);
        assert!(budget.tick(11).is_err());
        assert!(budget.begin_replay().is_err());
        let tmp = tempfile::tempdir().unwrap();
        let (mut c, _) = fixture(tmp.path());
        let p = Protocol::frozen();
        for panel in &mut c.panels {
            panel.graph.bytes = 100_000 * 512;
            panel.pq.bytes = 24 + 64 * 256 * 12 * 4 + 100_000 * 64;
        }
        let bound = preflight_work(&c, &p).unwrap();
        assert_eq!(bound.nomination, 235_363_368_960);
        assert!(
            bound.construction > 235_363_368_960 && bound.construction < CONSTRUCTION_OPERATIONS
        );
        assert!(bound.replay < REPLAY_OPERATIONS);
        let output = Path::new("/tmp/co-selection-report.json");
        let output_bound = preflight_output(&c, &p, output).unwrap();
        assert_eq!(output_bound.selection_per_panel * 2, 38_228_384);
        assert!(output_bound.total < OUTPUT_BYTES);
        let output_pin =
            artifact_output_bound(&output.with_extension("relaion.selections.bin")).unwrap();
        let root_pin = c
            .panels
            .iter()
            .map(|p| artifact_output_bound(&p.root.path).unwrap())
            .max()
            .unwrap();
        assert_eq!(
            output_bound.total,
            60_354_084 + 2304 * output_pin + 128 * root_pin
        );
        let mut insufficient = c.clone();
        insufficient.caps.output_bytes = output_bound.total - 1;
        assert!(preflight_output(&insufficient, &p, output).is_err());
        let mut budget = work();
        budget.retained_bytes = budget.caps.memory_bytes - FIXED_MEMORY;
        assert!(budget.serialization(1).is_err());
        let value = json!({"literal":"\\\"\n"});
        let expected = serde_json::to_vec(&value).unwrap();
        assert!(json_bytes(&value, expected.len() - 1, false).is_err());
        let line = json_bytes(&value, expected.len() + 1, true).unwrap();
        assert_eq!(&line[..expected.len()], expected);
        assert_eq!(line.last(), Some(&b'\n'));
        c.caps.construction_operations = bound.construction - 1;
        assert!(preflight_work(&c, &p).is_err());
        c.caps.construction_operations = CONSTRUCTION_OPERATIONS;
        c.caps.replay_operations = bound.replay - 1;
        assert!(preflight_work(&c, &p).is_err());
    }
    #[test]
    fn co_selection_terminal_fsync_failure_overrides_all128_success() {
        let tmp = tempfile::tempdir().unwrap();
        let (c, p) = fixture(tmp.path());
        let output = tmp.path().join("terminal-sync.json");
        assert!(run_fixture(&c, &p, &output, &mut |_, _| Ok(()), Some(15)).is_err());
        let report: Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(report["status"], "INVALID");
        assert_eq!(
            fs::read_to_string(output.with_extension("plans.jsonl"))
                .unwrap()
                .lines()
                .count(),
            128
        );
    }
}
