//! Format-v26 lane-owned foreground ingest primitive.
#![allow(
    dead_code,
    reason = "format-v26 internals include bounded maintenance hooks staged for asynchronous spill"
)]

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    BorsukError, PhysicalFormat, RequestCounts, Result, VectorElementType, VectorRecord,
    storage::Storage,
};
use object_store::{ObjectStore, UpdateVersion};
use rayon::prelude::*;

const BLOCK_MAGIC: &[u8; 8] = b"BRSLBL25";
const HEAD_MAGIC: &[u8; 8] = b"BRSLHD26";
const EPOCH_HEAD_MAGIC: &[u8; 8] = b"BRSLHD27";
const EXTENT_MAGIC: &[u8; 8] = b"BRSLXT27";
const CHECKSUM_BYTES: usize = 32;
const INLINE_SPILL_BYTE_THRESHOLD: u64 = 8 * 1024 * 1024;
const MAX_UNMATERIALIZED_BLOCKS: usize = 128;
const MAX_UNMATERIALIZED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_UNMATERIALIZED_RECORDS: u64 = 65_536;
const ID_AUTHORITY_ENTRY_OVERHEAD_BYTES: u64 = 80;

/// Durable identity and foreground object-store cost of one lane append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaneLogReceipt {
    pub(crate) lane: u16,
    pub(crate) lease_epoch: u64,
    pub(crate) sequence: u64,
    pub(crate) records: u64,
    pub(crate) acknowledgement_bytes: u64,
    pub(crate) requests: RequestCounts,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LaneLogSnapshot {
    pub(crate) record_blocks: Vec<LaneLogRecordBlock>,
    pub(crate) committed_sequences: Vec<u64>,
    pub(crate) head_checksums: Vec<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LaneLogRecordBlock {
    key: String,
    pub(crate) lane: u16,
    pub(crate) bytes: u64,
    pub(crate) records: Arc<Vec<VectorRecord>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneIdState {
    Live,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneIdDeltaState {
    Live,
    Deleted,
    Purged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaneIdDelta {
    id: Vec<u8>,
    state: LaneIdDeltaState,
}

#[derive(Debug)]
struct LaneIdAuthority {
    states: HashMap<Vec<u8>, LaneIdState>,
    resident_bytes: u64,
    budget_bytes: u64,
}

impl LaneIdAuthority {
    fn from_entries<'a>(
        entries: impl IntoIterator<Item = (&'a [u8], LaneIdState)>,
        budget_bytes: u64,
    ) -> Result<Self> {
        let mut authority = Self {
            states: HashMap::new(),
            resident_bytes: 0,
            budget_bytes,
        };
        for (id, state) in entries {
            if id.is_empty() {
                return Err(BorsukError::InvalidStorage(
                    "lane ID authority contains an empty ID".to_string(),
                ));
            }
            if let Some(existing) = authority.states.get(id) {
                if *existing != state {
                    return Err(BorsukError::InvalidStorage(
                        "lane ID authority contains conflicting states".to_string(),
                    ));
                }
                continue;
            }
            authority.charge(id.len())?;
            authority.states.insert(id.to_vec(), state);
        }
        Ok(authority)
    }

    fn charge(&mut self, id_bytes: usize) -> Result<()> {
        let id_bytes = u64::try_from(id_bytes).map_err(|_| BorsukError::RamBudgetExceeded {
            resident_bytes: u64::MAX,
            budget_bytes: self.budget_bytes,
        })?;
        let resident_bytes = self
            .resident_bytes
            .checked_add(ID_AUTHORITY_ENTRY_OVERHEAD_BYTES)
            .and_then(|value| value.checked_add(id_bytes))
            .ok_or(BorsukError::RamBudgetExceeded {
                resident_bytes: u64::MAX,
                budget_bytes: self.budget_bytes,
            })?;
        if resident_bytes > self.budget_bytes {
            return Err(BorsukError::RamBudgetExceeded {
                resident_bytes,
                budget_bytes: self.budget_bytes,
            });
        }
        self.resident_bytes = resident_bytes;
        Ok(())
    }

    fn prepare_insert(&self, ids: &[&[u8]]) -> Result<(Vec<Vec<u8>>, u64)> {
        let mut batch = HashSet::with_capacity(ids.len());
        let mut prepared = Vec::with_capacity(ids.len());
        let mut resident_bytes = self.resident_bytes;
        for id in ids {
            if id.is_empty() {
                return Err(BorsukError::InvalidRecordInput(
                    "record ids must not be empty".to_string(),
                ));
            }
            if !batch.insert(*id) || self.states.contains_key(*id) {
                return Err(BorsukError::InvalidRecordInput(
                    "duplicate record id already exists".to_string(),
                ));
            }
            let id_bytes = u64::try_from(id.len()).map_err(|_| BorsukError::RamBudgetExceeded {
                resident_bytes: u64::MAX,
                budget_bytes: self.budget_bytes,
            })?;
            resident_bytes = resident_bytes
                .checked_add(ID_AUTHORITY_ENTRY_OVERHEAD_BYTES)
                .and_then(|value| value.checked_add(id_bytes))
                .ok_or(BorsukError::RamBudgetExceeded {
                    resident_bytes: u64::MAX,
                    budget_bytes: self.budget_bytes,
                })?;
            if resident_bytes > self.budget_bytes {
                return Err(BorsukError::RamBudgetExceeded {
                    resident_bytes,
                    budget_bytes: self.budget_bytes,
                });
            }
            prepared.push(id.to_vec());
        }
        Ok((prepared, resident_bytes))
    }

    fn commit_insert(&mut self, ids: Vec<Vec<u8>>, resident_bytes: u64) {
        for id in ids {
            self.states.insert(id, LaneIdState::Live);
        }
        self.resident_bytes = resident_bytes;
    }

    fn prepare_upsert(&self, ids: &[&[u8]]) -> Result<(Vec<Vec<u8>>, u64)> {
        let mut batch = HashSet::with_capacity(ids.len());
        let mut prepared = Vec::with_capacity(ids.len());
        let mut resident_bytes = self.resident_bytes;
        for id in ids {
            if id.is_empty() || !batch.insert(*id) {
                return Err(BorsukError::InvalidRecordInput(
                    "upsert IDs must be non-empty and unique".to_string(),
                ));
            }
            if !self.states.contains_key(*id) {
                let id_bytes =
                    u64::try_from(id.len()).map_err(|_| BorsukError::RamBudgetExceeded {
                        resident_bytes: u64::MAX,
                        budget_bytes: self.budget_bytes,
                    })?;
                resident_bytes = resident_bytes
                    .checked_add(ID_AUTHORITY_ENTRY_OVERHEAD_BYTES)
                    .and_then(|value| value.checked_add(id_bytes))
                    .ok_or(BorsukError::RamBudgetExceeded {
                        resident_bytes: u64::MAX,
                        budget_bytes: self.budget_bytes,
                    })?;
                if resident_bytes > self.budget_bytes {
                    return Err(BorsukError::RamBudgetExceeded {
                        resident_bytes,
                        budget_bytes: self.budget_bytes,
                    });
                }
            }
            prepared.push(id.to_vec());
        }
        Ok((prepared, resident_bytes))
    }

    fn prepare_state_change(
        &self,
        ids: &[&[u8]],
        required: LaneIdState,
        operation: &str,
    ) -> Result<Vec<Vec<u8>>> {
        let mut batch = HashSet::with_capacity(ids.len());
        let mut prepared = Vec::with_capacity(ids.len());
        for id in ids {
            if id.is_empty() || !batch.insert(*id) || self.states.get(*id) != Some(&required) {
                return Err(BorsukError::InvalidRecordInput(format!(
                    "{operation} requires unique IDs in the expected lane state"
                )));
            }
            prepared.push(id.to_vec());
        }
        Ok(prepared)
    }

    fn commit_state(&mut self, ids: Vec<Vec<u8>>, state: LaneIdDeltaState, resident_bytes: u64) {
        for id in ids {
            match state {
                LaneIdDeltaState::Live => {
                    self.states.insert(id, LaneIdState::Live);
                }
                LaneIdDeltaState::Deleted => {
                    self.states.insert(id, LaneIdState::Deleted);
                }
                LaneIdDeltaState::Purged => {
                    if let Some((id, _)) = self.states.remove_entry(id.as_slice()) {
                        self.resident_bytes = self
                            .resident_bytes
                            .saturating_sub(ID_AUTHORITY_ENTRY_OVERHEAD_BYTES + id.len() as u64);
                    }
                }
            }
        }
        if state != LaneIdDeltaState::Purged {
            self.resident_bytes = resident_bytes;
        }
    }

    fn apply_recovered(&mut self, delta: &LaneIdDelta) -> Result<()> {
        if delta.id.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "lane-log ID delta contains an empty ID".to_string(),
            ));
        }
        match delta.state {
            LaneIdDeltaState::Live | LaneIdDeltaState::Deleted => {
                if !self.states.contains_key(delta.id.as_slice()) {
                    self.charge(delta.id.len())?;
                }
                let state = match delta.state {
                    LaneIdDeltaState::Live => LaneIdState::Live,
                    LaneIdDeltaState::Deleted => LaneIdState::Deleted,
                    LaneIdDeltaState::Purged => unreachable!(),
                };
                self.states.insert(delta.id.clone(), state);
            }
            LaneIdDeltaState::Purged => {
                if let Some((id, _)) = self.states.remove_entry(delta.id.as_slice()) {
                    self.resident_bytes = self
                        .resident_bytes
                        .saturating_sub(ID_AUTHORITY_ENTRY_OVERHEAD_BYTES + id.len() as u64);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaneLogBlockRef {
    lease_epoch: u64,
    sequence: u64,
    generation: u64,
    checksum: [u8; CHECKSUM_BYTES],
    bytes: u64,
    records: u64,
    inline_bytes: Option<Vec<u8>>,
}

impl LaneLogBlockRef {
    fn path(&self, lane: u16) -> String {
        block_path(lane, self.lease_epoch, self.sequence, &self.checksum)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaneLogHead {
    format_version: u8,
    lane: u16,
    lease_epoch: u64,
    lease_owner: [u8; 16],
    lease_expires_at_ms: u64,
    committed_sequence: u64,
    materialized_sequence: u64,
    generation_clock: u64,
    blocks: Vec<LaneLogBlockRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LaneEpochSeal {
    lease_epoch: u64,
    durable_sequence: u64,
    generation_end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaneEpochHead {
    lane: u16,
    lease_epoch: u64,
    lease_owner: [u8; 16],
    lease_expires_at_ms: u64,
    durable_sequence: u64,
    materialized_sequence: u64,
    generation_base: u64,
    sealed_epoch: Option<LaneEpochSeal>,
}

impl LaneEpochHead {
    fn validate(&self, expected_lane: u16) -> Result<()> {
        if self.lane != expected_lane || self.materialized_sequence > self.durable_sequence {
            return Err(BorsukError::InvalidStorage(
                "invalid epoch lane-log HEAD identity or frontier".to_string(),
            ));
        }
        if self
            .sealed_epoch
            .is_some_and(|seal| seal.lease_epoch >= self.lease_epoch)
        {
            return Err(BorsukError::InvalidStorage(
                "epoch lane-log HEAD seal must precede its owning epoch".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaneExtent {
    lane: u16,
    lease_epoch: u64,
    sequence: u64,
    first_generation: u64,
    records: u64,
    payload: Vec<u8>,
}

impl LaneLogHead {
    fn empty(lane: u16, lease_epoch: u64) -> Self {
        Self {
            format_version: 26,
            lane,
            lease_epoch,
            lease_owner: [0; 16],
            lease_expires_at_ms: u64::MAX,
            committed_sequence: 0,
            materialized_sequence: 0,
            generation_clock: 0,
            blocks: Vec::new(),
        }
    }

    fn validate(&self, expected_lane: u16, expected_epoch: u64) -> Result<()> {
        if self.format_version != 26
            || self.lane != expected_lane
            || self.lease_epoch > expected_epoch
            || self.materialized_sequence > self.committed_sequence
            || self.blocks.len() > MAX_UNMATERIALIZED_BLOCKS
        {
            return Err(BorsukError::InvalidStorage(
                "invalid lane-log HEAD identity or bounds".to_string(),
            ));
        }
        let tail_bytes = self.blocks.iter().try_fold(0_u64, |total, block| {
            total.checked_add(block.bytes).ok_or_else(|| {
                BorsukError::InvalidStorage("lane-log tail byte count overflow".to_string())
            })
        })?;
        let tail_records = self.blocks.iter().try_fold(0_u64, |total, block| {
            total.checked_add(block.records).ok_or_else(|| {
                BorsukError::InvalidStorage("lane-log tail record count overflow".to_string())
            })
        })?;
        if tail_bytes > MAX_UNMATERIALIZED_BYTES || tail_records > MAX_UNMATERIALIZED_RECORDS {
            return Err(BorsukError::InvalidStorage(
                "lane-log HEAD exceeds its hard tail bound".to_string(),
            ));
        }
        let mut previous = self.materialized_sequence;
        let mut previous_generation = 0;
        for block in &self.blocks {
            let expected_sequence = previous.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("lane-log sequence exceeds u64".to_string())
            })?;
            if block.sequence != expected_sequence
                || block.sequence > self.committed_sequence
                || block.generation <= previous_generation
                || block.generation > self.generation_clock
            {
                return Err(BorsukError::InvalidStorage(
                    "lane-log HEAD block sequence is not strictly ordered".to_string(),
                ));
            }
            previous = block.sequence;
            previous_generation = block.generation;
            if let Some(bytes) = &block.inline_bytes
                && (bytes.len() as u64 != block.bytes
                    || blake3::hash(bytes).as_bytes() != &block.checksum)
            {
                return Err(BorsukError::InvalidStorage(
                    "lane-log inline block identity mismatch".to_string(),
                ));
            }
        }
        if self.blocks.last().map(|block| block.sequence) != Some(self.committed_sequence)
            && self.committed_sequence != self.materialized_sequence
        {
            return Err(BorsukError::InvalidStorage(
                "lane-log HEAD does not end at its committed sequence".to_string(),
            ));
        }
        Ok(())
    }
}

fn fenced_bytes(magic: &[u8; 8], body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(magic.len() + 8 + body.len() + CHECKSUM_BYTES);
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&(body.len() as u64).to_le_bytes());
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(blake3::hash(body).as_bytes());
    bytes
}

fn fenced_body<'a>(bytes: &'a [u8], magic: &[u8; 8], label: &str) -> Result<&'a [u8]> {
    let header_bytes = magic.len() + 8;
    if bytes.len() < header_bytes + CHECKSUM_BYTES || &bytes[..magic.len()] != magic {
        return Err(BorsukError::InvalidStorage(format!(
            "invalid lane-log {label} envelope"
        )));
    }
    let body_len = u64::from_le_bytes(
        bytes[magic.len()..header_bytes]
            .try_into()
            .expect("eight-byte length"),
    );
    let body_len = usize::try_from(body_len).map_err(|_| {
        BorsukError::InvalidStorage(format!("lane-log {label} length does not fit usize"))
    })?;
    let body_end = header_bytes
        .checked_add(body_len)
        .ok_or_else(|| BorsukError::InvalidStorage(format!("lane-log {label} length overflow")))?;
    if body_end.checked_add(CHECKSUM_BYTES) != Some(bytes.len()) {
        return Err(BorsukError::InvalidStorage(format!(
            "lane-log {label} has trailing or truncated bytes"
        )));
    }
    let body = &bytes[header_bytes..body_end];
    if blake3::hash(body).as_bytes() != &bytes[body_end..] {
        return Err(BorsukError::InvalidStorage(format!(
            "lane-log {label} checksum mismatch"
        )));
    }
    Ok(body)
}

fn epoch_head_bytes(head: &LaneEpochHead) -> Result<Vec<u8>> {
    head.validate(head.lane)?;
    let mut body = Vec::with_capacity(96);
    body.push(27);
    body.extend_from_slice(&head.lane.to_le_bytes());
    body.extend_from_slice(&head.lease_epoch.to_le_bytes());
    body.extend_from_slice(&head.lease_owner);
    body.extend_from_slice(&head.lease_expires_at_ms.to_le_bytes());
    body.extend_from_slice(&head.durable_sequence.to_le_bytes());
    body.extend_from_slice(&head.materialized_sequence.to_le_bytes());
    body.extend_from_slice(&head.generation_base.to_le_bytes());
    let seal = head.sealed_epoch.unwrap_or(LaneEpochSeal {
        lease_epoch: 0,
        durable_sequence: 0,
        generation_end: 0,
    });
    body.push(u8::from(head.sealed_epoch.is_some()));
    body.extend_from_slice(&seal.lease_epoch.to_le_bytes());
    body.extend_from_slice(&seal.durable_sequence.to_le_bytes());
    body.extend_from_slice(&seal.generation_end.to_le_bytes());
    Ok(fenced_bytes(EPOCH_HEAD_MAGIC, &body))
}

fn epoch_head_from_bytes(bytes: &[u8], expected_lane: u16) -> Result<LaneEpochHead> {
    let body = fenced_body(bytes, EPOCH_HEAD_MAGIC, "epoch HEAD")?;
    let mut cursor = 0;
    if take_u8(body, &mut cursor)? != 27 {
        return Err(BorsukError::InvalidStorage(
            "unsupported epoch lane-log HEAD version".to_string(),
        ));
    }
    let lane = take_u16(body, &mut cursor)?;
    let lease_epoch = take_u64(body, &mut cursor)?;
    let lease_owner = take_array(body, &mut cursor)?;
    let lease_expires_at_ms = take_u64(body, &mut cursor)?;
    let durable_sequence = take_u64(body, &mut cursor)?;
    let materialized_sequence = take_u64(body, &mut cursor)?;
    let generation_base = take_u64(body, &mut cursor)?;
    let seal_present = take_u8(body, &mut cursor)?;
    let seal = LaneEpochSeal {
        lease_epoch: take_u64(body, &mut cursor)?,
        durable_sequence: take_u64(body, &mut cursor)?,
        generation_end: take_u64(body, &mut cursor)?,
    };
    if cursor != body.len() || seal_present > 1 {
        return Err(BorsukError::InvalidStorage(
            "epoch lane-log HEAD has trailing bytes or an invalid seal tag".to_string(),
        ));
    }
    if seal_present == 0
        && seal
            != (LaneEpochSeal {
                lease_epoch: 0,
                durable_sequence: 0,
                generation_end: 0,
            })
    {
        return Err(BorsukError::InvalidStorage(
            "epoch lane-log HEAD absent seal must use canonical zeros".to_string(),
        ));
    }
    let head = LaneEpochHead {
        lane,
        lease_epoch,
        lease_owner,
        lease_expires_at_ms,
        durable_sequence,
        materialized_sequence,
        generation_base,
        sealed_epoch: (seal_present == 1).then_some(seal),
    };
    head.validate(expected_lane)?;
    Ok(head)
}

fn extent_bytes(extent: &LaneExtent) -> Result<Vec<u8>> {
    if extent.sequence == 0 || extent.records == 0 || extent.payload.is_empty() {
        return Err(BorsukError::InvalidRecordInput(
            "epoch lane-log extents require positive sequence, records, and payload".to_string(),
        ));
    }
    let payload_len = u64::try_from(extent.payload.len()).map_err(|_| {
        BorsukError::InvalidRecordInput("epoch lane-log extent payload exceeds u64".to_string())
    })?;
    let mut body = Vec::with_capacity(43_usize.saturating_add(extent.payload.len()));
    body.push(27);
    body.extend_from_slice(&extent.lane.to_le_bytes());
    body.extend_from_slice(&extent.lease_epoch.to_le_bytes());
    body.extend_from_slice(&extent.sequence.to_le_bytes());
    body.extend_from_slice(&extent.first_generation.to_le_bytes());
    body.extend_from_slice(&extent.records.to_le_bytes());
    body.extend_from_slice(&payload_len.to_le_bytes());
    body.extend_from_slice(&extent.payload);
    Ok(fenced_bytes(EXTENT_MAGIC, &body))
}

fn extent_path(lane: u16, lease_epoch: u64, sequence: u64, checksum: &str) -> Result<String> {
    if checksum.len() != 64
        || !checksum
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(BorsukError::InvalidStorage(
            "epoch lane-log extent checksum must be lowercase blake3 hex".to_string(),
        ));
    }
    Ok(format!(
        "lane-log/lanes/{lane:04}/epochs/{lease_epoch:016x}/extents/{sequence:016x}-{checksum}.wal"
    ))
}

fn extent_from_bytes(
    path: &str,
    bytes: &[u8],
    expected_lane: u16,
    expected_epoch: u64,
    expected_sequence: u64,
    expected_checksum: &str,
) -> Result<LaneExtent> {
    let actual_checksum = blake3::hash(bytes).to_hex().to_string();
    if actual_checksum != expected_checksum
        || path
            != extent_path(
                expected_lane,
                expected_epoch,
                expected_sequence,
                expected_checksum,
            )?
    {
        return Err(BorsukError::InvalidStorage(
            "epoch lane-log extent path or checksum identity mismatch".to_string(),
        ));
    }
    let body = fenced_body(bytes, EXTENT_MAGIC, "extent")?;
    let mut cursor = 0;
    if take_u8(body, &mut cursor)? != 27 {
        return Err(BorsukError::InvalidStorage(
            "unsupported epoch lane-log extent version".to_string(),
        ));
    }
    let lane = take_u16(body, &mut cursor)?;
    let lease_epoch = take_u64(body, &mut cursor)?;
    let sequence = take_u64(body, &mut cursor)?;
    let first_generation = take_u64(body, &mut cursor)?;
    let records = take_u64(body, &mut cursor)?;
    let payload_len = usize::try_from(take_u64(body, &mut cursor)?).map_err(|_| {
        BorsukError::InvalidStorage("epoch lane-log extent payload exceeds usize".to_string())
    })?;
    let payload_end = cursor.checked_add(payload_len).ok_or_else(|| {
        BorsukError::InvalidStorage("epoch lane-log extent payload length overflow".to_string())
    })?;
    let payload = body
        .get(cursor..payload_end)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("epoch lane-log extent payload is truncated".to_string())
        })?
        .to_vec();
    cursor = payload_end;
    if cursor != body.len()
        || lane != expected_lane
        || lease_epoch != expected_epoch
        || sequence != expected_sequence
        || sequence == 0
        || records == 0
        || payload.is_empty()
    {
        return Err(BorsukError::InvalidStorage(
            "epoch lane-log extent identity or bounds mismatch".to_string(),
        ));
    }
    Ok(LaneExtent {
        lane,
        lease_epoch,
        sequence,
        first_generation,
        records,
        payload,
    })
}

fn block_bytes(payload: &[u8]) -> Vec<u8> {
    block_bytes_with_deltas(payload, &[]).expect("empty ID delta list is valid")
}

fn block_payload(bytes: &[u8]) -> Result<&[u8]> {
    Ok(block_from_bytes(bytes)?.0)
}

fn block_bytes_with_deltas(payload: &[u8], deltas: &[LaneIdDelta]) -> Result<Vec<u8>> {
    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        BorsukError::InvalidRecordInput("lane-log payload length exceeds u64".to_string())
    })?;
    let delta_count = u32::try_from(deltas.len()).map_err(|_| {
        BorsukError::InvalidRecordInput("lane-log ID delta count exceeds u32".to_string())
    })?;
    let mut body = Vec::new();
    body.push(1);
    body.extend_from_slice(&payload_len.to_le_bytes());
    body.extend_from_slice(&delta_count.to_le_bytes());
    body.extend_from_slice(payload);
    for delta in deltas {
        if delta.id.is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "lane-log ID deltas require non-empty IDs".to_string(),
            ));
        }
        body.push(match delta.state {
            LaneIdDeltaState::Live => 1,
            LaneIdDeltaState::Deleted => 2,
            LaneIdDeltaState::Purged => 3,
        });
        let id_len = u32::try_from(delta.id.len()).map_err(|_| {
            BorsukError::InvalidRecordInput("lane-log ID length exceeds u32".to_string())
        })?;
        body.extend_from_slice(&id_len.to_le_bytes());
        body.extend_from_slice(&delta.id);
    }
    Ok(fenced_bytes(BLOCK_MAGIC, &body))
}

fn block_from_bytes(bytes: &[u8]) -> Result<(&[u8], Vec<LaneIdDelta>)> {
    let body = fenced_body(bytes, BLOCK_MAGIC, "block")?;
    let mut cursor = 0;
    if take_u8(body, &mut cursor)? != 1 {
        return Err(BorsukError::InvalidStorage(
            "unsupported lane-log block version".to_string(),
        ));
    }
    let payload_len = usize::try_from(take_u64(body, &mut cursor)?).map_err(|_| {
        BorsukError::InvalidStorage("lane-log payload length exceeds usize".to_string())
    })?;
    let delta_count = usize::try_from(take_u32(body, &mut cursor)?).map_err(|_| {
        BorsukError::InvalidStorage("lane-log delta count exceeds usize".to_string())
    })?;
    let payload_end = cursor.checked_add(payload_len).ok_or_else(|| {
        BorsukError::InvalidStorage("lane-log payload length overflow".to_string())
    })?;
    let payload = body.get(cursor..payload_end).ok_or_else(|| {
        BorsukError::InvalidStorage("lane-log block payload is truncated".to_string())
    })?;
    cursor = payload_end;
    let mut deltas = Vec::with_capacity(delta_count);
    for _ in 0..delta_count {
        let state = match take_u8(body, &mut cursor)? {
            1 => LaneIdDeltaState::Live,
            2 => LaneIdDeltaState::Deleted,
            3 => LaneIdDeltaState::Purged,
            value => {
                return Err(BorsukError::InvalidStorage(format!(
                    "invalid lane-log ID delta state {value}"
                )));
            }
        };
        let id_len = usize::try_from(take_u32(body, &mut cursor)?).map_err(|_| {
            BorsukError::InvalidStorage("lane-log ID length exceeds usize".to_string())
        })?;
        let id_end = cursor.checked_add(id_len).ok_or_else(|| {
            BorsukError::InvalidStorage("lane-log ID length overflow".to_string())
        })?;
        let id = body
            .get(cursor..id_end)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("lane-log ID delta is truncated".to_string())
            })?
            .to_vec();
        cursor = id_end;
        if id.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "lane-log ID delta contains an empty ID".to_string(),
            ));
        }
        deltas.push(LaneIdDelta { id, state });
    }
    if cursor != body.len() {
        return Err(BorsukError::InvalidStorage(
            "lane-log block contains trailing bytes".to_string(),
        ));
    }
    Ok((payload, deltas))
}

fn head_bytes(head: &LaneLogHead) -> Result<Vec<u8>> {
    head.validate(head.lane, head.lease_epoch)?;
    let block_count = u16::try_from(head.blocks.len()).map_err(|_| {
        BorsukError::InvalidStorage("lane-log HEAD block count exceeds u16".to_string())
    })?;
    let inline_bytes = head
        .blocks
        .iter()
        .filter_map(|block| block.inline_bytes.as_ref())
        .map(Vec::len)
        .sum::<usize>();
    let mut body = Vec::with_capacity(61 + head.blocks.len() * 77 + inline_bytes);
    body.push(head.format_version);
    body.extend_from_slice(&head.lane.to_le_bytes());
    body.extend_from_slice(&head.lease_epoch.to_le_bytes());
    body.extend_from_slice(&head.lease_owner);
    body.extend_from_slice(&head.lease_expires_at_ms.to_le_bytes());
    body.extend_from_slice(&head.committed_sequence.to_le_bytes());
    body.extend_from_slice(&head.materialized_sequence.to_le_bytes());
    body.extend_from_slice(&head.generation_clock.to_le_bytes());
    body.extend_from_slice(&block_count.to_le_bytes());
    for block in &head.blocks {
        body.extend_from_slice(&block.lease_epoch.to_le_bytes());
        body.extend_from_slice(&block.sequence.to_le_bytes());
        body.extend_from_slice(&block.generation.to_le_bytes());
        body.extend_from_slice(&block.checksum);
        body.extend_from_slice(&block.bytes.to_le_bytes());
        body.extend_from_slice(&block.records.to_le_bytes());
        match &block.inline_bytes {
            Some(bytes) => {
                body.push(1);
                body.extend_from_slice(
                    &u32::try_from(bytes.len())
                        .map_err(|_| {
                            BorsukError::InvalidStorage(
                                "lane-log inline block exceeds u32".to_string(),
                            )
                        })?
                        .to_le_bytes(),
                );
                body.extend_from_slice(bytes);
            }
            None => body.push(0),
        }
    }
    Ok(fenced_bytes(HEAD_MAGIC, &body))
}

fn head_from_bytes(bytes: &[u8], lane: u16, lease_epoch: u64) -> Result<LaneLogHead> {
    let body = fenced_body(bytes, HEAD_MAGIC, "HEAD")?;
    let mut cursor = 0;
    let format_version = take_u8(body, &mut cursor)?;
    let stored_lane = take_u16(body, &mut cursor)?;
    let stored_epoch = take_u64(body, &mut cursor)?;
    let lease_owner = take_array(body, &mut cursor)?;
    let lease_expires_at_ms = take_u64(body, &mut cursor)?;
    let committed_sequence = take_u64(body, &mut cursor)?;
    let materialized_sequence = take_u64(body, &mut cursor)?;
    let generation_clock = take_u64(body, &mut cursor)?;
    let block_count = usize::from(take_u16(body, &mut cursor)?);
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        let lease_epoch = take_u64(body, &mut cursor)?;
        let sequence = take_u64(body, &mut cursor)?;
        let generation = take_u64(body, &mut cursor)?;
        let checksum = take_array(body, &mut cursor)?;
        let bytes = take_u64(body, &mut cursor)?;
        let records = take_u64(body, &mut cursor)?;
        let inline = take_u8(body, &mut cursor)?;
        let inline_bytes = match inline {
            0 => None,
            1 => {
                let length = usize::try_from(take_u32(body, &mut cursor)?).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "lane-log inline block length exceeds usize".to_string(),
                    )
                })?;
                let end = cursor.checked_add(length).ok_or_else(|| {
                    BorsukError::InvalidStorage("lane-log inline block cursor overflow".to_string())
                })?;
                let value = body.get(cursor..end).ok_or_else(|| {
                    BorsukError::InvalidStorage("lane-log inline block is truncated".to_string())
                })?;
                cursor = end;
                Some(value.to_vec())
            }
            _ => {
                return Err(BorsukError::InvalidStorage(
                    "invalid lane-log block representation".to_string(),
                ));
            }
        };
        blocks.push(LaneLogBlockRef {
            lease_epoch,
            sequence,
            generation,
            checksum,
            bytes,
            records,
            inline_bytes,
        });
    }
    if cursor != body.len() {
        return Err(BorsukError::InvalidStorage(
            "lane-log HEAD contains trailing descriptor bytes".to_string(),
        ));
    }
    let head = LaneLogHead {
        format_version,
        lane: stored_lane,
        lease_epoch: stored_epoch,
        lease_owner,
        lease_expires_at_ms,
        committed_sequence,
        materialized_sequence,
        generation_clock,
        blocks,
    };
    head.validate(lane, lease_epoch)?;
    Ok(head)
}

fn take_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| BorsukError::InvalidStorage("lane-log HEAD cursor overflow".to_string()))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| BorsukError::InvalidStorage("lane-log HEAD is truncated".to_string()))?;
    *cursor = end;
    Ok(value.try_into().expect("fixed-width slice"))
}

fn take_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    Ok(take_array::<1>(bytes, cursor)?[0])
}

fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    Ok(u16::from_le_bytes(take_array(bytes, cursor)?))
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(take_array(bytes, cursor)?))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(take_array(bytes, cursor)?))
}

fn head_path(lane: u16) -> String {
    format!("lane-log/lanes/{lane:04}/HEAD")
}

pub(crate) fn initialize_empty_lane_heads(storage: &Storage, lane_count: u16) -> Result<()> {
    if lane_count == 0 || lane_count > 64 {
        return Err(BorsukError::InvalidStorage(
            "lane-log lane_count must be between 1 and 64".to_string(),
        ));
    }
    for lane in 0..lane_count {
        let mut head = LaneLogHead::empty(lane, 1);
        head.lease_owner = [0xff; 16];
        head.lease_expires_at_ms = 1;
        let bytes = head_bytes(&head)?;
        let path = head_path(lane);
        match storage.write_coordination_object(&path, &bytes, None) {
            Ok(_) => {}
            Err(BorsukError::ConcurrentModification { .. }) => {
                let stored = storage.read_coordination_object(&path)?.ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "lane-log HEAD `{path}` disappeared during initialization"
                    ))
                })?;
                if stored.bytes != bytes {
                    return Err(BorsukError::InvalidStorage(format!(
                        "lane-log HEAD `{path}` conflicts with index initialization"
                    )));
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn block_path(lane: u16, lease_epoch: u64, sequence: u64, checksum: &[u8; 32]) -> String {
    let checksum = blake3::Hash::from_bytes(*checksum).to_hex();
    format!(
        "lane-log/lanes/{lane:04}/epochs/{lease_epoch:020}/blocks/{sequence:020}-{checksum}.blk"
    )
}

/// Single-owner append handle. The mutable HEAD is also the lease authority, so
/// one takeover CAS immediately fences the prior owner without another object.
pub(crate) struct LaneLogWriter {
    storage: Storage,
    head: LaneLogHead,
    head_version: Option<UpdateVersion>,
    id_authority: Option<LaneIdAuthority>,
    recovery_required: bool,
}

struct LaneEpochWriter {
    storage: Storage,
    head: LaneEpochHead,
}

impl LaneEpochWriter {
    #[cfg(test)]
    fn new_empty(
        store: Arc<dyn ObjectStore>,
        uri: impl Into<String>,
        lane: u16,
        lease_epoch: u64,
        lease_expires_at_ms: u64,
    ) -> Result<Self> {
        let storage = Storage::from_object_store(uri.into(), store)?;
        let head = LaneEpochHead {
            lane,
            lease_epoch,
            lease_owner: [1; 16],
            lease_expires_at_ms,
            durable_sequence: 0,
            materialized_sequence: 0,
            generation_base: 0,
            sealed_epoch: None,
        };
        storage.write_coordination_object(&head_path(lane), &epoch_head_bytes(&head)?, None)?;
        Ok(Self { storage, head })
    }

    fn append_extent_at(
        &mut self,
        payload: &[u8],
        records: u64,
        completed_at_ms: u64,
    ) -> Result<LaneLogReceipt> {
        let before = self.storage.request_counts();
        let sequence = self.head.durable_sequence.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("epoch lane-log sequence exceeds u64".to_string())
        })?;
        let first_generation = self.head.generation_base.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("epoch lane-log generation exceeds u64".to_string())
        })?;
        let extent = LaneExtent {
            lane: self.head.lane,
            lease_epoch: self.head.lease_epoch,
            sequence,
            first_generation,
            records,
            payload: payload.to_vec(),
        };
        let bytes = extent_bytes(&extent)?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = extent_path(self.head.lane, self.head.lease_epoch, sequence, &checksum)?;
        self.storage
            .create_bytes_verified(&path, &bytes, &checksum)?;
        if completed_at_ms >= self.head.lease_expires_at_ms {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{path}/LEASE_EXPIRED"),
            });
        }
        self.head.durable_sequence = sequence;
        self.head.generation_base = first_generation;
        Ok(LaneLogReceipt {
            lane: self.head.lane,
            lease_epoch: self.head.lease_epoch,
            sequence,
            records,
            acknowledgement_bytes: bytes.len() as u64,
            requests: self.storage.request_counts().delta(&before),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneReadConsistency {
    Committed,
    Linearizable,
}

struct LaneEpochReader {
    storage: Storage,
    lane_count: u16,
}

impl LaneEpochReader {
    fn new(store: Arc<dyn ObjectStore>, uri: impl Into<String>, lane_count: u16) -> Result<Self> {
        if lane_count == 0 || lane_count > 64 {
            return Err(BorsukError::InvalidStorage(
                "epoch lane-log reader lane_count must be between 1 and 64".to_string(),
            ));
        }
        Ok(Self {
            storage: Storage::from_object_store(uri.into(), store)?,
            lane_count,
        })
    }

    fn read_lane(&self, lane: u16, consistency: LaneReadConsistency) -> Result<Vec<LaneExtent>> {
        if lane >= self.lane_count {
            return Err(BorsukError::InvalidStorage(format!(
                "epoch lane-log lane {lane} exceeds configured lane count {}",
                self.lane_count
            )));
        }
        let path = head_path(lane);
        let stored = self
            .storage
            .read_coordination_object(&path)?
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!("epoch lane-log HEAD `{path}` is missing"))
            })?;
        let head = epoch_head_from_bytes(&stored.bytes, lane)?;
        let mut extents = Vec::new();
        if let Some(seal) = head.sealed_epoch {
            extents.extend(self.read_epoch(lane, seal.lease_epoch, Some(seal.durable_sequence))?);
        }
        let current_limit = match consistency {
            LaneReadConsistency::Committed => Some(head.durable_sequence),
            LaneReadConsistency::Linearizable => None,
        };
        extents.extend(self.read_epoch(lane, head.lease_epoch, current_limit)?);
        Ok(extents)
    }

    fn read_epoch(
        &self,
        lane: u16,
        lease_epoch: u64,
        maximum_sequence: Option<u64>,
    ) -> Result<Vec<LaneExtent>> {
        let prefix = format!("lane-log/lanes/{lane:04}/epochs/{lease_epoch:016x}/extents/");
        let mut identities = self
            .storage
            .list_objects(&prefix)?
            .into_iter()
            .map(|object| {
                let name = object.path.strip_prefix(&prefix).ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "epoch lane-log extent `{}` is outside `{prefix}`",
                        object.path
                    ))
                })?;
                let (sequence, checksum) = name
                    .strip_suffix(".wal")
                    .and_then(|name| name.split_once('-'))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "epoch lane-log extent `{}` has an invalid name",
                            object.path
                        ))
                    })?;
                let sequence = u64::from_str_radix(sequence, 16).map_err(|_| {
                    BorsukError::InvalidStorage(format!(
                        "epoch lane-log extent `{}` has an invalid sequence",
                        object.path
                    ))
                })?;
                Ok((sequence, checksum.to_string(), object.path))
            })
            .collect::<Result<Vec<_>>>()?;
        identities.sort_by_key(|(sequence, _, _)| *sequence);
        let mut previous = None;
        let mut extents = Vec::new();
        for (sequence, checksum, path) in identities {
            if maximum_sequence.is_some_and(|maximum| sequence > maximum) {
                continue;
            }
            if previous == Some(sequence) {
                return Err(BorsukError::InvalidStorage(format!(
                    "epoch lane-log has multiple extents for lane {lane} epoch {lease_epoch} sequence {sequence}"
                )));
            }
            previous = Some(sequence);
            let bytes = self.storage.read_object_fresh(&path)?.ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "listed epoch lane-log extent `{path}` disappeared"
                ))
            })?;
            extents.push(extent_from_bytes(
                &path,
                &bytes,
                lane,
                lease_epoch,
                sequence,
                &checksum,
            )?);
        }
        Ok(extents)
    }
}

impl Drop for LaneLogWriter {
    fn drop(&mut self) {
        if self.recovery_required || self.head_version.is_none() || self.head.lease_owner == [0; 16]
        {
            return;
        }
        let mut released = self.head.clone();
        released.lease_owner = [0; 16];
        released.lease_expires_at_ms = 0;
        let Ok(bytes) = head_bytes(&released) else {
            return;
        };
        if let Ok(version) = self.storage.write_coordination_object(
            &head_path(self.head.lane),
            &bytes,
            self.head_version.clone(),
        ) {
            self.head = released;
            self.head_version = Some(version);
        }
    }
}

/// Fixed-fanout reader for HEAD-reachable format-v26 lane records.
pub(crate) struct LaneLogReader {
    storage: Storage,
    lane_count: u16,
}

struct LaneLogHeads {
    heads: Vec<Option<LaneLogHead>>,
    checksums: Vec<[u8; 32]>,
}

impl LaneLogReader {
    fn new(store: Arc<dyn ObjectStore>, uri: impl Into<String>, lane_count: u16) -> Result<Self> {
        if lane_count == 0 || lane_count > 64 {
            return Err(BorsukError::InvalidStorage(
                "lane-log reader lane_count must be between 1 and 64".to_string(),
            ));
        }
        Ok(Self {
            storage: Storage::from_object_store(uri.into(), store)?,
            lane_count,
        })
    }

    pub(crate) fn from_storage(storage: Storage, lane_count: u16) -> Result<Self> {
        if lane_count == 0 || lane_count > 64 {
            return Err(BorsukError::InvalidStorage(
                "lane-log reader lane_count must be between 1 and 64".to_string(),
            ));
        }
        Ok(Self {
            storage,
            lane_count,
        })
    }

    fn request_counts(&self) -> RequestCounts {
        self.storage.request_counts()
    }

    fn read_heads(&self) -> Result<LaneLogHeads> {
        let per_lane = read_lane_fanout(self.lane_count, |lane| {
            let path = head_path(lane);
            let stored = self
                .storage
                .read_coordination_object(&path)?
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "authoritative lane-log HEAD `{path}` is missing"
                    ))
                })?;
            let head_checksum = *blake3::hash(&stored.bytes).as_bytes();
            let head = head_from_bytes(&stored.bytes, lane, u64::MAX)?;
            Ok((Some(head), head_checksum))
        })?;
        let mut heads = Vec::with_capacity(per_lane.len());
        let mut checksums = Vec::with_capacity(per_lane.len());
        for (head, checksum) in per_lane {
            heads.push(head);
            checksums.push(checksum);
        }
        Ok(LaneLogHeads { heads, checksums })
    }

    fn decode_snapshot(
        &self,
        heads: LaneLogHeads,
        current_blocks: &[LaneLogRecordBlock],
        runtime: Option<&crate::index::WalTailRuntime>,
    ) -> Result<LaneLogSnapshot> {
        let committed_sequences = heads
            .heads
            .iter()
            .map(|head| head.as_ref().map_or(0, |head| head.committed_sequence))
            .collect::<Vec<_>>();
        let blocks = heads
            .heads
            .iter()
            .enumerate()
            .flat_map(|(lane, head)| {
                head.iter().flat_map(move |head| {
                    head.blocks.iter().map(move |block| {
                        (
                            u16::try_from(lane).expect("validated lane count fits in u16"),
                            block,
                        )
                    })
                })
            })
            .collect::<Vec<_>>();
        let current_blocks = current_blocks
            .iter()
            .map(|block| (block.key.as_str(), Arc::clone(&block.records)))
            .collect::<HashMap<_, _>>();
        let decoded = crate::parallel::install_io(|| {
            blocks
                .par_iter()
                .map(|(lane, block)| {
                    let path = block.path(*lane);
                    let key = format!("lane-log:{path}:generation-{}", block.generation);
                    if let Some(records) = current_blocks.get(key.as_str()) {
                        return Ok(LaneLogRecordBlock {
                            key,
                            lane: *lane,
                            bytes: block.bytes,
                            records: Arc::clone(records),
                        });
                    }
                    let load = || {
                        let bytes = match &block.inline_bytes {
                            Some(bytes) => bytes.clone(),
                            None => self
                                .storage
                                .read_coordination_object(&path)?
                                .map(|stored| stored.bytes)
                                .ok_or_else(|| {
                                    BorsukError::InvalidStorage(format!(
                                        "committed lane-log block `{path}` is missing"
                                    ))
                                })?,
                        };
                        if blake3::hash(&bytes).as_bytes() != &block.checksum {
                            return Err(BorsukError::InvalidStorage(format!(
                                "lane-log block `{path}` checksum mismatch"
                            )));
                        }
                        let payload = block_payload(&bytes)?.to_vec();
                        let mut records =
                            crate::format::wal_records_from_table(payload, "lane-records.parquet")?;
                        for record in &mut records {
                            record.generation = block.generation;
                        }
                        Ok(records)
                    };
                    let decoded = match runtime {
                        Some(runtime) => runtime.load_record_run(&key, block.bytes, load)?,
                        None => Arc::new(load()?),
                    };
                    Ok(LaneLogRecordBlock {
                        key,
                        lane: *lane,
                        bytes: block.bytes,
                        records: decoded,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(LaneLogSnapshot {
            record_blocks: decoded,
            committed_sequences,
            head_checksums: heads.checksums,
        })
    }

    pub(crate) fn read_snapshot_if_changed(
        &self,
        current_head_checksums: &[[u8; 32]],
        current_blocks: &[LaneLogRecordBlock],
        runtime: &crate::index::WalTailRuntime,
    ) -> Result<Option<LaneLogSnapshot>> {
        let heads = self.read_heads()?;
        if heads.checksums == current_head_checksums {
            return Ok(None);
        }
        self.decode_snapshot(heads, current_blocks, Some(runtime))
            .map(Some)
    }

    pub(crate) fn read_snapshot(&self) -> Result<LaneLogSnapshot> {
        let heads = self.read_heads()?;
        self.decode_snapshot(heads, &[], None)
    }

    pub(crate) fn read_records(&self) -> Result<Vec<VectorRecord>> {
        let snapshot = self.read_snapshot()?;
        let record_count = snapshot
            .record_blocks
            .iter()
            .map(|block| block.records.len())
            .sum();
        let mut records = Vec::with_capacity(record_count);
        for block in snapshot.record_blocks {
            records.extend(block.records.iter().cloned());
        }
        Ok(records)
    }
}

fn read_lane_fanout<T, F>(lane_count: u16, read: F) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(u16) -> Result<T> + Send + Sync,
{
    crate::parallel::install_io(|| {
        (0..lane_count)
            .into_par_iter()
            .map(read)
            .collect::<Result<Vec<_>>>()
    })
}

impl LaneLogWriter {
    pub(crate) fn lane(&self) -> u16 {
        self.head.lane
    }

    pub(crate) fn acquire_for_upserts(
        storage: Storage,
        lane: u16,
        owner: [u8; 16],
        now_ms: u64,
        ttl_ms: u64,
        id_budget_bytes: u64,
        minimum_generation: u64,
    ) -> Result<Self> {
        let authority = LaneIdAuthority::from_entries(
            std::iter::empty::<(&[u8], LaneIdState)>(),
            id_budget_bytes,
        )?;
        Self::acquire_with_storage(
            storage,
            lane,
            owner,
            now_ms,
            ttl_ms,
            authority,
            minimum_generation,
        )
    }

    #[cfg(test)]
    fn acquire(
        store: Arc<dyn ObjectStore>,
        uri: impl Into<String>,
        lane: u16,
        owner: [u8; 16],
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Self> {
        let storage = Storage::from_object_store(uri.into(), store)?;
        let (head, expected) = Self::prepare_acquisition(&storage, lane, owner, now_ms, ttl_ms)?;
        let version = Self::publish_acquisition(&storage, &head, expected)?;
        Ok(Self {
            storage,
            head,
            head_version: Some(version),
            id_authority: None,
            recovery_required: false,
        })
    }

    fn prepare_acquisition(
        storage: &Storage,
        lane: u16,
        owner: [u8; 16],
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<(LaneLogHead, Option<UpdateVersion>)> {
        if owner == [0; 16] || ttl_ms == 0 {
            return Err(BorsukError::InvalidRecordInput(
                "lane lease requires a nonzero owner and TTL".to_string(),
            ));
        }
        let expires_at_ms = now_ms.checked_add(ttl_ms).ok_or_else(|| {
            BorsukError::InvalidRecordInput("lane lease expiry exceeds u64".to_string())
        })?;
        let path = head_path(lane);
        let current = storage.read_coordination_object(&path)?;
        let (mut head, expected) = match current {
            Some(stored) => {
                let mut head = head_from_bytes(&stored.bytes, lane, u64::MAX)?;
                if head.lease_expires_at_ms > now_ms && head.lease_owner != owner {
                    return Err(BorsukError::ConcurrentModification { path });
                }
                if head.lease_owner != owner || head.lease_expires_at_ms <= now_ms {
                    head.lease_epoch = head.lease_epoch.checked_add(1).ok_or_else(|| {
                        BorsukError::InvalidStorage("lane lease epoch exceeds u64".to_string())
                    })?;
                }
                (head, Some(stored.version))
            }
            None => (LaneLogHead::empty(lane, 1), None),
        };
        head.lease_owner = owner;
        head.lease_expires_at_ms = expires_at_ms;
        Ok((head, expected))
    }

    fn publish_acquisition(
        storage: &Storage,
        head: &LaneLogHead,
        expected: Option<UpdateVersion>,
    ) -> Result<UpdateVersion> {
        let path = head_path(head.lane);
        let bytes = head_bytes(head)?;
        let version = match storage.write_coordination_object(&path, &bytes, expected) {
            Ok(version) => version,
            Err(
                error @ (BorsukError::ConcurrentModification { .. }
                | BorsukError::ObjectStoreRetryable { .. }),
            ) => {
                let Some(stored) = storage.read_coordination_object(&path)? else {
                    return Err(error);
                };
                if stored.bytes != bytes {
                    return Err(error);
                }
                stored.version
            }
            Err(error) => return Err(error),
        };
        Ok(version)
    }

    fn acquire_with_authority(
        store: Arc<dyn ObjectStore>,
        uri: impl Into<String>,
        lane: u16,
        owner: [u8; 16],
        now_ms: u64,
        ttl_ms: u64,
        authority: LaneIdAuthority,
    ) -> Result<Self> {
        let storage = Storage::from_object_store(uri.into(), store)?;
        Self::acquire_with_storage(storage, lane, owner, now_ms, ttl_ms, authority, 0)
    }

    fn acquire_with_storage(
        storage: Storage,
        lane: u16,
        owner: [u8; 16],
        now_ms: u64,
        ttl_ms: u64,
        mut authority: LaneIdAuthority,
        minimum_generation: u64,
    ) -> Result<Self> {
        let (mut head, expected) =
            Self::prepare_acquisition(&storage, lane, owner, now_ms, ttl_ms)?;
        head.generation_clock = head.generation_clock.max(minimum_generation);
        for block in &head.blocks {
            let path = block.path(head.lane);
            let bytes = match &block.inline_bytes {
                Some(bytes) => bytes.clone(),
                None => storage
                    .read_coordination_object(&path)?
                    .map(|stored| stored.bytes)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "committed lane-log block `{path}` is missing"
                        ))
                    })?,
            };
            if blake3::hash(&bytes).as_bytes() != &block.checksum {
                return Err(BorsukError::InvalidStorage(format!(
                    "lane-log block `{path}` checksum mismatch"
                )));
            }
            for delta in &block_from_bytes(&bytes)?.1 {
                authority.apply_recovered(delta)?;
            }
        }
        let version = Self::publish_acquisition(&storage, &head, expected)?;
        Ok(Self {
            storage,
            head,
            head_version: Some(version),
            id_authority: Some(authority),
            recovery_required: false,
        })
    }

    #[cfg(test)]
    fn new_empty(
        store: Arc<dyn ObjectStore>,
        uri: impl Into<String>,
        lane: u16,
        lease_epoch: u64,
    ) -> Result<Self> {
        Ok(Self {
            storage: Storage::from_object_store(uri.into(), store)?,
            head: LaneLogHead::empty(lane, lease_epoch),
            head_version: None,
            id_authority: None,
            recovery_required: false,
        })
    }

    #[cfg(test)]
    fn open(
        store: Arc<dyn ObjectStore>,
        uri: impl Into<String>,
        lane: u16,
        lease_epoch: u64,
    ) -> Result<Self> {
        let storage = Storage::from_object_store(uri.into(), store)?;
        let path = head_path(lane);
        let stored = storage.read_coordination_object(&path)?.ok_or_else(|| {
            BorsukError::InvalidStorage(format!("lane-log HEAD `{path}` does not exist"))
        })?;
        let mut head = head_from_bytes(&stored.bytes, lane, lease_epoch)?;
        head.lease_epoch = lease_epoch;
        Ok(Self {
            storage,
            head,
            head_version: Some(stored.version),
            id_authority: None,
            recovery_required: false,
        })
    }

    fn request_counts(&self) -> RequestCounts {
        self.storage.request_counts()
    }

    #[cfg(test)]
    fn stage_block(&self, sequence: u64, payload: &[u8], records: u64) -> Result<LaneLogBlockRef> {
        self.stage_block_with_deltas(sequence, sequence, payload, records, &[])
    }

    fn stage_block_with_deltas(
        &self,
        sequence: u64,
        generation: u64,
        payload: &[u8],
        records: u64,
        deltas: &[LaneIdDelta],
    ) -> Result<LaneLogBlockRef> {
        if sequence == 0 || records == 0 {
            return Err(BorsukError::InvalidStorage(
                "lane-log blocks require positive sequence and record count".to_string(),
            ));
        }
        let bytes = block_bytes_with_deltas(payload, deltas)?;
        let encoded_bytes = u64::try_from(bytes.len()).map_err(|_| {
            BorsukError::InvalidRecordInput("lane-log block length exceeds u64".to_string())
        })?;
        if records > MAX_UNMATERIALIZED_RECORDS || encoded_bytes > MAX_UNMATERIALIZED_BYTES {
            return Err(BorsukError::InvalidRecordInput(format!(
                "one lane-log append must fit within {MAX_UNMATERIALIZED_BYTES} bytes and {MAX_UNMATERIALIZED_RECORDS} records"
            )));
        }
        let checksum = *blake3::hash(&bytes).as_bytes();
        Ok(LaneLogBlockRef {
            lease_epoch: self.head.lease_epoch,
            sequence,
            generation,
            checksum,
            bytes: bytes.len() as u64,
            records,
            inline_bytes: Some(bytes),
        })
    }

    fn publish_staged(&mut self, block: LaneLogBlockRef) -> Result<()> {
        self.publish_staged_with_lease_expiry(block, None)
    }

    fn publish_staged_with_lease_expiry(
        &mut self,
        block: LaneLogBlockRef,
        lease_expires_at_ms: Option<u64>,
    ) -> Result<()> {
        let expected_sequence = self.head.committed_sequence.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("lane-log sequence exceeds u64".to_string())
        })?;
        if block.sequence != expected_sequence {
            return Err(BorsukError::InvalidStorage(format!(
                "lane-log block sequence {} does not follow {}",
                block.sequence, self.head.committed_sequence
            )));
        }
        let tail_bytes = self.head.blocks.iter().map(|item| item.bytes).sum::<u64>();
        let tail_records = self
            .head
            .blocks
            .iter()
            .map(|item| item.records)
            .sum::<u64>();
        if self.head.blocks.len() >= MAX_UNMATERIALIZED_BLOCKS
            || tail_bytes.saturating_add(block.bytes) > MAX_UNMATERIALIZED_BYTES
            || tail_records.saturating_add(block.records) > MAX_UNMATERIALIZED_RECORDS
        {
            return Err(BorsukError::IngestBackpressure {
                lane: self.head.lane,
                tail_bytes: tail_bytes.saturating_add(block.bytes),
                tail_records: tail_records.saturating_add(block.records),
                max_bytes: MAX_UNMATERIALIZED_BYTES,
                max_records: MAX_UNMATERIALIZED_RECORDS,
            });
        }
        let mut next = self.head.clone();
        if let Some(lease_expires_at_ms) = lease_expires_at_ms {
            next.lease_expires_at_ms = lease_expires_at_ms;
        }
        next.committed_sequence = block.sequence;
        next.generation_clock = block.generation;
        next.blocks.push(block);
        let path = head_path(self.head.lane);
        let bytes = head_bytes(&next)?;
        match self
            .storage
            .write_coordination_object(&path, &bytes, self.head_version.clone())
        {
            Ok(version) => {
                self.head = next;
                self.head_version = Some(version);
                Ok(())
            }
            Err(
                error @ (BorsukError::ConcurrentModification { .. }
                | BorsukError::ObjectStoreRetryable { .. }),
            ) => self.reconcile_publish(next, error),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn spill_inline_blocks(&mut self) -> Result<()> {
        let inline = self
            .head
            .blocks
            .iter()
            .filter_map(|block| {
                block
                    .inline_bytes
                    .as_ref()
                    .map(|bytes| (block.path(self.head.lane), bytes.as_slice()))
            })
            .collect::<Vec<_>>();
        if inline.is_empty() {
            return Ok(());
        }
        for (path, bytes) in inline {
            self.storage.write_bytes_content_addressed(&path, bytes)?;
        }
        let mut next = self.head.clone();
        for block in &mut next.blocks {
            block.inline_bytes = None;
        }
        let path = head_path(self.head.lane);
        let bytes = head_bytes(&next)?;
        match self
            .storage
            .write_coordination_object(&path, &bytes, self.head_version.clone())
        {
            Ok(version) => {
                self.head = next;
                self.head_version = Some(version);
                Ok(())
            }
            Err(
                error @ (BorsukError::ConcurrentModification { .. }
                | BorsukError::ObjectStoreRetryable { .. }),
            ) => self.reconcile_publish(next, error),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn inline_spill_needed(&self) -> bool {
        let mut bytes = 0_u64;
        for block in &self.head.blocks {
            if block.inline_bytes.is_some() {
                bytes = bytes.saturating_add(block.bytes);
            }
        }
        bytes >= INLINE_SPILL_BYTE_THRESHOLD
    }

    pub(crate) fn mark_materialized_through(&mut self, sequence: u64) -> Result<()> {
        if self.recovery_required {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{}/RECOVERY_REQUIRED", head_path(self.head.lane)),
            });
        }
        if sequence <= self.head.materialized_sequence {
            return Ok(());
        }
        if sequence > self.head.committed_sequence {
            return Err(BorsukError::InvalidStorage(format!(
                "lane-log materialization sequence {sequence} exceeds committed sequence {}",
                self.head.committed_sequence
            )));
        }
        let mut next = self.head.clone();
        next.materialized_sequence = sequence;
        next.blocks.retain(|block| block.sequence > sequence);
        let path = head_path(self.head.lane);
        let bytes = head_bytes(&next)?;
        match self
            .storage
            .write_coordination_object(&path, &bytes, self.head_version.clone())
        {
            Ok(version) => {
                self.head = next;
                self.head_version = Some(version);
                Ok(())
            }
            Err(
                error @ (BorsukError::ConcurrentModification { .. }
                | BorsukError::ObjectStoreRetryable { .. }),
            ) => self.reconcile_publish(next, error),
            Err(error) => Err(error),
        }
    }

    fn reconcile_publish(&mut self, intended: LaneLogHead, error: BorsukError) -> Result<()> {
        let path = head_path(self.head.lane);
        let observed = match self.storage.read_coordination_object(&path) {
            Ok(observed) => observed,
            Err(read_error) => {
                self.recovery_required = true;
                return Err(read_error);
            }
        };
        let Some(stored) = observed else {
            return Err(error);
        };
        if stored.bytes != head_bytes(&intended)? {
            return Err(error);
        }
        self.head = intended;
        self.head_version = Some(stored.version);
        Ok(())
    }

    #[cfg(test)]
    fn append(&mut self, payload: &[u8], records: u64) -> Result<LaneLogReceipt> {
        self.append_with_deltas(payload, records, &[])
    }

    fn append_with_deltas(
        &mut self,
        payload: &[u8],
        records: u64,
        deltas: &[LaneIdDelta],
    ) -> Result<LaneLogReceipt> {
        self.append_with_deltas_and_lease_expiry(payload, records, deltas, None)
    }

    fn append_with_deltas_and_lease_expiry(
        &mut self,
        payload: &[u8],
        records: u64,
        deltas: &[LaneIdDelta],
        lease_expires_at_ms: Option<u64>,
    ) -> Result<LaneLogReceipt> {
        if self.recovery_required {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{}/RECOVERY_REQUIRED", head_path(self.head.lane)),
            });
        }
        let before = self.request_counts();
        let sequence = self.head.committed_sequence.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("lane-log sequence exceeds u64".to_string())
        })?;
        let generation = self.head.generation_clock.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("lane-log generation exceeds u64".to_string())
        })?;
        let block = self.stage_block_with_deltas(sequence, generation, payload, records, deltas)?;
        self.publish_staged_with_lease_expiry(block, lease_expires_at_ms)?;
        let acknowledgement_bytes = u64::try_from(head_bytes(&self.head)?.len()).map_err(|_| {
            BorsukError::InvalidStorage("lane-log HEAD length exceeds u64".to_string())
        })?;
        Ok(LaneLogReceipt {
            lane: self.head.lane,
            lease_epoch: self.head.lease_epoch,
            sequence,
            records,
            acknowledgement_bytes,
            requests: self.request_counts().delta(&before),
        })
    }

    #[cfg(test)]
    fn append_at(&mut self, payload: &[u8], records: u64, now_ms: u64) -> Result<LaneLogReceipt> {
        if now_ms >= self.head.lease_expires_at_ms {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{}/LEASE_EXPIRED", head_path(self.head.lane)),
            });
        }
        self.append(payload, records)
    }

    fn append_insert_at(
        &mut self,
        payload: &[u8],
        ids: &[&[u8]],
        now_ms: u64,
    ) -> Result<LaneLogReceipt> {
        let authority = self.id_authority.as_ref().ok_or_else(|| {
            BorsukError::InvalidStorage(
                "strict lane insert requires an exact ID authority".to_string(),
            )
        })?;
        let (prepared, resident_bytes) = authority.prepare_insert(ids)?;
        if now_ms >= self.head.lease_expires_at_ms {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{}/LEASE_EXPIRED", head_path(self.head.lane)),
            });
        }
        let deltas = prepared
            .iter()
            .map(|id| LaneIdDelta {
                id: id.clone(),
                state: LaneIdDeltaState::Live,
            })
            .collect::<Vec<_>>();
        let receipt = self.append_with_deltas(payload, ids.len() as u64, &deltas)?;
        self.id_authority
            .as_mut()
            .expect("exact authority remains installed")
            .commit_insert(prepared, resident_bytes);
        Ok(receipt)
    }

    fn append_insert_records_at(
        &mut self,
        records: &[VectorRecord],
        dimensions: usize,
        now_ms: u64,
    ) -> Result<LaneLogReceipt> {
        let payload = crate::format::wal_records_to_table(
            records,
            dimensions,
            VectorElementType::Float32,
            PhysicalFormat::Parquet,
        )?;
        let ids = records
            .iter()
            .map(|record| record.id.as_bytes())
            .collect::<Vec<_>>();
        self.append_insert_at(&payload, &ids, now_ms)
    }

    fn append_upsert_at(
        &mut self,
        payload: &[u8],
        ids: &[&[u8]],
        now_ms: u64,
    ) -> Result<LaneLogReceipt> {
        let authority = self.id_authority.as_ref().ok_or_else(|| {
            BorsukError::InvalidStorage("lane upsert requires an exact ID authority".to_string())
        })?;
        let (prepared, resident_bytes) = authority.prepare_upsert(ids)?;
        self.append_id_state_at(
            payload,
            &prepared,
            LaneIdDeltaState::Live,
            now_ms,
            resident_bytes,
        )
    }

    pub(crate) fn append_upsert_records_at(
        &mut self,
        records: &[VectorRecord],
        dimensions: usize,
        now_ms: u64,
    ) -> Result<LaneLogReceipt> {
        let payload = crate::format::wal_records_to_table(
            records,
            dimensions,
            VectorElementType::Float32,
            PhysicalFormat::Parquet,
        )?;
        let ids = records
            .iter()
            .map(|record| record.id.as_bytes())
            .collect::<Vec<_>>();
        self.append_upsert_at(&payload, &ids, now_ms)
    }

    pub(crate) fn append_upsert_records_with_renewal_at(
        &mut self,
        records: &[VectorRecord],
        dimensions: usize,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<LaneLogReceipt> {
        let authority = self.id_authority.as_ref().ok_or_else(|| {
            BorsukError::InvalidStorage("lane upsert requires an exact ID authority".to_string())
        })?;
        let ids = records
            .iter()
            .map(|record| record.id.as_bytes())
            .collect::<Vec<_>>();
        let (prepared, resident_bytes) = authority.prepare_upsert(&ids)?;
        if now_ms >= self.head.lease_expires_at_ms {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{}/LEASE_EXPIRED", head_path(self.head.lane)),
            });
        }
        let lease_expires_at_ms =
            if self.head.lease_expires_at_ms.saturating_sub(now_ms) <= ttl_ms / 2 {
                Some(now_ms.checked_add(ttl_ms).ok_or_else(|| {
                    BorsukError::InvalidRecordInput("lane lease expiry exceeds u64".to_string())
                })?)
            } else {
                None
            };
        let payload = crate::format::wal_records_to_table(
            records,
            dimensions,
            VectorElementType::Float32,
            PhysicalFormat::Parquet,
        )?;
        let deltas = prepared
            .iter()
            .map(|id| LaneIdDelta {
                id: id.clone(),
                state: LaneIdDeltaState::Live,
            })
            .collect::<Vec<_>>();
        let receipt = self.append_with_deltas_and_lease_expiry(
            &payload,
            prepared.len() as u64,
            &deltas,
            lease_expires_at_ms,
        )?;
        self.id_authority
            .as_mut()
            .expect("exact authority remains installed")
            .commit_state(prepared, LaneIdDeltaState::Live, resident_bytes);
        Ok(receipt)
    }

    fn append_delete_at(
        &mut self,
        payload: &[u8],
        ids: &[&[u8]],
        now_ms: u64,
    ) -> Result<LaneLogReceipt> {
        let authority = self.id_authority.as_ref().ok_or_else(|| {
            BorsukError::InvalidStorage("lane delete requires an exact ID authority".to_string())
        })?;
        let prepared = authority.prepare_state_change(ids, LaneIdState::Live, "delete")?;
        let resident_bytes = authority.resident_bytes;
        self.append_id_state_at(
            payload,
            &prepared,
            LaneIdDeltaState::Deleted,
            now_ms,
            resident_bytes,
        )
    }

    fn append_purge_at(
        &mut self,
        payload: &[u8],
        ids: &[&[u8]],
        now_ms: u64,
    ) -> Result<LaneLogReceipt> {
        let authority = self.id_authority.as_ref().ok_or_else(|| {
            BorsukError::InvalidStorage("lane purge requires an exact ID authority".to_string())
        })?;
        let prepared = authority.prepare_state_change(ids, LaneIdState::Deleted, "purge")?;
        let resident_bytes = authority.resident_bytes;
        self.append_id_state_at(
            payload,
            &prepared,
            LaneIdDeltaState::Purged,
            now_ms,
            resident_bytes,
        )
    }

    fn append_id_state_at(
        &mut self,
        payload: &[u8],
        ids: &[Vec<u8>],
        state: LaneIdDeltaState,
        now_ms: u64,
        resident_bytes: u64,
    ) -> Result<LaneLogReceipt> {
        if now_ms >= self.head.lease_expires_at_ms {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{}/LEASE_EXPIRED", head_path(self.head.lane)),
            });
        }
        let deltas = ids
            .iter()
            .map(|id| LaneIdDelta {
                id: id.clone(),
                state,
            })
            .collect::<Vec<_>>();
        let receipt = self.append_with_deltas(payload, ids.len() as u64, &deltas)?;
        self.id_authority
            .as_mut()
            .expect("exact authority remains installed")
            .commit_state(ids.to_vec(), state, resident_bytes);
        Ok(receipt)
    }

    fn renew_at(&mut self, now_ms: u64, ttl_ms: u64) -> Result<()> {
        if self.recovery_required
            || self.head.lease_owner == [0; 16]
            || ttl_ms == 0
            || now_ms >= self.head.lease_expires_at_ms
        {
            return Err(BorsukError::ConcurrentModification {
                path: format!("{}/LEASE_EXPIRED", head_path(self.head.lane)),
            });
        }
        let expires_at_ms = now_ms.checked_add(ttl_ms).ok_or_else(|| {
            BorsukError::InvalidRecordInput("lane lease expiry exceeds u64".to_string())
        })?;
        let mut intended = self.head.clone();
        intended.lease_expires_at_ms = expires_at_ms;
        let path = head_path(self.head.lane);
        let bytes = head_bytes(&intended)?;
        match self
            .storage
            .write_coordination_object(&path, &bytes, self.head_version.clone())
        {
            Ok(version) => {
                self.head = intended;
                self.head_version = Some(version);
                Ok(())
            }
            Err(
                error @ (BorsukError::ConcurrentModification { .. }
                | BorsukError::ObjectStoreRetryable { .. }),
            ) => self.reconcile_publish(intended, error),
            Err(error) => Err(error),
        }
    }

    fn visible_payloads(&self) -> Result<Vec<Vec<u8>>> {
        self.head
            .blocks
            .iter()
            .map(|block| {
                let path = block.path(self.head.lane);
                let bytes = match &block.inline_bytes {
                    Some(bytes) => bytes.clone(),
                    None => self
                        .storage
                        .read_coordination_object(&path)?
                        .map(|stored| stored.bytes)
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(format!(
                                "committed lane-log block `{}` is missing",
                                path
                            ))
                        })?,
                };
                if blake3::hash(&bytes).as_bytes() != &block.checksum {
                    return Err(BorsukError::InvalidStorage(format!(
                        "lane-log block `{}` checksum mismatch",
                        path
                    )));
                }
                Ok(block_payload(&bytes)?.to_vec())
            })
            .collect()
    }

    fn visible_records(&self) -> Result<Vec<VectorRecord>> {
        let mut records = Vec::new();
        for payload in self.visible_payloads()? {
            records.extend(crate::format::wal_records_from_table(
                payload,
                "lane-records.parquet",
            )?);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn writer(uri: &str) -> LaneLogWriter {
        LaneLogWriter::new_empty(Arc::new(InMemory::new()), uri, 3, 7).unwrap()
    }

    #[test]
    fn v27_head_size_is_constant_across_extent_counts() {
        let head = |durable_sequence| LaneEpochHead {
            lane: 3,
            lease_epoch: 7,
            lease_owner: [9; 16],
            lease_expires_at_ms: 123_456,
            durable_sequence,
            materialized_sequence: durable_sequence.saturating_sub(1),
            generation_base: 42,
            sealed_epoch: Some(LaneEpochSeal {
                lease_epoch: 6,
                durable_sequence: 91,
                generation_end: 132,
            }),
        };

        let first = epoch_head_bytes(&head(1)).unwrap();
        let millionth = epoch_head_bytes(&head(1_000_000)).unwrap();

        assert_eq!(first.len(), millionth.len());
        assert_eq!(
            epoch_head_from_bytes(&millionth, 3).unwrap(),
            head(1_000_000)
        );
    }

    #[test]
    fn v27_extent_round_trips_identity_and_records() {
        let extent = LaneExtent {
            lane: 3,
            lease_epoch: 7,
            sequence: 11,
            first_generation: 101,
            records: 2,
            payload: b"two durable records".to_vec(),
        };
        let bytes = extent_bytes(&extent).unwrap();
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = extent_path(3, 7, 11, &checksum).unwrap();

        assert_eq!(
            extent_from_bytes(&path, &bytes, 3, 7, 11, &checksum).unwrap(),
            extent
        );
    }

    #[test]
    fn v27_extent_rejects_path_or_checksum_identity_mismatch() {
        let extent = LaneExtent {
            lane: 3,
            lease_epoch: 7,
            sequence: 11,
            first_generation: 101,
            records: 2,
            payload: b"two durable records".to_vec(),
        };
        let bytes = extent_bytes(&extent).unwrap();
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let path = extent_path(3, 7, 11, &checksum).unwrap();

        for (lane, epoch, sequence) in [(4, 7, 11), (3, 8, 11), (3, 7, 12)] {
            assert!(extent_from_bytes(&path, &bytes, lane, epoch, sequence, &checksum).is_err());
        }
        assert!(extent_from_bytes(&path, &bytes, 3, 7, 11, &"0".repeat(64)).is_err());
        assert!(extent_from_bytes(&path, &bytes[..bytes.len() - 1], 3, 7, 11, &checksum).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(extent_from_bytes(&path, &trailing, 3, 7, 11, &checksum).is_err());
    }

    #[test]
    fn v27_extent_put_is_the_acknowledgement_boundary() {
        let mut writer = LaneEpochWriter::new_empty(
            Arc::new(InMemory::new()),
            "memory:///epoch-extent-ack",
            3,
            7,
            100,
        )
        .unwrap();

        let receipt = writer.append_extent_at(b"durable", 1, 99).unwrap();

        assert_eq!(receipt.lane, 3);
        assert_eq!(receipt.lease_epoch, 7);
        assert_eq!(receipt.sequence, 1);
        assert_eq!(receipt.records, 1);
        assert_eq!(receipt.requests.puts, 1);
        assert_eq!(receipt.requests.gets, 0);
        assert_eq!(writer.head.durable_sequence, 1);
        assert_eq!(
            writer
                .storage
                .list_objects("lane-log/lanes/0003/epochs/0000000000000007/extents")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn v27_extent_completing_after_lease_guard_is_not_acknowledged() {
        let mut writer = LaneEpochWriter::new_empty(
            Arc::new(InMemory::new()),
            "memory:///epoch-expired-extent",
            3,
            7,
            100,
        )
        .unwrap();

        let error = writer.append_extent_at(b"ambiguous", 1, 100).unwrap_err();

        assert!(matches!(error, BorsukError::ConcurrentModification { .. }));
        assert_eq!(writer.head.durable_sequence, 0);
        assert_eq!(
            writer
                .storage
                .list_objects("lane-log/lanes/0003/epochs/0000000000000007/extents")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn v27_linearizable_reader_recovers_extents_beyond_a_stale_watermark() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///epoch-stale-watermark";
        let mut writer = LaneEpochWriter::new_empty(Arc::clone(&store), uri, 3, 7, 100).unwrap();
        writer.append_extent_at(b"first", 1, 10).unwrap();
        writer.append_extent_at(b"second", 1, 11).unwrap();
        let reader = LaneEpochReader::new(Arc::clone(&store), uri, 8).unwrap();

        assert!(
            reader
                .read_lane(3, LaneReadConsistency::Committed)
                .unwrap()
                .is_empty(),
            "the deliberately stale durable watermark remains zero"
        );
        assert_eq!(
            reader
                .read_lane(3, LaneReadConsistency::Linearizable)
                .unwrap()
                .into_iter()
                .map(|extent| extent.payload)
                .collect::<Vec<_>>(),
            vec![b"first".to_vec(), b"second".to_vec()]
        );
    }

    #[test]
    fn v27_sealed_epoch_excludes_a_late_zombie_extent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///epoch-zombie-seal";
        let mut writer = LaneEpochWriter::new_empty(Arc::clone(&store), uri, 3, 7, 100).unwrap();
        writer.append_extent_at(b"acknowledged", 1, 10).unwrap();
        writer.append_extent_at(b"late-zombie", 1, 11).unwrap();
        let stored = writer
            .storage
            .read_coordination_object(&head_path(3))
            .unwrap()
            .unwrap();
        let successor = LaneEpochHead {
            lane: 3,
            lease_epoch: 8,
            lease_owner: [8; 16],
            lease_expires_at_ms: 200,
            durable_sequence: 0,
            materialized_sequence: 0,
            generation_base: 1,
            sealed_epoch: Some(LaneEpochSeal {
                lease_epoch: 7,
                durable_sequence: 1,
                generation_end: 1,
            }),
        };
        writer
            .storage
            .write_coordination_object(
                &head_path(3),
                &epoch_head_bytes(&successor).unwrap(),
                Some(stored.version),
            )
            .unwrap();
        let reader = LaneEpochReader::new(store, uri, 8).unwrap();

        assert_eq!(
            reader
                .read_lane(3, LaneReadConsistency::Linearizable)
                .unwrap()
                .into_iter()
                .map(|extent| extent.payload)
                .collect::<Vec<_>>(),
            vec![b"acknowledged".to_vec()]
        );
    }

    #[test]
    fn warm_lane_append_is_one_conditional_put_and_zero_reads() {
        let mut writer = writer("memory:///lane-one-write-boundary");
        writer.append(b"first", 1).unwrap();

        let receipt = writer.append(b"second", 1).unwrap();

        assert_eq!(receipt.lane, 3);
        assert_eq!(receipt.lease_epoch, 7);
        assert_eq!(receipt.sequence, 2);
        assert_eq!(receipt.records, 1);
        let requests = receipt.requests;
        assert_eq!(requests.puts, 1, "one authoritative HEAD: {requests:?}");
        assert_eq!(
            requests.gets, 0,
            "acknowledgement must not GET: {requests:?}"
        );
        assert_eq!(
            requests.heads, 0,
            "acknowledgement must not HEAD: {requests:?}"
        );
        assert_eq!(
            requests.lists, 0,
            "acknowledgement must not LIST: {requests:?}"
        );
        assert_eq!(
            requests.deletes, 0,
            "acknowledgement must not delete: {requests:?}"
        );
        assert_eq!(
            writer.visible_payloads().unwrap(),
            vec![b"first".to_vec(), b"second".to_vec()]
        );
    }

    #[test]
    fn initialized_lane_set_fails_closed_when_one_authoritative_head_is_missing() {
        const LANES: u16 = 4;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = Storage::from_object_store(
            "memory:///lane-required-heads".to_string(),
            Arc::clone(&store),
        )
        .unwrap();
        initialize_empty_lane_heads(&storage, LANES).unwrap();
        storage.delete_object(&head_path(2)).unwrap();

        let error = LaneLogReader::new(store, "memory:///lane-required-heads", LANES)
            .unwrap()
            .read_records()
            .unwrap_err();

        assert!(error.to_string().contains("lane-log HEAD"));
        assert!(error.to_string().contains('2'));
    }

    #[test]
    fn spill_uploads_inline_blocks_before_replacing_them_with_external_descriptors() {
        let mut writer = writer("memory:///lane-inline-spill");
        writer.append(b"first", 1).unwrap();
        writer.append(b"second", 1).unwrap();
        let before = writer.request_counts();

        writer.spill_inline_blocks().unwrap();

        let requests = writer.request_counts().delta(&before);
        assert_eq!(requests.puts, 3, "two immutable blocks plus one HEAD CAS");
        assert!(
            writer
                .head
                .blocks
                .iter()
                .all(|block| block.inline_bytes.is_none())
        );
        assert_eq!(
            writer.visible_payloads().unwrap(),
            vec![b"first".to_vec(), b"second".to_vec()]
        );
    }

    #[test]
    fn block_without_head_publication_is_invisible_and_retry_safe() {
        let mut writer = writer("memory:///lane-crash-before-head");
        let orphan = writer.stage_block(1, b"orphan", 1).unwrap();
        assert!(writer.visible_payloads().unwrap().is_empty());

        writer.publish_staged(orphan).unwrap();
        assert_eq!(writer.visible_payloads().unwrap(), vec![b"orphan".to_vec()]);
    }

    #[test]
    fn lane_block_and_head_envelopes_reject_corruption() {
        let mut block = block_bytes(b"payload");
        block[20] ^= 0xff;
        assert!(block_payload(&block).is_err());

        let head = LaneLogHead::empty(1, 9);
        let mut bytes = head_bytes(&head).unwrap();
        bytes.push(0);
        assert!(head_from_bytes(&bytes, 1, 9).is_err());
    }

    #[test]
    fn inline_head_uses_its_own_format_v26_envelope() {
        let bytes = head_bytes(&LaneLogHead::empty(1, 9)).unwrap();
        assert_eq!(&bytes[..HEAD_MAGIC.len()], b"BRSLHD26");
    }

    #[test]
    fn mixed_inline_and_external_head_round_trips() {
        let inline = block_bytes(b"inline");
        let mut head = LaneLogHead::empty(3, 7);
        head.committed_sequence = 2;
        head.generation_clock = 2;
        head.blocks = vec![
            LaneLogBlockRef {
                lease_epoch: 7,
                sequence: 1,
                generation: 1,
                checksum: *blake3::hash(&inline).as_bytes(),
                bytes: inline.len() as u64,
                records: 1,
                inline_bytes: Some(inline),
            },
            LaneLogBlockRef {
                lease_epoch: 7,
                sequence: 2,
                generation: 2,
                checksum: [2; CHECKSUM_BYTES],
                bytes: 2,
                records: 1,
                inline_bytes: None,
            },
        ];

        let decoded = head_from_bytes(&head_bytes(&head).unwrap(), 3, 7).unwrap();
        assert_eq!(decoded, head);
    }

    #[test]
    fn inline_head_rejects_invalid_tag_length_and_payload_identity() {
        const HEAD_FIXED_BODY_BYTES: usize = 61;
        const BLOCK_DESCRIPTOR_BYTES_BEFORE_TAG: usize = 72;
        let inline = block_bytes(b"inline");
        let mut head = LaneLogHead::empty(3, 7);
        head.committed_sequence = 1;
        head.generation_clock = 1;
        head.blocks.push(LaneLogBlockRef {
            lease_epoch: 7,
            sequence: 1,
            generation: 1,
            checksum: *blake3::hash(&inline).as_bytes(),
            bytes: inline.len() as u64,
            records: 1,
            inline_bytes: Some(inline),
        });
        let encoded = head_bytes(&head).unwrap();
        let body = fenced_body(&encoded, HEAD_MAGIC, "HEAD").unwrap();
        let tag = HEAD_FIXED_BODY_BYTES + BLOCK_DESCRIPTOR_BYTES_BEFORE_TAG;

        let mut invalid_tag = body.to_vec();
        invalid_tag[tag] = 2;
        assert!(head_from_bytes(&fenced_bytes(HEAD_MAGIC, &invalid_tag), 3, 7).is_err());

        let mut invalid_length = body.to_vec();
        invalid_length[tag + 1..tag + 5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(head_from_bytes(&fenced_bytes(HEAD_MAGIC, &invalid_length), 3, 7).is_err());

        let mut invalid_payload = body.to_vec();
        invalid_payload[tag + 5] ^= 0xff;
        assert!(head_from_bytes(&fenced_bytes(HEAD_MAGIC, &invalid_payload), 3, 7).is_err());
    }

    #[test]
    fn lane_head_rejects_a_gap_in_the_acknowledged_sequence() {
        let mut head = LaneLogHead::empty(3, 7);
        head.committed_sequence = 3;
        head.generation_clock = 3;
        head.blocks = vec![
            LaneLogBlockRef {
                lease_epoch: 7,
                sequence: 1,
                generation: 1,
                checksum: [1; CHECKSUM_BYTES],
                bytes: 1,
                records: 1,
                inline_bytes: None,
            },
            LaneLogBlockRef {
                lease_epoch: 7,
                sequence: 3,
                generation: 3,
                checksum: [3; CHECKSUM_BYTES],
                bytes: 1,
                records: 1,
                inline_bytes: None,
            },
        ];

        assert!(head.validate(3, 7).is_err());
    }

    #[test]
    fn reopened_higher_epoch_writer_preserves_and_extends_the_committed_tail() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut original =
            LaneLogWriter::new_empty(Arc::clone(&store), "memory:///lane-reopen", 3, 7).unwrap();
        original.append(b"before-restart", 1).unwrap();
        drop(original);

        let mut reopened = LaneLogWriter::open(store, "memory:///lane-reopen", 3, 8).unwrap();
        reopened.append(b"after-restart", 1).unwrap();

        assert_eq!(
            reopened.visible_payloads().unwrap(),
            vec![b"before-restart".to_vec(), b"after-restart".to_vec()]
        );
    }

    #[test]
    fn competing_unleased_writer_cannot_rebase_over_the_owned_head() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut owner =
            LaneLogWriter::new_empty(Arc::clone(&store), "memory:///lane-stale-owner", 3, 7)
                .unwrap();
        let mut stale =
            LaneLogWriter::new_empty(store, "memory:///lane-stale-owner", 3, 7).unwrap();

        owner.append(b"owner", 1).unwrap();
        let error = stale.append(b"stale", 1).unwrap_err();

        assert!(
            matches!(error, BorsukError::ConcurrentModification { .. }),
            "a competing owner must fail its HEAD CAS: {error}"
        );
        assert_eq!(owner.visible_payloads().unwrap(), vec![b"owner".to_vec()]);
    }

    #[test]
    fn rejected_head_cas_does_not_commit_prepared_id_authority_state() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-rejected-id-state";
        let empty_authority = || {
            LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 1_024)
                .unwrap()
        };
        let mut owner = LaneLogWriter::new_empty(Arc::clone(&store), uri, 3, 7).unwrap();
        let mut stale = LaneLogWriter::new_empty(store, uri, 3, 7).unwrap();
        owner.id_authority = Some(empty_authority());
        stale.id_authority = Some(empty_authority());
        owner.append(b"owner", 1).unwrap();

        let error = stale
            .append_upsert_records_at(&[VectorRecord::new("rejected", vec![1.0, 0.0])], 2, 1)
            .unwrap_err();

        assert!(matches!(error, BorsukError::ConcurrentModification { .. }));
        assert!(
            !stale
                .id_authority
                .as_ref()
                .unwrap()
                .states
                .contains_key(b"rejected".as_slice())
        );
    }

    #[test]
    fn accepted_head_cas_with_a_lost_response_is_reconciled_as_success() {
        let mut writer = writer("memory:///lane-lost-cas-response");
        let block = writer.stage_block(1, b"durable", 1).unwrap();
        let mut intended = writer.head.clone();
        intended.committed_sequence = 1;
        intended.generation_clock = 1;
        intended.blocks.push(block);
        writer
            .storage
            .write_coordination_object(&head_path(3), &head_bytes(&intended).unwrap(), None)
            .unwrap();

        writer
            .reconcile_publish(
                intended,
                BorsukError::ConcurrentModification { path: head_path(3) },
            )
            .unwrap();

        assert_eq!(
            writer.visible_payloads().unwrap(),
            vec![b"durable".to_vec()]
        );
        writer.append(b"next", 1).unwrap();
    }

    #[test]
    fn maximum_external_descriptor_only_head_stays_below_sixteen_kibibytes() {
        let mut head = LaneLogHead::empty(65_535, u64::MAX);
        for sequence in 1..=MAX_UNMATERIALIZED_BLOCKS as u64 {
            head.blocks.push(LaneLogBlockRef {
                lease_epoch: u64::MAX,
                sequence,
                generation: sequence,
                checksum: [0xff; CHECKSUM_BYTES],
                bytes: 1,
                records: 1,
                inline_bytes: None,
            });
        }
        head.committed_sequence = MAX_UNMATERIALIZED_BLOCKS as u64;
        head.generation_clock = MAX_UNMATERIALIZED_BLOCKS as u64;

        let encoded = head_bytes(&head).unwrap();
        assert!(
            encoded.len() <= 16 * 1024,
            "external descriptor-only HEAD is {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn full_tail_returns_explicit_retryable_backpressure() {
        let mut writer = writer("memory:///lane-tail-backpressure");
        let oversized = LaneLogBlockRef {
            lease_epoch: 7,
            sequence: 1,
            generation: 1,
            checksum: [7; CHECKSUM_BYTES],
            bytes: MAX_UNMATERIALIZED_BYTES + 1,
            records: 1,
            inline_bytes: None,
        };

        let error = writer.publish_staged(oversized).unwrap_err();
        assert!(matches!(error, BorsukError::IngestBackpressure { .. }));
        assert_eq!(error.code(), "ingest_backpressure");
    }

    #[test]
    fn materialization_checkpoint_retires_only_the_published_prefix() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-checkpoint-prefix";
        let mut writer = LaneLogWriter::new_empty(Arc::clone(&store), uri, 3, 1).unwrap();
        writer.append(b"first", 1).unwrap();
        writer.append(b"second", 1).unwrap();

        writer.mark_materialized_through(1).unwrap();

        assert_eq!(writer.head.materialized_sequence, 1);
        assert_eq!(writer.head.committed_sequence, 2);
        assert_eq!(writer.head.blocks.len(), 1);
        assert_eq!(writer.head.blocks[0].sequence, 2);
        assert_eq!(writer.visible_payloads().unwrap(), vec![b"second".to_vec()]);
    }

    #[test]
    fn permanently_oversized_append_fails_before_any_object_store_request() {
        let mut writer = writer("memory:///lane-oversized-append");
        let before = writer.request_counts();

        let error = writer
            .append(&vec![0; MAX_UNMATERIALIZED_BYTES as usize], 1)
            .unwrap_err();

        assert!(matches!(error, BorsukError::InvalidRecordInput(_)));
        assert_eq!(writer.request_counts().delta(&before).total(), 0);
    }

    #[test]
    fn head_lease_takeover_immediately_fences_the_previous_writer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-head-lease-fencing";
        let mut first =
            LaneLogWriter::acquire(Arc::clone(&store), uri, 3, [1; 16], 1_000, 100).unwrap();
        first.append_at(b"first", 1, 1_001).unwrap();

        let mut successor = LaneLogWriter::acquire(store, uri, 3, [2; 16], 1_101, 100).unwrap();
        let before = first.request_counts();
        let error = first.append_at(b"zombie", 1, 1_050).unwrap_err();
        assert!(matches!(error, BorsukError::ConcurrentModification { .. }));
        assert_eq!(first.request_counts().delta(&before).puts, 1);

        successor.append_at(b"successor", 1, 1_102).unwrap();
        assert_eq!(
            successor.visible_payloads().unwrap(),
            vec![b"first".to_vec(), b"successor".to_vec()]
        );
    }

    #[test]
    fn expired_local_lease_rejects_an_append_without_store_requests() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut writer =
            LaneLogWriter::acquire(store, "memory:///lane-local-expiry", 3, [1; 16], 10, 5)
                .unwrap();
        let before = writer.request_counts();

        let error = writer.append_at(b"late", 1, 15).unwrap_err();

        assert!(matches!(error, BorsukError::ConcurrentModification { .. }));
        assert_eq!(writer.request_counts().delta(&before).total(), 0);
    }

    #[test]
    fn unexpired_lane_lease_cannot_be_stolen() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-live-lease";
        let _first = LaneLogWriter::acquire(Arc::clone(&store), uri, 3, [1; 16], 10, 100).unwrap();

        let error = match LaneLogWriter::acquire(store, uri, 3, [2; 16], 50, 100) {
            Ok(_) => panic!("a live lane lease must not be stolen"),
            Err(error) => error,
        };

        assert!(matches!(error, BorsukError::ConcurrentModification { .. }));
    }

    #[test]
    fn lease_renewal_is_one_head_cas_and_extends_local_append_authority() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut writer =
            LaneLogWriter::acquire(store, "memory:///lane-renew", 3, [1; 16], 10, 100).unwrap();
        let before = writer.request_counts();

        writer.renew_at(60, 100).unwrap();

        let requests = writer.request_counts().delta(&before);
        assert_eq!(requests.puts, 1);
        assert_eq!(requests.gets, 0);
        writer.append_at(b"after-original-expiry", 1, 150).unwrap();
    }

    #[test]
    fn long_lived_upsert_renews_before_half_the_lease_remains() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority =
            LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 1_024)
                .unwrap();
        let mut writer = LaneLogWriter::acquire_with_authority(
            store,
            "memory:///lane-auto-renew",
            3,
            [1; 16],
            10,
            100,
            authority,
        )
        .unwrap();

        let receipt = writer
            .append_upsert_records_with_renewal_at(
                &[VectorRecord::new("renewed", vec![1.0, 2.0])],
                2,
                61,
                100,
            )
            .unwrap();

        assert_eq!(receipt.requests.puts, 1);
        assert_eq!(writer.head.lease_expires_at_ms, 161);
    }

    #[test]
    fn combined_renewal_append_reopens_and_fences_the_expired_owner() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-combined-renewal-reopen";
        let empty_authority = || {
            LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 1_024)
                .unwrap()
        };
        let mut first = LaneLogWriter::acquire_with_authority(
            Arc::clone(&store),
            uri,
            3,
            [1; 16],
            10,
            100,
            empty_authority(),
        )
        .unwrap();
        first
            .append_upsert_records_with_renewal_at(
                &[VectorRecord::new("renewed", vec![1.0, 2.0])],
                2,
                61,
                100,
            )
            .unwrap();

        let successor = LaneLogWriter::acquire_with_authority(
            store,
            uri,
            3,
            [2; 16],
            162,
            100,
            empty_authority(),
        )
        .unwrap();
        assert_eq!(successor.head.lease_epoch, first.head.lease_epoch + 1);
        assert_eq!(successor.visible_payloads().unwrap().len(), 1);
        assert!(
            successor
                .id_authority
                .as_ref()
                .unwrap()
                .states
                .contains_key(b"renewed".as_slice())
        );

        let error = first
            .append_upsert_records_with_renewal_at(
                &[VectorRecord::new("zombie", vec![2.0, 1.0])],
                2,
                150,
                100,
            )
            .unwrap_err();
        assert!(matches!(error, BorsukError::ConcurrentModification { .. }));
        assert!(
            !first
                .id_authority
                .as_ref()
                .unwrap()
                .states
                .contains_key(b"zombie".as_slice())
        );
    }

    #[test]
    fn strict_duplicate_is_rejected_from_exact_lane_authority_without_store_io() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority =
            LaneIdAuthority::from_entries([(b"existing".as_slice(), LaneIdState::Live)], 1_024)
                .unwrap();
        let mut writer = LaneLogWriter::acquire_with_authority(
            store,
            "memory:///lane-exact-ids",
            3,
            [1; 16],
            10,
            100,
            authority,
        )
        .unwrap();
        let before = writer.request_counts();

        let error = writer
            .append_insert_at(b"duplicate", &[b"existing".as_slice()], 11)
            .unwrap_err();

        assert!(matches!(error, BorsukError::InvalidRecordInput(_)));
        assert_eq!(writer.request_counts().delta(&before).total(), 0);
    }

    #[test]
    fn exact_id_enters_authority_only_after_durable_head_publication() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority =
            LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 1_024)
                .unwrap();
        let mut writer = LaneLogWriter::acquire_with_authority(
            store,
            "memory:///lane-id-after-ack",
            3,
            [1; 16],
            10,
            100,
            authority,
        )
        .unwrap();

        writer
            .append_insert_at(b"first", &[b"new".as_slice()], 11)
            .unwrap();
        let before = writer.request_counts();
        let error = writer
            .append_insert_at(b"retry", &[b"new".as_slice()], 12)
            .unwrap_err();

        assert!(matches!(error, BorsukError::InvalidRecordInput(_)));
        assert_eq!(writer.request_counts().delta(&before).total(), 0);
    }

    #[test]
    fn committed_lane_record_payload_round_trips_losslessly() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority =
            LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 4_096)
                .unwrap();
        let mut writer = LaneLogWriter::acquire_with_authority(
            store,
            "memory:///lane-record-round-trip",
            3,
            [1; 16],
            10,
            100,
            authority,
        )
        .unwrap();
        let records = vec![
            crate::VectorRecord::new_bytes(vec![0, 255], vec![1.0, 2.0]),
            crate::VectorRecord::new("second", vec![3.0, 4.0]),
        ];

        let receipt = writer.append_insert_records_at(&records, 2, 11).unwrap();

        assert_eq!(receipt.records, 2);
        assert_eq!(writer.visible_records().unwrap(), records);
    }

    #[test]
    fn reader_uses_fixed_lane_head_fanout_without_listing() {
        const LANES: u16 = 4;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-fixed-reader";
        let storage = Storage::from_object_store(uri.to_string(), Arc::clone(&store)).unwrap();
        initialize_empty_lane_heads(&storage, LANES).unwrap();
        drop(storage);
        let records = [
            (0_u16, VectorRecord::new("zero", vec![1.0, 2.0])),
            (2_u16, VectorRecord::new("two", vec![3.0, 4.0])),
        ];
        for (lane, record) in &records {
            let authority =
                LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 1_024)
                    .unwrap();
            let mut writer = LaneLogWriter::acquire_with_authority(
                Arc::clone(&store),
                uri,
                *lane,
                [(*lane as u8) + 1; 16],
                10,
                100,
                authority,
            )
            .unwrap();
            writer
                .append_insert_records_at(std::slice::from_ref(record), 2, 11)
                .unwrap();
        }
        let reader = LaneLogReader::new(store, uri, LANES).unwrap();

        let mut visible = reader.read_records().unwrap();
        visible.sort_by(|left, right| left.id.cmp(&right.id));

        let mut expected = vec![records[1].1.clone(), records[0].1.clone()];
        for record in &mut expected {
            record.generation = 1;
        }
        assert_eq!(
            visible, expected,
            "reader must return records from every configured ownership lane"
        );
        let requests = reader.request_counts();
        assert_eq!(
            requests.gets,
            u64::from(LANES),
            "inline blocks require only fixed HEAD GETs"
        );
        assert_eq!(requests.lists, 0);
        assert_eq!(requests.heads, 0);
    }

    #[test]
    fn unchanged_snapshot_reads_only_the_fixed_head_set() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-unchanged-snapshot";
        let mut writer = LaneLogWriter::acquire_with_authority(
            Arc::clone(&store),
            uri,
            0,
            [1; 16],
            10,
            100,
            LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 1_024)
                .unwrap(),
        )
        .unwrap();
        writer
            .append_insert_records_at(&[VectorRecord::new("first", vec![1.0, 2.0])], 2, 11)
            .unwrap();
        let reader = LaneLogReader::new(store, uri, 1).unwrap();
        let runtime = crate::index::WalTailRuntime::new(1024 * 1024, 1024 * 1024);
        let initial = reader
            .read_snapshot_if_changed(&[], &[], &runtime)
            .unwrap()
            .expect("an empty local snapshot must load the committed lane tail");

        let before = reader.request_counts();
        let unchanged = reader
            .read_snapshot_if_changed(&initial.head_checksums, &initial.record_blocks, &runtime)
            .unwrap();
        let requests = reader.request_counts().delta(&before);

        assert!(unchanged.is_none());
        assert_eq!(
            requests.gets, 1,
            "an unchanged poll reads the one lane HEAD"
        );
        assert_eq!(requests.heads, 0);
        assert_eq!(requests.lists, 0);
    }

    #[test]
    fn advanced_snapshot_fetches_only_new_immutable_blocks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-incremental-snapshot";
        let mut writer = LaneLogWriter::acquire_with_authority(
            Arc::clone(&store),
            uri,
            0,
            [1; 16],
            10,
            100,
            LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 1_024)
                .unwrap(),
        )
        .unwrap();
        writer
            .append_insert_records_at(&[VectorRecord::new("first", vec![1.0, 2.0])], 2, 11)
            .unwrap();
        let reader = LaneLogReader::new(Arc::clone(&store), uri, 1).unwrap();
        let runtime = crate::index::WalTailRuntime::new(0, 1024 * 1024);
        let initial = reader
            .read_snapshot_if_changed(&[], &[], &runtime)
            .unwrap()
            .unwrap();
        writer
            .append_insert_records_at(&[VectorRecord::new("second", vec![3.0, 4.0])], 2, 12)
            .unwrap();

        let before = reader.request_counts();
        let advanced = reader
            .read_snapshot_if_changed(&initial.head_checksums, &initial.record_blocks, &runtime)
            .unwrap()
            .expect("the changed lane HEAD must advance the snapshot");
        let requests = reader.request_counts().delta(&before);

        assert_eq!(
            advanced
                .record_blocks
                .iter()
                .map(|block| block.records.len())
                .sum::<usize>(),
            2
        );
        assert!(Arc::ptr_eq(
            &initial.record_blocks[0].records,
            &advanced.record_blocks[0].records
        ));
        assert_eq!(
            requests.gets, 1,
            "one lane HEAD contains the new inline block"
        );
        assert_eq!(requests.heads, 0);
        assert_eq!(requests.lists, 0);
    }

    #[test]
    fn lane_head_fanout_overlaps_blocking_reads_on_the_shared_io_pool() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);

        let lanes = read_lane_fanout(8, |lane| {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(10));
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(lane)
        })
        .unwrap();

        assert_eq!(lanes, (0_u16..8).collect::<Vec<_>>());
        assert!(peak.load(Ordering::SeqCst) > 1);
        assert!(peak.load(Ordering::SeqCst) <= crate::configured_io_threads());
    }

    #[test]
    fn exact_id_authority_fails_closed_when_its_ram_budget_is_insufficient() {
        let error = LaneIdAuthority::from_entries(
            [(b"too-large-for-budget".as_slice(), LaneIdState::Deleted)],
            1,
        )
        .unwrap_err();

        assert!(matches!(error, BorsukError::RamBudgetExceeded { .. }));
    }

    #[test]
    fn lease_takeover_replays_committed_tail_into_exact_id_authority() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let uri = "memory:///lane-id-tail-recovery";
        let empty = || {
            LaneIdAuthority::from_entries(std::iter::empty::<(&[u8], LaneIdState)>(), 1_024)
                .unwrap()
        };
        let mut first = LaneLogWriter::acquire_with_authority(
            Arc::clone(&store),
            uri,
            3,
            [1; 16],
            10,
            5,
            empty(),
        )
        .unwrap();
        first
            .append_insert_at(b"first", &[b"durable-id".as_slice()], 11)
            .unwrap();
        drop(first);

        let mut successor =
            LaneLogWriter::acquire_with_authority(store, uri, 3, [2; 16], 15, 100, empty())
                .unwrap();
        let before = successor.request_counts();
        let error = successor
            .append_insert_at(b"duplicate", &[b"durable-id".as_slice()], 16)
            .unwrap_err();

        assert!(matches!(error, BorsukError::InvalidRecordInput(_)));
        assert_eq!(successor.request_counts().delta(&before).total(), 0);
    }

    #[test]
    fn unresolved_head_outcome_poison_prevents_a_different_mutation() {
        let mut writer = writer("memory:///lane-ambiguous-poison");
        writer.recovery_required = true;
        let before = writer.request_counts();

        let error = writer.append(b"must-not-stage", 1).unwrap_err();

        assert!(matches!(error, BorsukError::ConcurrentModification { .. }));
        assert_eq!(writer.request_counts().delta(&before).total(), 0);
    }

    #[test]
    fn delete_upsert_and_purge_update_exact_authority_after_ack() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let authority =
            LaneIdAuthority::from_entries([(b"record".as_slice(), LaneIdState::Live)], 1_024)
                .unwrap();
        let mut writer = LaneLogWriter::acquire_with_authority(
            store,
            "memory:///lane-id-transitions",
            3,
            [1; 16],
            10,
            100,
            authority,
        )
        .unwrap();

        writer
            .append_delete_at(b"delete", &[b"record".as_slice()], 11)
            .unwrap();
        assert!(
            writer
                .append_insert_at(b"insert-deleted", &[b"record".as_slice()], 12)
                .is_err(),
            "insert-only remains forbidden until purge"
        );
        writer
            .append_upsert_at(b"upsert", &[b"record".as_slice()], 13)
            .unwrap();
        writer
            .append_delete_at(b"delete-again", &[b"record".as_slice()], 14)
            .unwrap();
        writer
            .append_purge_at(b"purge", &[b"record".as_slice()], 15)
            .unwrap();
        writer
            .append_insert_at(b"insert-after-purge", &[b"record".as_slice()], 16)
            .unwrap();
    }
}
