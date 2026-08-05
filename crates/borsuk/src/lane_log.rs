//! Format-v25 lane-owned foreground ingest primitive.
#![allow(
    dead_code,
    reason = "staged format-v25 primitive; becomes authoritative when lease and reader cutover lands"
)]

use std::sync::Arc;

use crate::{BorsukError, RequestCounts, Result, storage::Storage};
use object_store::{ObjectStore, UpdateVersion};

const BLOCK_MAGIC: &[u8; 8] = b"BRSLBL25";
const HEAD_MAGIC: &[u8; 8] = b"BRSLHD25";
const CHECKSUM_BYTES: usize = 32;
const MAX_UNMATERIALIZED_BLOCKS: usize = 128;
const MAX_UNMATERIALIZED_BYTES: u64 = 64 * 1024 * 1024;
const MAX_UNMATERIALIZED_RECORDS: u64 = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaneLogBlockRef {
    lease_epoch: u64,
    sequence: u64,
    checksum: [u8; CHECKSUM_BYTES],
    bytes: u64,
    records: u64,
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
    committed_sequence: u64,
    materialized_sequence: u64,
    blocks: Vec<LaneLogBlockRef>,
}

impl LaneLogHead {
    fn empty(lane: u16, lease_epoch: u64) -> Self {
        Self {
            format_version: 25,
            lane,
            lease_epoch,
            committed_sequence: 0,
            materialized_sequence: 0,
            blocks: Vec::new(),
        }
    }

    fn validate(&self, expected_lane: u16, expected_epoch: u64) -> Result<()> {
        if self.format_version != 25
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
        for block in &self.blocks {
            let expected_sequence = previous.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("lane-log sequence exceeds u64".to_string())
            })?;
            if block.sequence != expected_sequence || block.sequence > self.committed_sequence {
                return Err(BorsukError::InvalidStorage(
                    "lane-log HEAD block sequence is not strictly ordered".to_string(),
                ));
            }
            previous = block.sequence;
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

fn block_bytes(payload: &[u8]) -> Vec<u8> {
    fenced_bytes(BLOCK_MAGIC, payload)
}

fn block_payload(bytes: &[u8]) -> Result<&[u8]> {
    fenced_body(bytes, BLOCK_MAGIC, "block")
}

fn head_bytes(head: &LaneLogHead) -> Result<Vec<u8>> {
    head.validate(head.lane, head.lease_epoch)?;
    let block_count = u16::try_from(head.blocks.len()).map_err(|_| {
        BorsukError::InvalidStorage("lane-log HEAD block count exceeds u16".to_string())
    })?;
    let mut body = Vec::with_capacity(29 + head.blocks.len() * 64);
    body.push(head.format_version);
    body.extend_from_slice(&head.lane.to_le_bytes());
    body.extend_from_slice(&head.lease_epoch.to_le_bytes());
    body.extend_from_slice(&head.committed_sequence.to_le_bytes());
    body.extend_from_slice(&head.materialized_sequence.to_le_bytes());
    body.extend_from_slice(&block_count.to_le_bytes());
    for block in &head.blocks {
        body.extend_from_slice(&block.lease_epoch.to_le_bytes());
        body.extend_from_slice(&block.sequence.to_le_bytes());
        body.extend_from_slice(&block.checksum);
        body.extend_from_slice(&block.bytes.to_le_bytes());
        body.extend_from_slice(&block.records.to_le_bytes());
    }
    Ok(fenced_bytes(HEAD_MAGIC, &body))
}

fn head_from_bytes(bytes: &[u8], lane: u16, lease_epoch: u64) -> Result<LaneLogHead> {
    let body = fenced_body(bytes, HEAD_MAGIC, "HEAD")?;
    let mut cursor = 0;
    let format_version = take_u8(body, &mut cursor)?;
    let stored_lane = take_u16(body, &mut cursor)?;
    let stored_epoch = take_u64(body, &mut cursor)?;
    let committed_sequence = take_u64(body, &mut cursor)?;
    let materialized_sequence = take_u64(body, &mut cursor)?;
    let block_count = usize::from(take_u16(body, &mut cursor)?);
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        blocks.push(LaneLogBlockRef {
            lease_epoch: take_u64(body, &mut cursor)?,
            sequence: take_u64(body, &mut cursor)?,
            checksum: take_array(body, &mut cursor)?,
            bytes: take_u64(body, &mut cursor)?,
            records: take_u64(body, &mut cursor)?,
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
        committed_sequence,
        materialized_sequence,
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

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(take_array(bytes, cursor)?))
}

fn head_path(lane: u16) -> String {
    format!("lane-log/lanes/{lane:04}/HEAD")
}

fn block_path(lane: u16, lease_epoch: u64, sequence: u64, checksum: &[u8; 32]) -> String {
    let checksum = blake3::Hash::from_bytes(*checksum).to_hex();
    format!(
        "lane-log/lanes/{lane:04}/epochs/{lease_epoch:020}/blocks/{sequence:020}-{checksum}.blk"
    )
}

/// Single-owner append handle. Lease acquisition and fencing are introduced in
/// stage two; this stage pins the two-write durability boundary and crash seam.
struct LaneLogWriter {
    storage: Storage,
    head: LaneLogHead,
    head_version: Option<UpdateVersion>,
}

impl LaneLogWriter {
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
        })
    }

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
        })
    }

    fn request_counts(&self) -> RequestCounts {
        self.storage.request_counts()
    }

    fn stage_block(&self, sequence: u64, payload: &[u8], records: u64) -> Result<LaneLogBlockRef> {
        if sequence == 0 || records == 0 {
            return Err(BorsukError::InvalidStorage(
                "lane-log blocks require positive sequence and record count".to_string(),
            ));
        }
        let bytes = block_bytes(payload);
        let checksum = *blake3::hash(&bytes).as_bytes();
        let path = block_path(self.head.lane, self.head.lease_epoch, sequence, &checksum);
        self.storage.write_bytes(&path, &bytes)?;
        Ok(LaneLogBlockRef {
            lease_epoch: self.head.lease_epoch,
            sequence,
            checksum,
            bytes: bytes.len() as u64,
            records,
        })
    }

    fn publish_staged(&mut self, block: LaneLogBlockRef) -> Result<()> {
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
        next.committed_sequence = block.sequence;
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

    fn reconcile_publish(&mut self, intended: LaneLogHead, error: BorsukError) -> Result<()> {
        let path = head_path(self.head.lane);
        let Some(stored) = self.storage.read_coordination_object(&path)? else {
            return Err(error);
        };
        if stored.bytes != head_bytes(&intended)? {
            return Err(error);
        }
        self.head = intended;
        self.head_version = Some(stored.version);
        Ok(())
    }

    fn append(&mut self, payload: &[u8], records: u64) -> Result<()> {
        let encoded_bytes = payload
            .len()
            .checked_add(BLOCK_MAGIC.len() + 8 + CHECKSUM_BYTES)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| {
                BorsukError::InvalidRecordInput(
                    "lane-log block encoded length exceeds u64".to_string(),
                )
            })?;
        if records == 0
            || records > MAX_UNMATERIALIZED_RECORDS
            || encoded_bytes > MAX_UNMATERIALIZED_BYTES
        {
            return Err(BorsukError::InvalidRecordInput(format!(
                "one lane-log append must fit within {MAX_UNMATERIALIZED_BYTES} bytes and {MAX_UNMATERIALIZED_RECORDS} records"
            )));
        }
        let sequence = self.head.committed_sequence.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("lane-log sequence exceeds u64".to_string())
        })?;
        let block = self.stage_block(sequence, payload, records)?;
        self.publish_staged(block)
    }

    fn visible_payloads(&self) -> Result<Vec<Vec<u8>>> {
        self.head
            .blocks
            .iter()
            .map(|block| {
                let path = block.path(self.head.lane);
                let bytes = self.storage.read_object_fresh(&path)?.ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "committed lane-log block `{}` is missing",
                        path
                    ))
                })?;
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
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    fn writer(uri: &str) -> LaneLogWriter {
        LaneLogWriter::new_empty(Arc::new(InMemory::new()), uri, 3, 7).unwrap()
    }

    #[test]
    fn warm_lane_append_is_exactly_two_puts_and_zero_reads() {
        let mut writer = writer("memory:///lane-two-write-boundary");
        writer.append(b"first", 1).unwrap();
        let before = writer.request_counts();

        writer.append(b"second", 1).unwrap();

        let requests = writer.request_counts().delta(&before);
        assert_eq!(requests.puts, 2, "one block plus one HEAD: {requests:?}");
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
    fn lane_head_rejects_a_gap_in_the_acknowledged_sequence() {
        let mut head = LaneLogHead::empty(3, 7);
        head.committed_sequence = 3;
        head.blocks = vec![
            LaneLogBlockRef {
                lease_epoch: 7,
                sequence: 1,
                checksum: [1; CHECKSUM_BYTES],
                bytes: 1,
                records: 1,
            },
            LaneLogBlockRef {
                lease_epoch: 7,
                sequence: 3,
                checksum: [3; CHECKSUM_BYTES],
                bytes: 1,
                records: 1,
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
    fn accepted_head_cas_with_a_lost_response_is_reconciled_as_success() {
        let mut writer = writer("memory:///lane-lost-cas-response");
        let block = writer.stage_block(1, b"durable", 1).unwrap();
        let mut intended = writer.head.clone();
        intended.committed_sequence = 1;
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
    fn maximum_bounded_head_stays_below_sixteen_kibibytes() {
        let mut head = LaneLogHead::empty(65_535, u64::MAX);
        for sequence in 1..=MAX_UNMATERIALIZED_BLOCKS as u64 {
            head.blocks.push(LaneLogBlockRef {
                lease_epoch: u64::MAX,
                sequence,
                checksum: [0xff; CHECKSUM_BYTES],
                bytes: 1,
                records: 1,
            });
        }
        head.committed_sequence = MAX_UNMATERIALIZED_BLOCKS as u64;

        let encoded = head_bytes(&head).unwrap();
        assert!(
            encoded.len() <= 16 * 1024,
            "bounded HEAD is {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn full_tail_returns_explicit_retryable_backpressure() {
        let mut writer = writer("memory:///lane-tail-backpressure");
        let oversized = LaneLogBlockRef {
            lease_epoch: 7,
            sequence: 1,
            checksum: [7; CHECKSUM_BYTES],
            bytes: MAX_UNMATERIALIZED_BYTES + 1,
            records: 1,
        };

        let error = writer.publish_staged(oversized).unwrap_err();
        assert!(matches!(error, BorsukError::IngestBackpressure { .. }));
        assert_eq!(error.code(), "ingest_backpressure");
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
}
