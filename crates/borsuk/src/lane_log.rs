//! Read-only adapter and initializer for lane artifacts present in V12 indexes.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    BorsukError, Result, VectorRecord,
    mutation_extent::{
        MutationExtentIdState, MutationExtentIdentity, decode_mutation_extent,
        inspect_mutation_extent,
    },
    storage::Storage,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const ACTIVE_STRIPE_PATH: &str = "lane-log/ACTIVE";
const COORDINATION_SCHEMA_VERSION: u8 = 31;
const EPOCH_HEAD_ROLE: &str = "lane_epoch_head";
const ACTIVE_STRIPE_ROLE: &str = "active_stripe_directory";

/// Fixed group-commit writer-stripe pool. Readers use the active directory and
/// do not fan out across every persisted slot.
pub const GROUP_COMMIT_STRIPE_COUNT: u16 = 64;

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
    pub(crate) lease_epoch: u64,
    pub(crate) sequence: u64,
    pub(crate) bytes: u64,
    pub(crate) records: Arc<Vec<VectorRecord>>,
    pub(crate) generation_fence_ids: Arc<HashSet<Vec<u8>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneIdDeltaState {
    Inserted,
    Live,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LaneIdDelta {
    id: Vec<u8>,
    state: LaneIdDeltaState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaneEpochSeal {
    lease_epoch: u64,
    durable_sequence: u64,
    materialized_sequence: u64,
    materialized_manifest_version: u64,
    max_mutation_hlc: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaneEpochHead {
    lane: u16,
    lease_epoch: u64,
    lease_owner: [u8; 16],
    lease_expires_at_ms: u64,
    durable_sequence: u64,
    materialized_sequence: u64,
    materialized_manifest_version: u64,
    max_mutation_hlc: u64,
    sealed_epoch: Option<LaneEpochSeal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveStripeDirectory {
    generation: u64,
    active_bits: u64,
    #[serde(with = "serde_u64_64")]
    activation_epochs: [u64; 64],
    #[serde(with = "serde_u64_64")]
    retirement_manifest_versions: [u64; 64],
}

mod serde_u64_64 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    pub(super) fn serialize<S>(values: &[u64; 64], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        values.as_slice().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<[u64; 64], D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<u64>::deserialize(deserializer)?;
        let length = values.len();
        values.try_into().map_err(|_| {
            D::Error::custom(format!(
                "active stripe directory requires exactly 64 entries, got {length}"
            ))
        })
    }
}

type ActiveLaneEpochStates = (ActiveStripeDirectory, [u8; 32], Vec<(u16, LaneEpochState)>);

impl ActiveStripeDirectory {
    fn active_stripes_for_manifest(self, lane_count: u16, manifest_version: u64) -> Vec<u16> {
        (0..lane_count)
            .filter(|lane| {
                let bit = 1_u64 << u32::from(*lane);
                self.active_bits & bit != 0
                    || self.retirement_manifest_versions[usize::from(*lane)] > manifest_version
            })
            .collect()
    }
}

impl LaneEpochHead {
    fn validate(&self, expected_lane: u16) -> Result<()> {
        if self.lane != expected_lane
            || self.materialized_sequence > self.durable_sequence
            || (self.materialized_sequence > 0 && self.materialized_manifest_version == 0)
        {
            return Err(BorsukError::InvalidStorage(
                "invalid epoch lane-log HEAD identity or frontier".to_string(),
            ));
        }
        if self.sealed_epoch.is_some_and(|seal| {
            seal.lease_epoch >= self.lease_epoch
                || seal.materialized_sequence > seal.durable_sequence
                || (seal.materialized_sequence > 0 && seal.materialized_manifest_version == 0)
        }) {
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
    records: u64,
    payload: Vec<u8>,
}

impl LaneExtent {
    fn decode_wal_records(&self) -> Result<(Vec<VectorRecord>, Vec<LaneIdDelta>)> {
        let decoded = decode_mutation_extent(&self.payload)?;
        if decoded.identity
            != (MutationExtentIdentity {
                stripe: self.lane,
                lease_epoch: self.lease_epoch,
                sequence: self.sequence,
            })
            || u64::try_from(decoded.mutations.len()).ok() != Some(self.records)
        {
            return Err(BorsukError::InvalidStorage(
                "epoch lane-log extent identity or row count changed during decode".to_string(),
            ));
        }
        let mut records = Vec::with_capacity(decoded.mutations.len());
        let mut deltas = Vec::with_capacity(decoded.mutations.len());
        for (mutation, id_state) in decoded.mutations.into_iter().zip(decoded.id_states) {
            deltas.push(LaneIdDelta {
                id: mutation.id().as_bytes().to_vec(),
                state: match id_state {
                    MutationExtentIdState::Inserted => LaneIdDeltaState::Inserted,
                    MutationExtentIdState::Live => LaneIdDeltaState::Live,
                    MutationExtentIdState::Deleted => LaneIdDeltaState::Deleted,
                },
            });
            if let Some(record) = mutation.into_record() {
                records.push(record);
            }
        }
        Ok((records, deltas))
    }
}

#[derive(Serialize)]
struct CoordinationDocumentRef<'a, T> {
    schema_version: u8,
    object_role: &'static str,
    payload_checksum_blake3: String,
    payload: &'a T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoordinationDocument<T> {
    schema_version: u8,
    object_role: String,
    payload_checksum_blake3: String,
    payload: T,
}

fn coordination_json_bytes<T: Serialize>(role: &'static str, payload: &T) -> Result<Vec<u8>> {
    let payload_bytes = serde_json::to_vec(payload).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "lane-log {role} payload is not JSON-serializable: {error}"
        ))
    })?;
    serde_json::to_vec(&CoordinationDocumentRef {
        schema_version: COORDINATION_SCHEMA_VERSION,
        object_role: role,
        payload_checksum_blake3: blake3::hash(&payload_bytes).to_hex().to_string(),
        payload,
    })
    .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "lane-log {role} document is not JSON-serializable: {error}"
        ))
    })
}

fn coordination_json_from_bytes<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    expected_role: &'static str,
) -> Result<T> {
    let document: CoordinationDocument<T> = serde_json::from_slice(bytes).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "invalid lane-log {expected_role} JSON document: {error}"
        ))
    })?;
    if document.schema_version != COORDINATION_SCHEMA_VERSION
        || document.object_role != expected_role
    {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported lane-log {expected_role} schema or object role"
        )));
    }
    let payload_bytes = serde_json::to_vec(&document.payload).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "lane-log {expected_role} payload cannot be canonicalized: {error}"
        ))
    })?;
    let actual_checksum = blake3::hash(&payload_bytes).to_hex().to_string();
    if document.payload_checksum_blake3 != actual_checksum {
        return Err(BorsukError::InvalidStorage(format!(
            "lane-log {expected_role} checksum mismatch"
        )));
    }
    Ok(document.payload)
}

fn epoch_head_bytes(head: &LaneEpochHead) -> Result<Vec<u8>> {
    head.validate(head.lane)?;
    coordination_json_bytes(EPOCH_HEAD_ROLE, head)
}

fn active_stripe_directory_bytes(directory: &ActiveStripeDirectory) -> Result<Vec<u8>> {
    coordination_json_bytes(ACTIVE_STRIPE_ROLE, directory)
}

fn active_stripe_directory_from_bytes(bytes: &[u8]) -> Result<ActiveStripeDirectory> {
    coordination_json_from_bytes(bytes, ACTIVE_STRIPE_ROLE)
}

fn epoch_head_from_bytes(bytes: &[u8], expected_lane: u16) -> Result<LaneEpochHead> {
    let head: LaneEpochHead = coordination_json_from_bytes(bytes, EPOCH_HEAD_ROLE)?;
    head.validate(expected_lane)?;
    Ok(head)
}

fn extent_path(lane: u16, lease_epoch: u64, sequence: u64) -> Result<String> {
    if sequence == 0 {
        return Err(BorsukError::InvalidStorage(
            "epoch lane-log extent sequence must be positive".to_string(),
        ));
    }
    Ok(format!(
        "lane-log/lanes/{lane:04}/epochs/{lease_epoch:016x}/extents/{sequence:016x}.arrow"
    ))
}

fn extent_from_bytes(
    path: &str,
    bytes: &[u8],
    expected_lane: u16,
    expected_epoch: u64,
    expected_sequence: u64,
) -> Result<LaneExtent> {
    if path != extent_path(expected_lane, expected_epoch, expected_sequence)? {
        return Err(BorsukError::InvalidStorage(
            "epoch lane-log extent path or checksum identity mismatch".to_string(),
        ));
    }
    let metadata = inspect_mutation_extent(bytes)?;
    if metadata.identity
        != (MutationExtentIdentity {
            stripe: expected_lane,
            lease_epoch: expected_epoch,
            sequence: expected_sequence,
        })
    {
        return Err(BorsukError::InvalidStorage(
            "epoch lane-log extent identity or bounds mismatch".to_string(),
        ));
    }
    Ok(LaneExtent {
        lane: expected_lane,
        lease_epoch: expected_epoch,
        sequence: expected_sequence,
        records: u64::try_from(metadata.row_count).map_err(|_| {
            BorsukError::InvalidStorage("epoch lane-log row count exceeds u64".to_string())
        })?,
        payload: bytes.to_vec(),
    })
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
        let head = LaneEpochHead {
            lane,
            lease_epoch: 0,
            lease_owner: [0; 16],
            lease_expires_at_ms: 0,
            durable_sequence: 0,
            materialized_sequence: 0,
            materialized_manifest_version: 0,
            max_mutation_hlc: 0,
            sealed_epoch: None,
        };
        let bytes = epoch_head_bytes(&head)?;
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
    let directory = ActiveStripeDirectory {
        generation: 1,
        active_bits: 0,
        activation_epochs: [0; 64],
        retirement_manifest_versions: [0; 64],
    };
    let bytes = active_stripe_directory_bytes(&directory)?;
    match storage.write_coordination_object(ACTIVE_STRIPE_PATH, &bytes, None) {
        Ok(_) => {}
        Err(BorsukError::ConcurrentModification { .. }) => {
            let observed = storage
                .read_coordination_object(ACTIVE_STRIPE_PATH)?
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "lane-log active stripe directory disappeared during initialization"
                            .to_string(),
                    )
                })?;
            if observed.bytes != bytes {
                return Err(BorsukError::InvalidStorage(
                    "lane-log active stripe directory conflicts with index initialization"
                        .to_string(),
                ));
            }
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

struct LaneEpochReader {
    storage: Storage,
    lane_count: u16,
    manifest_version: u64,
}

struct LaneEpochState {
    head: LaneEpochHead,
    head_checksum: [u8; 32],
    extents: Vec<LaneExtent>,
}

struct LaneEpochIdentityState {
    head_checksum: [u8; 32],
}

fn epoch_state_checksum(
    head_bytes: &[u8],
    sealed_sequence: u64,
    current_sequence: u64,
) -> [u8; 32] {
    let mut identity = blake3::Hasher::new();
    identity.update(head_bytes);
    identity.update(&sealed_sequence.to_le_bytes());
    identity.update(&current_sequence.to_le_bytes());
    *identity.finalize().as_bytes()
}

impl LaneEpochReader {
    fn from_storage(storage: Storage, lane_count: u16) -> Result<Self> {
        if lane_count == 0 || lane_count > 64 {
            return Err(BorsukError::InvalidStorage(
                "epoch lane-log reader lane_count must be between 1 and 64".to_string(),
            ));
        }
        Ok(Self {
            storage,
            lane_count,
            manifest_version: u64::MAX,
        })
    }

    fn from_storage_at_manifest(
        storage: Storage,
        lane_count: u16,
        manifest_version: u64,
    ) -> Result<Self> {
        let mut reader = Self::from_storage(storage, lane_count)?;
        reader.manifest_version = manifest_version;
        Ok(reader)
    }

    fn read_lane_state(&self, lane: u16) -> Result<LaneEpochState> {
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
            let first_sealed = if self.manifest_version >= seal.materialized_manifest_version {
                seal.materialized_sequence.saturating_add(1)
            } else {
                1
            };
            for sequence in first_sealed..=seal.durable_sequence {
                extents.push(
                    self.read_sequence(lane, seal.lease_epoch, sequence)?
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(format!(
                                "sealed epoch lane {lane} epoch {} is missing sequence {sequence}",
                                seal.lease_epoch
                            ))
                        })?,
                );
            }
        }
        let first_current = if self.manifest_version >= head.materialized_manifest_version {
            head.materialized_sequence.saturating_add(1)
        } else {
            1
        };
        for sequence in first_current..=head.durable_sequence {
            extents.push(
                self.read_sequence(lane, head.lease_epoch, sequence)?
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(format!(
                            "durable epoch lane {lane} epoch {} is missing sequence {sequence}",
                            head.lease_epoch
                        ))
                    })?,
            );
        }
        let current_sequence = extents
            .iter()
            .filter(|extent| extent.lease_epoch == head.lease_epoch)
            .map(|extent| extent.sequence)
            .max()
            .unwrap_or(head.durable_sequence)
            .max(head.durable_sequence);
        Ok(LaneEpochState {
            head_checksum: epoch_state_checksum(
                &stored.bytes,
                head.sealed_epoch.map_or(0, |seal| seal.durable_sequence),
                current_sequence,
            ),
            head,
            extents,
        })
    }

    fn read_lane_identity(
        &self,
        lane: u16,
        _known_current: Option<(u64, u64)>,
    ) -> Result<LaneEpochIdentityState> {
        let path = head_path(lane);
        let stored = self
            .storage
            .read_coordination_object(&path)?
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!("epoch lane-log HEAD `{path}` is missing"))
            })?;
        let head = epoch_head_from_bytes(&stored.bytes, lane)?;
        Ok(LaneEpochIdentityState {
            head_checksum: epoch_state_checksum(
                &stored.bytes,
                head.sealed_epoch.map_or(0, |seal| seal.durable_sequence),
                head.durable_sequence,
            ),
        })
    }

    fn read_sequence(
        &self,
        lane: u16,
        lease_epoch: u64,
        sequence: u64,
    ) -> Result<Option<LaneExtent>> {
        let path = extent_path(lane, lease_epoch, sequence)?;
        self.storage
            .read_object_fresh(&path)?
            .map(|bytes| extent_from_bytes(&path, &bytes, lane, lease_epoch, sequence))
            .transpose()
    }
}

/// Fixed-fanout reader for positioned-era lane records still present in V12 indexes.
pub(crate) struct LaneLogReader {
    storage: Storage,
    lane_count: u16,
    manifest_version: u64,
}

impl LaneLogReader {
    pub(crate) fn from_storage(storage: Storage, lane_count: u16) -> Result<Self> {
        if lane_count == 0 || lane_count > 64 {
            return Err(BorsukError::InvalidStorage(
                "lane-log reader lane_count must be between 1 and 64".to_string(),
            ));
        }
        Ok(Self {
            storage,
            lane_count,
            manifest_version: u64::MAX,
        })
    }

    pub(crate) fn from_storage_at_manifest(
        storage: Storage,
        lane_count: u16,
        manifest_version: u64,
    ) -> Result<Self> {
        let mut reader = Self::from_storage(storage, lane_count)?;
        reader.manifest_version = manifest_version;
        Ok(reader)
    }

    fn active_directory(&self) -> Result<(ActiveStripeDirectory, [u8; 32])> {
        let stored = self
            .storage
            .read_coordination_object(ACTIVE_STRIPE_PATH)?
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "lane-log active stripe directory is missing".to_string(),
                )
            })?;
        let directory = active_stripe_directory_from_bytes(&stored.bytes)?;
        Ok((directory, *blake3::hash(&stored.bytes).as_bytes()))
    }

    fn read_epoch_states(&self) -> Result<ActiveLaneEpochStates> {
        let (directory, directory_checksum) = self.active_directory()?;
        let reader = LaneEpochReader::from_storage_at_manifest(
            self.storage.clone(),
            self.lane_count,
            self.manifest_version,
        )?;
        let stripes = directory.active_stripes_for_manifest(self.lane_count, self.manifest_version);
        let states = read_selected_lane_fanout(&stripes, |lane| {
            reader.read_lane_state(lane).map(|state| (lane, state))
        })?;
        Ok((directory, directory_checksum, states))
    }

    fn read_epoch_identities(
        &self,
        current_blocks: &[LaneLogRecordBlock],
    ) -> Result<Vec<[u8; 32]>> {
        let (directory, directory_checksum) = self.active_directory()?;
        let reader = LaneEpochReader::from_storage_at_manifest(
            self.storage.clone(),
            self.lane_count,
            self.manifest_version,
        )?;
        let mut known = vec![None; usize::from(self.lane_count)];
        for block in current_blocks {
            let mut fields = block.key.split(':');
            if fields.next() != Some("lane-epoch") {
                continue;
            }
            let (Some(lane), Some(epoch), Some(sequence)) =
                (fields.next(), fields.next(), fields.next())
            else {
                continue;
            };
            let (Ok(lane), Ok(epoch), Ok(sequence)) = (
                lane.parse::<usize>(),
                epoch.parse::<u64>(),
                sequence.parse::<u64>(),
            ) else {
                continue;
            };
            if lane >= known.len() {
                continue;
            }
            if known[lane].is_none_or(|current| (epoch, sequence) > current) {
                known[lane] = Some((epoch, sequence));
            }
        }
        let stripes = directory.active_stripes_for_manifest(self.lane_count, self.manifest_version);
        let identities = read_selected_lane_fanout(&stripes, |lane| {
            reader
                .read_lane_identity(lane, known[usize::from(lane)])
                .map(|identity| (lane, identity))
        })?;
        let mut checksums = vec![[0; 32]; usize::from(self.lane_count) + 1];
        checksums[usize::from(self.lane_count)] = directory_checksum;
        for (lane, identity) in identities {
            checksums[usize::from(lane)] = identity.head_checksum;
        }
        Ok(checksums)
    }

    fn decode_epoch_snapshot(
        &self,
        states: ActiveLaneEpochStates,
        current_blocks: &[LaneLogRecordBlock],
        runtime: Option<&crate::index::WalTailRuntime>,
    ) -> Result<LaneLogSnapshot> {
        let (_, directory_checksum, states) = states;
        let mut committed_sequences = vec![0; usize::from(self.lane_count)];
        let mut head_checksums = vec![[0; 32]; usize::from(self.lane_count) + 1];
        head_checksums[usize::from(self.lane_count)] = directory_checksum;
        for (lane, state) in &states {
            committed_sequences[usize::from(*lane)] = state
                .extents
                .iter()
                .filter(|extent| extent.lease_epoch == state.head.lease_epoch)
                .map(|extent| extent.sequence)
                .max()
                .unwrap_or(state.head.durable_sequence)
                .max(state.head.durable_sequence);
            head_checksums[usize::from(*lane)] = state.head_checksum;
        }
        let current_blocks = current_blocks
            .iter()
            .map(|block| {
                (
                    block.key.as_str(),
                    (
                        Arc::clone(&block.records),
                        Arc::clone(&block.generation_fence_ids),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let extents = states
            .into_iter()
            .flat_map(|(_, state)| state.extents)
            .collect::<Vec<_>>();
        let record_blocks = crate::parallel::install_io(|| {
            extents
                .into_par_iter()
                .map(|extent| {
                    let payload_checksum = blake3::hash(&extent.payload).to_hex();
                    let key = format!(
                        "lane-epoch:{}:{}:{}:{payload_checksum}",
                        extent.lane, extent.lease_epoch, extent.sequence
                    );
                    let (records, generation_fence_ids) =
                        if let Some((records, fence_ids)) = current_blocks.get(key.as_str()) {
                            (Arc::clone(records), Arc::clone(fence_ids))
                        } else {
                            let bytes = u64::try_from(extent.payload.len()).map_err(|_| {
                                BorsukError::InvalidStorage(
                                    "epoch lane-log payload length exceeds u64".to_string(),
                                )
                            })?;
                            let (decoded_records, deltas) = extent.decode_wal_records()?;
                            let load = || Ok(decoded_records);
                            let records = match runtime {
                                Some(runtime) => runtime.load_record_run(&key, bytes, load)?,
                                None => Arc::new(load()?),
                            };
                            let fence_ids = deltas
                                .into_iter()
                                .filter(|delta| delta.state == LaneIdDeltaState::Live)
                                .map(|delta| delta.id)
                                .collect::<HashSet<_>>();
                            (records, Arc::new(fence_ids))
                        };
                    Ok(LaneLogRecordBlock {
                        key,
                        lane: extent.lane,
                        lease_epoch: extent.lease_epoch,
                        sequence: extent.sequence,
                        bytes: u64::try_from(extent.payload.len()).map_err(|_| {
                            BorsukError::InvalidStorage(
                                "epoch lane-log payload length exceeds u64".to_string(),
                            )
                        })?,
                        records,
                        generation_fence_ids,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(LaneLogSnapshot {
            record_blocks,
            committed_sequences,
            head_checksums,
        })
    }

    pub(crate) fn read_snapshot_if_changed(
        &self,
        current_head_checksums: &[[u8; 32]],
        current_blocks: &[LaneLogRecordBlock],
        runtime: &crate::index::WalTailRuntime,
    ) -> Result<Option<LaneLogSnapshot>> {
        // `read_epoch_identities` decodes every authoritative HEAD below, so
        // it also validates the epoch-v30 format. Avoid a separate lane-zero
        // HEAD read on every WAL-tail poll; active-tail readers already pay the
        // fixed fan-out needed to detect newly created immutable extents.
        let identities = self.read_epoch_identities(current_blocks)?;
        if identities == current_head_checksums {
            return Ok(None);
        }
        let states = self.read_epoch_states()?;
        self.decode_epoch_snapshot(states, current_blocks, Some(runtime))
            .map(Some)
    }
}

fn read_selected_lane_fanout<T, F>(lanes: &[u16], read: F) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(u16) -> Result<T> + Send + Sync,
{
    crate::parallel::install_io(|| {
        lanes
            .par_iter()
            .copied()
            .map(read)
            .collect::<Result<Vec<_>>>()
    })
}
