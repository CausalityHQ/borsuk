use std::collections::BTreeSet;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    BorsukError, Result, manifest::Manifest, positioned_log::PositionedMaterializationWatermark,
    record::VectorKind,
};

const COLLECTION_CODEC_VERSION: u8 = 4;
const COLLECTION_CHECKSUM_LEN: usize = 32;
const COLLECTION_HEADER_LEN: usize = 4 + 1 + 4;
const COLLECTION_WAL_FRONTIER_HEAD_MAGIC: &[u8; 4] = b"BCWH";
const PENDING_COLLECTION_COMMIT_MAGIC: &[u8; 4] = b"BCPC";
const COLLECTION_CONTROL_SCHEMA_VERSION: u8 = 1;
const COLLECTION_CONTROL_MAX_BYTES: usize = 256 * 1024;
const COLLECTION_MAX_MODALITIES: usize = 64;
const COLLECTION_MAX_MODALITY_NAME_BYTES: usize = 128;
const COLLECTION_MAX_RELATIVE_PATH_BYTES: usize = 512;
const COLLECTION_CURRENT_ROLE: &str = "collection_current";
const COLLECTION_SNAPSHOT_ROLE: &str = "collection_snapshot";

pub(crate) const PRIMARY_MODALITY: &str = "@primary";
pub(crate) const COLLECTION_CURRENT: &str = "collection/CURRENT";
pub(crate) const COLLECTION_WAL_FRONTIER_SHARDS: u8 = 64;
/// Hard admission bound for one root shard. A stalled maintenance subsystem
/// cannot make reader traversal grow without limit.
pub(crate) const COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD: u32 = 64;
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CollectionCurrent {
    pub snapshot_path: String,
    pub snapshot_checksum: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CollectionManifestRef {
    pub modality: String,
    pub prefix: String,
    pub version: u64,
    pub manifest_path: String,
    pub manifest_checksum: String,
    pub routing_path: String,
    pub routing_checksum: String,
    pub pivots_path: String,
    pub pivots_checksum: String,
    pub consumed_wal_frontier_checksum: String,
    /// Mandatory manifest/control bytes for paged routing.
    pub resident_bytes_estimate: u64,
    /// Manifest/control/routing/pivot bytes when routing is resident.
    pub resident_routing_bytes_estimate: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CollectionSnapshot {
    pub generation: u64,
    pub schema_fingerprint: String,
    pub previous_snapshot_checksum: Option<String>,
    pub positioned_source_epoch: u64,
    #[serde(with = "positioned_materialization_watermarks_json")]
    pub positioned_materialized_watermarks:
        [PositionedMaterializationWatermark; COLLECTION_WAL_FRONTIER_SHARDS as usize],
    pub modalities: Vec<CollectionManifestRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionDescriptorRef {
    pub modality: String,
    pub prefix: String,
    pub descriptor_path: String,
    pub descriptor_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionCommit {
    pub transaction_id: String,
    pub snapshot_generation: u64,
    pub schema_fingerprint: String,
    pub descriptors: Vec<CollectionDescriptorRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingCollectionCommit {
    pub epoch: String,
    pub created_at_ms: u64,
    pub commit: CollectionCommit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionWalReservation {
    pub transaction_id: String,
    pub schema_fingerprint: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionWalFrontierHead {
    pub generation: u64,
    pub reservations: Vec<CollectionWalReservation>,
    pub transactions: Vec<CollectionCommit>,
}

pub(crate) fn collection_current_bytes(current: &CollectionCurrent) -> Result<Vec<u8>> {
    validate_collection_current(current)?;
    collection_control_json_bytes(COLLECTION_CURRENT_ROLE, current)
}

pub(crate) fn collection_current_from_slice(bytes: &[u8], path: &str) -> Result<CollectionCurrent> {
    let current = collection_control_json_from_bytes(bytes, COLLECTION_CURRENT_ROLE, path)?;
    validate_collection_current(&current)?;
    Ok(current)
}

pub(crate) fn collection_snapshot_bytes(snapshot: &CollectionSnapshot) -> Result<Vec<u8>> {
    validate_collection_snapshot(snapshot)?;
    let bytes = collection_control_json_bytes(COLLECTION_SNAPSHOT_ROLE, snapshot)?;
    if snapshot.previous_snapshot_checksum.is_none() {
        // Admission of a newly created collection must also prove that its
        // first generation advance can add the fixed-width predecessor pin.
        let mut advanced = snapshot.clone();
        advanced.previous_snapshot_checksum = Some("0".repeat(64));
        collection_control_json_bytes(COLLECTION_SNAPSHOT_ROLE, &advanced)?;
    }
    Ok(bytes)
}

pub(crate) fn collection_snapshot_from_slice(
    bytes: &[u8],
    path: &str,
) -> Result<CollectionSnapshot> {
    let snapshot = collection_control_json_from_bytes(bytes, COLLECTION_SNAPSHOT_ROLE, path)?;
    validate_collection_snapshot(&snapshot)?;
    Ok(snapshot)
}

#[derive(Serialize)]
struct CollectionControlDocumentRef<'a, T> {
    schema_version: u8,
    object_role: &'static str,
    payload_checksum_blake3: String,
    payload: &'a T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectionControlDocument<T> {
    schema_version: u8,
    object_role: String,
    payload_checksum_blake3: String,
    payload: T,
}

mod positioned_materialization_watermarks_json {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    use crate::positioned_log::{PositionedMaterializationWatermark, SOURCE_SHARD_COUNT};

    pub(super) fn serialize<S>(
        watermarks: &[PositionedMaterializationWatermark; SOURCE_SHARD_COUNT as usize],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        watermarks.as_slice().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> std::result::Result<
        [PositionedMaterializationWatermark; SOURCE_SHARD_COUNT as usize],
        D::Error,
    >
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<PositionedMaterializationWatermark>::deserialize(deserializer)?;
        values.try_into().map_err(|values: Vec<_>| {
            D::Error::custom(format!(
                "expected exactly {SOURCE_SHARD_COUNT} positioned watermarks, got {}",
                values.len()
            ))
        })
    }
}

fn collection_control_json_bytes<T: Serialize>(role: &'static str, payload: &T) -> Result<Vec<u8>> {
    let payload_bytes = serde_json::to_vec(payload).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "collection {role} payload cannot be encoded: {error}"
        ))
    })?;
    let bytes = serde_json::to_vec(&CollectionControlDocumentRef {
        schema_version: COLLECTION_CONTROL_SCHEMA_VERSION,
        object_role: role,
        payload_checksum_blake3: blake3::hash(&payload_bytes).to_hex().to_string(),
        payload,
    })
    .map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "collection {role} document is not JSON-serializable: {error}"
        ))
    })?;
    if bytes.len() > COLLECTION_CONTROL_MAX_BYTES {
        return Err(BorsukError::InvalidStorage(format!(
            "collection {role} JSON document exceeds {} bytes",
            COLLECTION_CONTROL_MAX_BYTES
        )));
    }
    Ok(bytes)
}

fn collection_control_json_from_bytes<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    expected_role: &'static str,
    path: &str,
) -> Result<T> {
    if bytes.len() > COLLECTION_CONTROL_MAX_BYTES {
        return Err(BorsukError::InvalidStorage(format!(
            "collection {expected_role} JSON document `{path}` exceeds {} bytes",
            COLLECTION_CONTROL_MAX_BYTES
        )));
    }
    let document: CollectionControlDocument<T> =
        serde_json::from_slice(bytes).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "invalid collection {expected_role} JSON document `{path}`: {error}"
            ))
        })?;
    if document.schema_version != COLLECTION_CONTROL_SCHEMA_VERSION
        || document.object_role != expected_role
    {
        return Err(BorsukError::InvalidStorage(format!(
            "unsupported collection {expected_role} JSON schema or object role"
        )));
    }
    let payload_bytes = serde_json::to_vec(&document.payload).map_err(|error| {
        BorsukError::InvalidStorage(format!(
            "collection {expected_role} JSON payload cannot be encoded: {error}"
        ))
    })?;
    let actual_checksum = blake3::hash(&payload_bytes).to_hex().to_string();
    if document.payload_checksum_blake3 != actual_checksum {
        return Err(BorsukError::InvalidStorage(format!(
            "collection {expected_role} JSON payload checksum mismatch"
        )));
    }
    Ok(document.payload)
}

fn write_collection_commit_fields(
    writer: &mut PackedCollectionWriter,
    commit: &CollectionCommit,
) -> Result<()> {
    writer.write_string(&commit.transaction_id, "transaction id")?;
    writer.write_u64(commit.snapshot_generation);
    writer.write_string(&commit.schema_fingerprint, "schema fingerprint")?;
    writer.write_len(commit.descriptors.len(), "commit descriptors")?;
    for reference in &commit.descriptors {
        write_descriptor_ref(writer, reference)?;
    }
    Ok(())
}

fn read_collection_commit_fields(
    reader: &mut PackedCollectionReader<'_>,
) -> Result<CollectionCommit> {
    let transaction_id = reader.read_string("transaction id")?;
    let snapshot_generation = reader.read_u64()?;
    let schema_fingerprint = reader.read_string("schema fingerprint")?;
    let descriptor_count = reader.read_len("commit descriptors")?;
    let mut descriptors = Vec::with_capacity(descriptor_count.min(64));
    for _ in 0..descriptor_count {
        descriptors.push(read_descriptor_ref(reader)?);
    }
    Ok(CollectionCommit {
        transaction_id,
        snapshot_generation,
        schema_fingerprint,
        descriptors,
    })
}

pub(crate) fn pending_collection_commit_path(epoch: &str, transaction_id: &str) -> Result<String> {
    validate_transaction_id(epoch)?;
    validate_transaction_id(transaction_id)?;
    Ok(format!(
        "collection/write-epochs/{epoch}/pending/{transaction_id}.commit"
    ))
}

pub(crate) fn pending_collection_commit_from_slice(
    bytes: &[u8],
    path: &str,
) -> Result<PendingCollectionCommit> {
    let mut reader = PackedCollectionReader::new(bytes, PENDING_COLLECTION_COMMIT_MAGIC, path)?;
    let pending = PendingCollectionCommit {
        epoch: reader.read_string("write epoch")?,
        created_at_ms: reader.read_u64()?,
        commit: read_collection_commit_fields(&mut reader)?,
    };
    reader.finish()?;
    let expected = pending_collection_commit_path(&pending.epoch, &pending.commit.transaction_id)?;
    if path != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "pending collection commit path `{path}` does not match `{expected}`"
        )));
    }
    if pending.created_at_ms == 0 {
        return Err(BorsukError::InvalidStorage(
            "pending collection commit creation time must be non-zero".to_string(),
        ));
    }
    validate_collection_commit(&pending.commit)?;
    Ok(pending)
}

pub(crate) fn collection_wal_frontier_shard(transaction_id: &str) -> Result<u8> {
    validate_transaction_id(transaction_id)?;
    let digest = blake3::hash(transaction_id.as_bytes());
    Ok(digest.as_bytes()[0] % COLLECTION_WAL_FRONTIER_SHARDS)
}

pub(crate) fn collection_wal_frontier_head_path(shard: u8) -> Result<String> {
    validate_collection_wal_frontier_shard(shard)?;
    Ok(format!("collection/wal-frontier/{shard}/HEAD"))
}

pub(crate) fn collection_wal_frontier_head_bytes(
    head: &CollectionWalFrontierHead,
    shard: u8,
) -> Result<Vec<u8>> {
    validate_collection_wal_frontier_shard(shard)?;
    validate_collection_wal_frontier_head(head, shard)?;
    let mut writer = PackedCollectionWriter::new(COLLECTION_WAL_FRONTIER_HEAD_MAGIC);
    writer.write_u64(head.generation);
    writer.write_len(head.reservations.len(), "WAL frontier reservations")?;
    for reservation in &head.reservations {
        writer.write_string(&reservation.transaction_id, "transaction id")?;
        writer.write_string(&reservation.schema_fingerprint, "schema fingerprint")?;
        writer.write_u64(reservation.expires_at_ms);
    }
    writer.write_len(head.transactions.len(), "WAL frontier transactions")?;
    for commit in &head.transactions {
        write_collection_commit_fields(&mut writer, commit)?;
    }
    writer.finish()
}

pub(crate) fn collection_wal_frontier_head_from_slice(
    bytes: &[u8],
    path: &str,
    shard: u8,
) -> Result<CollectionWalFrontierHead> {
    validate_collection_wal_frontier_shard(shard)?;
    let mut reader = PackedCollectionReader::new(bytes, COLLECTION_WAL_FRONTIER_HEAD_MAGIC, path)?;
    let generation = reader.read_u64()?;
    let reservation_count = reader.read_len("WAL frontier reservations")?;
    let mut reservations = Vec::with_capacity(
        reservation_count.min(COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize),
    );
    for _ in 0..reservation_count {
        reservations.push(CollectionWalReservation {
            transaction_id: reader.read_string("transaction id")?,
            schema_fingerprint: reader.read_string("schema fingerprint")?,
            expires_at_ms: reader.read_u64()?,
        });
    }
    let transaction_count = reader.read_len("WAL frontier transactions")?;
    let mut transactions = Vec::with_capacity(
        transaction_count.min(COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize),
    );
    for _ in 0..transaction_count {
        transactions.push(read_collection_commit_fields(&mut reader)?);
    }
    let head = CollectionWalFrontierHead {
        generation,
        reservations,
        transactions,
    };
    reader.finish()?;
    validate_collection_wal_frontier_head(&head, shard)?;
    Ok(head)
}

fn validate_collection_wal_frontier_head(
    head: &CollectionWalFrontierHead,
    shard: u8,
) -> Result<()> {
    if head
        .reservations
        .len()
        .saturating_add(head.transactions.len())
        > COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD as usize
    {
        return Err(BorsukError::InvalidStorage(format!(
            "collection WAL frontier head count {} exceeds hard shard bound {}",
            head.reservations
                .len()
                .saturating_add(head.transactions.len()),
            COLLECTION_WAL_FRONTIER_HARD_TRANSACTIONS_PER_SHARD
        )));
    }
    let mut transaction_ids = BTreeSet::new();
    let mut previous_reservation_id: Option<&str> = None;
    for reservation in &head.reservations {
        validate_transaction_id(&reservation.transaction_id)?;
        validate_checksum(
            &reservation.schema_fingerprint,
            "collection reservation schema fingerprint",
        )?;
        if reservation.expires_at_ms == 0 {
            return Err(BorsukError::InvalidStorage(
                "collection WAL reservation expiry must be non-zero".to_string(),
            ));
        }
        if collection_wal_frontier_shard(&reservation.transaction_id)? != shard {
            return Err(BorsukError::InvalidStorage(format!(
                "collection transaction `{}` does not belong to WAL frontier shard {shard}",
                reservation.transaction_id
            )));
        }
        if previous_reservation_id
            .is_some_and(|previous| previous >= reservation.transaction_id.as_str())
        {
            return Err(BorsukError::InvalidStorage(
                "collection WAL frontier reservations are not in canonical order".to_string(),
            ));
        }
        if !transaction_ids.insert(reservation.transaction_id.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate collection transaction `{}` in WAL frontier",
                reservation.transaction_id
            )));
        }
        previous_reservation_id = Some(&reservation.transaction_id);
    }
    let mut previous_id: Option<&str> = None;
    for commit in &head.transactions {
        validate_collection_commit(commit)?;
        if collection_wal_frontier_shard(&commit.transaction_id)? != shard {
            return Err(BorsukError::InvalidStorage(format!(
                "collection transaction `{}` does not belong to WAL frontier shard {shard}",
                commit.transaction_id
            )));
        }
        if previous_id.is_some_and(|previous| previous >= commit.transaction_id.as_str()) {
            return Err(BorsukError::InvalidStorage(
                "collection WAL frontier transactions must be strictly ordered".to_string(),
            ));
        }
        if !transaction_ids.insert(commit.transaction_id.as_str()) {
            return Err(BorsukError::InvalidStorage(format!(
                "duplicate collection transaction `{}` in WAL frontier",
                commit.transaction_id
            )));
        }
        previous_id = Some(&commit.transaction_id);
    }
    Ok(())
}

fn validate_collection_wal_frontier_shard(shard: u8) -> Result<()> {
    if shard >= COLLECTION_WAL_FRONTIER_SHARDS {
        return Err(BorsukError::InvalidStorage(format!(
            "collection WAL frontier shard must be below {COLLECTION_WAL_FRONTIER_SHARDS}, got {shard}"
        )));
    }
    Ok(())
}

fn validate_collection_snapshot(snapshot: &CollectionSnapshot) -> Result<()> {
    validate_checksum(&snapshot.schema_fingerprint, "schema fingerprint")?;
    if snapshot.positioned_source_epoch == 0 {
        return Err(BorsukError::InvalidStorage(
            "collection positioned source epoch must be positive".to_string(),
        ));
    }
    if let Some(checksum) = &snapshot.previous_snapshot_checksum {
        validate_checksum(checksum, "previous snapshot checksum")?;
    }
    for watermark in &snapshot.positioned_materialized_watermarks {
        watermark.validate()?;
    }
    if snapshot.modalities.len() > COLLECTION_MAX_MODALITIES {
        return Err(BorsukError::InvalidStorage(format!(
            "collection snapshot named modality count exceeds {}",
            COLLECTION_MAX_MODALITIES - 1
        )));
    }
    validate_canonical_modalities(
        snapshot
            .modalities
            .iter()
            .map(|reference| reference.modality.as_str()),
        "snapshot",
    )?;
    for reference in &snapshot.modalities {
        validate_collection_manifest_ref(reference)?;
    }
    Ok(())
}

fn validate_collection_current(current: &CollectionCurrent) -> Result<()> {
    validate_checksum(&current.snapshot_checksum, "snapshot checksum")?;
    validate_relative_path(&current.snapshot_path, "snapshot path")?;
    let expected_path = format!("collection/snapshots/{}.json", current.snapshot_checksum);
    if current.snapshot_path != expected_path {
        return Err(BorsukError::InvalidStorage(format!(
            "collection snapshot path must be `{expected_path}`, got `{}`",
            current.snapshot_path
        )));
    }
    Ok(())
}

fn validate_collection_commit(commit: &CollectionCommit) -> Result<()> {
    validate_transaction_id(&commit.transaction_id)?;
    validate_checksum(&commit.schema_fingerprint, "schema fingerprint")?;
    validate_canonical_modalities(
        commit
            .descriptors
            .iter()
            .map(|reference| reference.modality.as_str()),
        "commit",
    )?;
    for reference in &commit.descriptors {
        validate_modality_prefix(&reference.modality, &reference.prefix)?;
        validate_relative_path(&reference.descriptor_path, "descriptor path")?;
        if !reference.descriptor_path.starts_with(&reference.prefix) {
            return Err(BorsukError::InvalidStorage(format!(
                "collection descriptor path `{}` is outside modality prefix `{}`",
                reference.descriptor_path, reference.prefix
            )));
        }
        let local_path = &reference.descriptor_path[reference.prefix.len()..];
        let expected_prefix = format!("transactions/{}/descriptors/", commit.transaction_id);
        if !local_path.starts_with(&expected_prefix) || !local_path.ends_with(".bin") {
            return Err(BorsukError::InvalidStorage(format!(
                "collection descriptor path `{}` does not belong to transaction `{}`",
                reference.descriptor_path, commit.transaction_id
            )));
        }
        validate_checksum(&reference.descriptor_checksum, "descriptor checksum")?;
    }
    Ok(())
}

pub(crate) fn validate_collection_manifest_ref(reference: &CollectionManifestRef) -> Result<()> {
    validate_modality_prefix(&reference.modality, &reference.prefix)?;
    validate_relative_path(&reference.manifest_path, "manifest path")?;
    validate_relative_path(&reference.routing_path, "routing path")?;
    validate_relative_path(&reference.pivots_path, "pivots path")?;
    for (path, label) in [
        (&reference.manifest_path, "manifest path"),
        (&reference.routing_path, "routing path"),
        (&reference.pivots_path, "pivots path"),
    ] {
        if !path.starts_with(&reference.prefix) {
            return Err(BorsukError::InvalidStorage(format!(
                "collection {label} `{path}` is outside modality prefix `{}`",
                reference.prefix
            )));
        }
    }
    validate_checksum(&reference.manifest_checksum, "manifest checksum")?;
    validate_checksum(&reference.routing_checksum, "routing checksum")?;
    validate_checksum(&reference.pivots_checksum, "pivots checksum")?;
    validate_checksum(
        &reference.consumed_wal_frontier_checksum,
        "consumed WAL frontier checksum",
    )?;
    if reference.resident_bytes_estimate == 0 {
        return Err(BorsukError::InvalidStorage(
            "collection manifest resident byte estimate must be greater than zero".to_string(),
        ));
    }
    if reference.resident_routing_bytes_estimate < reference.resident_bytes_estimate {
        return Err(BorsukError::InvalidStorage(
            "collection resident-routing byte estimate cannot be smaller than the paged estimate"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn collection_modality_prefix(modality: &str) -> Result<String> {
    validate_modality_name(modality)?;
    Ok(if modality == PRIMARY_MODALITY {
        String::new()
    } else {
        format!("vectors/{modality}/")
    })
}

pub(crate) fn consumed_wal_frontier_checksum<'a>(
    runs: impl IntoIterator<Item = &'a str>,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"borsuk.collection.consumed-wal-frontier.v1");
    for run in runs {
        hasher.update(&(run.len() as u64).to_le_bytes());
        hasher.update(run.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

pub(crate) fn collection_schema_fingerprint(manifest: &Manifest) -> String {
    fn update(hasher: &mut blake3::Hasher, value: &[u8]) {
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value);
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"borsuk.collection.schema.v1");
    update(
        &mut hasher,
        &(manifest.config.dimensions as u64).to_le_bytes(),
    );
    update(&mut hasher, manifest.config.metric.to_string().as_bytes());
    update(
        &mut hasher,
        manifest
            .build_config
            .vector_element_type
            .as_str()
            .as_bytes(),
    );
    update(&mut hasher, &[u8::from(manifest.config.text)]);
    update(
        &mut hasher,
        manifest.text_tokenizer.as_deref().unwrap_or("").as_bytes(),
    );
    update(
        &mut hasher,
        &(manifest.config.named_vectors.len() as u64).to_le_bytes(),
    );
    for (name, spec) in &manifest.config.named_vectors {
        update(&mut hasher, name.as_bytes());
        update(&mut hasher, &(spec.dimensions as u64).to_le_bytes());
        update(&mut hasher, spec.metric.to_string().as_bytes());
        update(
            &mut hasher,
            match spec.kind {
                VectorKind::Dense => b"dense",
                VectorKind::Sparse => b"sparse",
                VectorKind::LateInteraction => b"late-interaction",
            },
        );
        update(&mut hasher, spec.element_type.as_str().as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn validate_canonical_modalities<'a>(
    modalities: impl IntoIterator<Item = &'a str>,
    label: &str,
) -> Result<()> {
    let modalities = modalities.into_iter().collect::<Vec<_>>();
    if modalities.is_empty() || modalities[0] != PRIMARY_MODALITY {
        return Err(BorsukError::InvalidStorage(format!(
            "collection {label} must contain `{PRIMARY_MODALITY}` as its first modality"
        )));
    }
    let mut seen = BTreeSet::new();
    for modality in &modalities {
        validate_modality_name(modality)?;
        if !seen.insert(*modality) {
            return Err(BorsukError::InvalidStorage(format!(
                "collection {label} contains duplicate modality `{modality}`"
            )));
        }
    }
    if modalities[1..]
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        return Err(BorsukError::InvalidStorage(format!(
            "collection {label} modalities are not in canonical modality order"
        )));
    }
    Ok(())
}

fn validate_modality_name(modality: &str) -> Result<()> {
    if modality == PRIMARY_MODALITY {
        return Ok(());
    }
    if modality.len() > COLLECTION_MAX_MODALITY_NAME_BYTES {
        return Err(BorsukError::InvalidStorage(format!(
            "collection modality name exceeds {COLLECTION_MAX_MODALITY_NAME_BYTES} bytes"
        )));
    }
    if modality.is_empty()
        || !modality
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(BorsukError::InvalidStorage(format!(
            "collection modality name `{modality}` is invalid"
        )));
    }
    Ok(())
}

fn validate_modality_prefix(modality: &str, prefix: &str) -> Result<()> {
    let expected = collection_modality_prefix(modality)?;
    if prefix != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "collection modality `{modality}` prefix must be `{expected}`, got `{prefix}`"
        )));
    }
    Ok(())
}

fn validate_relative_path(path: &str, label: &str) -> Result<()> {
    if path.len() > COLLECTION_MAX_RELATIVE_PATH_BYTES {
        return Err(BorsukError::InvalidStorage(format!(
            "collection {label} exceeds {COLLECTION_MAX_RELATIVE_PATH_BYTES} bytes"
        )));
    }
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || path.bytes().any(|byte| byte == b'\\' || byte == 0)
    {
        return Err(BorsukError::InvalidStorage(format!(
            "collection {label} `{path}` is not a safe relative path"
        )));
    }
    Ok(())
}

fn validate_transaction_id(transaction_id: &str) -> Result<()> {
    if transaction_id.is_empty()
        || !transaction_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(BorsukError::InvalidStorage(
            "collection transaction id must contain only ASCII letters, digits, '-' or '_'"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_checksum(checksum: &str, label: &str) -> Result<()> {
    if checksum.len() != 64
        || !checksum
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(BorsukError::InvalidStorage(format!(
            "collection {label} must be a 64-character lowercase hexadecimal checksum"
        )));
    }
    Ok(())
}

fn write_descriptor_ref(
    writer: &mut PackedCollectionWriter,
    reference: &CollectionDescriptorRef,
) -> Result<()> {
    writer.write_string(&reference.modality, "modality")?;
    writer.write_string(&reference.prefix, "modality prefix")?;
    writer.write_string(&reference.descriptor_path, "descriptor path")?;
    writer.write_string(&reference.descriptor_checksum, "descriptor checksum")
}

fn read_descriptor_ref(reader: &mut PackedCollectionReader<'_>) -> Result<CollectionDescriptorRef> {
    Ok(CollectionDescriptorRef {
        modality: reader.read_string("modality")?,
        prefix: reader.read_string("modality prefix")?,
        descriptor_path: reader.read_string("descriptor path")?,
        descriptor_checksum: reader.read_string("descriptor checksum")?,
    })
}

struct PackedCollectionWriter {
    magic: &'static [u8; 4],
    payload: Vec<u8>,
}

impl PackedCollectionWriter {
    fn new(magic: &'static [u8; 4]) -> Self {
        Self {
            magic,
            payload: Vec::with_capacity(256),
        }
    }

    fn write_u32(&mut self, value: u32) {
        self.payload.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.payload.extend_from_slice(&value.to_le_bytes());
    }

    fn write_len(&mut self, value: usize, label: &str) -> Result<()> {
        let value = u32::try_from(value).map_err(|_| {
            BorsukError::InvalidStorage(format!("collection {label} count exceeds u32"))
        })?;
        self.write_u32(value);
        Ok(())
    }

    fn write_string(&mut self, value: &str, label: &str) -> Result<()> {
        let length = u32::try_from(value.len()).map_err(|_| {
            BorsukError::InvalidStorage(format!("collection {label} exceeds u32 bytes"))
        })?;
        self.write_u32(length);
        self.payload.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn finish(self) -> Result<Vec<u8>> {
        let payload_length = u32::try_from(self.payload.len()).map_err(|_| {
            BorsukError::InvalidStorage("collection control payload exceeds u32 bytes".to_string())
        })?;
        let mut bytes = Vec::with_capacity(
            COLLECTION_HEADER_LEN + self.payload.len() + COLLECTION_CHECKSUM_LEN,
        );
        bytes.extend_from_slice(self.magic);
        bytes.push(COLLECTION_CODEC_VERSION);
        bytes.extend_from_slice(&payload_length.to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        let checksum = blake3::hash(&bytes);
        bytes.extend_from_slice(checksum.as_bytes());
        Ok(bytes)
    }
}

struct PackedCollectionReader<'a> {
    payload: &'a [u8],
    cursor: usize,
    path: String,
}

impl<'a> PackedCollectionReader<'a> {
    fn new(bytes: &'a [u8], magic: &[u8; 4], path: &str) -> Result<Self> {
        if bytes.len() < COLLECTION_HEADER_LEN + COLLECTION_CHECKSUM_LEN {
            return Err(BorsukError::InvalidStorage(format!(
                "collection control object `{path}` is truncated"
            )));
        }
        if bytes.get(..4) != Some(magic.as_slice()) {
            return Err(BorsukError::InvalidStorage(format!(
                "collection control object `{path}` has invalid magic"
            )));
        }
        if bytes[4] != COLLECTION_CODEC_VERSION {
            return Err(BorsukError::InvalidStorage(format!(
                "collection control object `{path}` uses unsupported codec version {}",
                bytes[4]
            )));
        }
        let payload_length = u32::from_le_bytes(
            bytes[5..9]
                .try_into()
                .expect("collection header contains four payload-length bytes"),
        ) as usize;
        let expected_length = COLLECTION_HEADER_LEN
            .checked_add(payload_length)
            .and_then(|length| length.checked_add(COLLECTION_CHECKSUM_LEN))
            .ok_or_else(|| {
                BorsukError::InvalidStorage(format!(
                    "collection control object `{path}` length overflows usize"
                ))
            })?;
        if bytes.len() != expected_length {
            let detail = if bytes.len() < expected_length {
                "truncated"
            } else {
                "contains trailing bytes"
            };
            return Err(BorsukError::InvalidStorage(format!(
                "collection control object `{path}` {detail}"
            )));
        }
        let checksum_offset = expected_length - COLLECTION_CHECKSUM_LEN;
        if &bytes[checksum_offset..] != blake3::hash(&bytes[..checksum_offset]).as_bytes() {
            return Err(BorsukError::InvalidStorage(format!(
                "collection control object `{path}` checksum mismatch"
            )));
        }
        Ok(Self {
            payload: &bytes[COLLECTION_HEADER_LEN..checksum_offset],
            cursor: 0,
            path: path.to_string(),
        })
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.cursor.checked_add(length).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "collection control object `{}` length overflows usize",
                self.path
            ))
        })?;
        let value = self.payload.get(self.cursor..end).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "collection control object `{}` is truncated",
                self.path
            ))
        })?;
        self.cursor = end;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .expect("collection reader returned four bytes"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .expect("collection reader returned eight bytes"),
        ))
    }

    fn read_len(&mut self, label: &str) -> Result<usize> {
        let length = self.read_u32()? as usize;
        let minimum_bytes = length.checked_mul(4).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "collection {label} count in `{}` overflows usize",
                self.path
            ))
        })?;
        if minimum_bytes > self.payload.len().saturating_sub(self.cursor) {
            return Err(BorsukError::InvalidStorage(format!(
                "collection {label} count in `{}` exceeds the remaining payload",
                self.path
            )));
        }
        Ok(length)
    }

    fn read_string(&mut self, label: &str) -> Result<String> {
        let length = self.read_u32()? as usize;
        let value = self.take(length)?;
        String::from_utf8(value.to_vec()).map_err(|_| {
            BorsukError::InvalidStorage(format!(
                "collection {label} in `{}` is not valid UTF-8",
                self.path
            ))
        })
    }

    fn finish(self) -> Result<()> {
        if self.cursor != self.payload.len() {
            return Err(BorsukError::InvalidStorage(format!(
                "collection control object `{}` contains trailing payload bytes",
                self.path
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checksum(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn manifest_ref(modality: &str, prefix: &str, version: u64) -> CollectionManifestRef {
        CollectionManifestRef {
            modality: modality.to_string(),
            prefix: prefix.to_string(),
            version,
            manifest_path: format!("{prefix}manifest-{version}.parquet"),
            manifest_checksum: checksum('a'),
            routing_path: format!("{prefix}routing-{version}.parquet"),
            routing_checksum: checksum('b'),
            pivots_path: format!("{prefix}pivots-{version}.parquet"),
            pivots_checksum: checksum('c'),
            consumed_wal_frontier_checksum: checksum('d'),
            resident_bytes_estimate: 1_024 + version,
            resident_routing_bytes_estimate: 2_048 + version,
        }
    }

    fn sample_snapshot() -> CollectionSnapshot {
        CollectionSnapshot {
            generation: 7,
            schema_fingerprint: checksum('e'),
            previous_snapshot_checksum: Some(checksum('f')),
            positioned_source_epoch: 3,
            positioned_materialized_watermarks: std::array::from_fn(|_| {
                PositionedMaterializationWatermark::empty()
            }),
            modalities: vec![
                manifest_ref(PRIMARY_MODALITY, "", 3),
                manifest_ref("dense", "vectors/dense/", 4),
                manifest_ref("late", "vectors/late/", 5),
            ],
        }
    }

    fn descriptor_ref(modality: &str, prefix: &str) -> CollectionDescriptorRef {
        CollectionDescriptorRef {
            modality: modality.to_string(),
            prefix: prefix.to_string(),
            descriptor_path: format!("{prefix}transactions/txn-1/descriptors/descriptor.bin"),
            descriptor_checksum: checksum('1'),
        }
    }

    fn sample_commit() -> CollectionCommit {
        CollectionCommit {
            transaction_id: "txn-1".to_string(),
            snapshot_generation: 7,
            schema_fingerprint: checksum('e'),
            descriptors: vec![
                descriptor_ref(PRIMARY_MODALITY, ""),
                descriptor_ref("dense", "vectors/dense/"),
            ],
        }
    }

    fn legacy_packed_control(magic: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9 + payload.len() + 32);
        bytes.extend_from_slice(magic);
        bytes.push(4);
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(payload);
        let checksum = blake3::hash(&bytes);
        bytes.extend_from_slice(checksum.as_bytes());
        bytes
    }

    fn legacy_write_string(payload: &mut Vec<u8>, value: &str) {
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(value.as_bytes());
    }

    fn legacy_manifest_ref(payload: &mut Vec<u8>, reference: &CollectionManifestRef) {
        legacy_write_string(payload, &reference.modality);
        legacy_write_string(payload, &reference.prefix);
        payload.extend_from_slice(&reference.version.to_le_bytes());
        legacy_write_string(payload, &reference.manifest_path);
        legacy_write_string(payload, &reference.manifest_checksum);
        legacy_write_string(payload, &reference.routing_path);
        legacy_write_string(payload, &reference.routing_checksum);
        legacy_write_string(payload, &reference.pivots_path);
        legacy_write_string(payload, &reference.pivots_checksum);
        legacy_write_string(payload, &reference.consumed_wal_frontier_checksum);
        payload.extend_from_slice(&reference.resident_bytes_estimate.to_le_bytes());
        payload.extend_from_slice(&reference.resident_routing_bytes_estimate.to_le_bytes());
    }

    fn legacy_packed_current(current: &CollectionCurrent) -> Vec<u8> {
        let mut payload = Vec::new();
        legacy_write_string(&mut payload, &current.snapshot_path);
        legacy_write_string(&mut payload, &current.snapshot_checksum);
        legacy_packed_control(b"BCCP", &payload)
    }

    fn legacy_packed_snapshot(snapshot: &CollectionSnapshot) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&snapshot.generation.to_le_bytes());
        legacy_write_string(&mut payload, &snapshot.schema_fingerprint);
        match &snapshot.previous_snapshot_checksum {
            Some(checksum) => {
                payload.push(1);
                legacy_write_string(&mut payload, checksum);
            }
            None => payload.push(0),
        }
        payload.extend_from_slice(&snapshot.positioned_source_epoch.to_le_bytes());
        for watermark in &snapshot.positioned_materialized_watermarks {
            payload.extend_from_slice(&watermark.sequence().to_le_bytes());
        }
        payload.extend_from_slice(&(snapshot.modalities.len() as u32).to_le_bytes());
        for reference in &snapshot.modalities {
            legacy_manifest_ref(&mut payload, reference);
        }
        legacy_packed_control(b"BCSN", &payload)
    }

    #[test]
    fn collection_current_round_trips_snapshot_reference() {
        let snapshot_checksum = checksum('a');
        let current = CollectionCurrent {
            snapshot_path: format!("collection/snapshots/{snapshot_checksum}.json"),
            snapshot_checksum,
        };
        let bytes = collection_current_bytes(&current).unwrap();

        assert_eq!(
            collection_current_from_slice(&bytes, "collection/CURRENT").unwrap(),
            current
        );
    }

    #[test]
    fn collection_snapshot_round_trips_canonical_modalities() {
        let snapshot = sample_snapshot();
        let bytes = collection_snapshot_bytes(&snapshot).unwrap();
        assert_eq!(
            collection_snapshot_from_slice(&bytes, "collection/snapshots/test.json").unwrap(),
            snapshot
        );
    }

    fn assert_checked_json_document<T: DeserializeOwned + Serialize>(
        role: &str,
        bytes: &[u8],
    ) -> Vec<u8> {
        assert!(
            bytes.len() <= COLLECTION_CONTROL_MAX_BYTES,
            "{role} control is unbounded"
        );
        let typed: CollectionControlDocument<T> = serde_json::from_slice(bytes)
            .unwrap_or_else(|error| panic!("{role} is not stock UTF-8 JSON: {error}"));
        assert_eq!(typed.object_role, role);
        assert_eq!(typed.schema_version, 1);
        let payload = serde_json::to_vec(&typed.payload).unwrap();
        assert_eq!(
            typed.payload_checksum_blake3,
            blake3::hash(&payload).to_hex().to_string()
        );
        let mut document: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(document["object_role"], role);
        assert_eq!(document["schema_version"], 1);
        document
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), serde_json::Value::Bool(true));
        serde_json::to_vec(&document).unwrap()
    }

    #[test]
    fn collection_current_is_a_checked_bounded_json_document() {
        let bytes = collection_current_bytes(&CollectionCurrent {
            snapshot_path: format!("collection/snapshots/{}.json", checksum('a')),
            snapshot_checksum: checksum('a'),
        })
        .unwrap();
        let unknown =
            assert_checked_json_document::<CollectionCurrent>("collection_current", &bytes);
        let error = collection_current_from_slice(&unknown, "collection/CURRENT").unwrap_err();
        assert!(error.to_string().contains("JSON"), "{error}");
    }

    #[test]
    fn collection_current_checksum_pins_direct_typed_payload_bytes() {
        let current = CollectionCurrent {
            snapshot_path: format!("collection/snapshots/{}.json", checksum('a')),
            snapshot_checksum: checksum('a'),
        };
        let payload = format!(
            "{{\"snapshot_path\":\"collection/snapshots/{}.json\",\"snapshot_checksum\":\"{}\"}}",
            checksum('a'),
            checksum('a')
        );
        let payload_checksum = blake3::hash(payload.as_bytes()).to_hex().to_string();
        let expected = format!(
            "{{\"schema_version\":1,\"object_role\":\"collection_current\",\"payload_checksum_blake3\":\"{payload_checksum}\",\"payload\":{payload}}}"
        );

        assert_eq!(
            collection_current_bytes(&current).unwrap(),
            expected.as_bytes()
        );
        assert_eq!(
            collection_current_bytes(&current).unwrap(),
            expected.as_bytes()
        );
    }

    #[test]
    fn collection_snapshot_is_a_checked_bounded_json_document() {
        let bytes = collection_snapshot_bytes(&sample_snapshot()).unwrap();
        let unknown =
            assert_checked_json_document::<CollectionSnapshot>("collection_snapshot", &bytes);
        let error =
            collection_snapshot_from_slice(&unknown, "collection/snapshots/test.json").unwrap_err();
        assert!(error.to_string().contains("JSON"), "{error}");
    }

    #[test]
    fn collection_current_rejects_old_packed_control() {
        let checksum = checksum('a');
        let current = CollectionCurrent {
            snapshot_path: format!("collection/snapshots/{checksum}.bin"),
            snapshot_checksum: checksum,
        };
        let current_error =
            collection_current_from_slice(&legacy_packed_current(&current), "collection/CURRENT")
                .unwrap_err()
                .to_string();
        assert!(current_error.contains("JSON"), "{current_error}");
    }

    #[test]
    fn collection_snapshot_rejects_old_packed_control() {
        let snapshot_error = collection_snapshot_from_slice(
            &legacy_packed_snapshot(&sample_snapshot()),
            "collection/snapshots/legacy.bin",
        )
        .unwrap_err()
        .to_string();
        assert!(snapshot_error.contains("JSON"), "{snapshot_error}");
    }

    #[test]
    fn snapshot_round_trip_binds_exact_sixty_four_shard_prefix_watermarks() {
        let mut watermarks = std::array::from_fn(|_| {
            crate::positioned_log::PositionedMaterializationWatermark::empty()
        });
        watermarks[3] = watermarks[3].advanced(3, 3, 1, &checksum('1')).unwrap();
        let snapshot = CollectionSnapshot {
            generation: 7,
            schema_fingerprint: checksum('e'),
            previous_snapshot_checksum: Some(checksum('f')),
            positioned_source_epoch: 3,
            positioned_materialized_watermarks: watermarks.clone(),
            modalities: vec![manifest_ref(PRIMARY_MODALITY, "", 3)],
        };

        let bytes = collection_snapshot_bytes(&snapshot).unwrap();
        let decoded =
            collection_snapshot_from_slice(&bytes, "collection/snapshots/test.json").unwrap();
        assert_eq!(decoded.positioned_materialized_watermarks, watermarks);
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn collection_snapshot_limits_admit_sixty_four_modalities_and_future_previous_checksum() {
        let mut snapshot = sample_snapshot();
        snapshot.modalities = std::iter::once(manifest_ref(PRIMARY_MODALITY, "", 1))
            .chain((0..63).map(|ordinal| {
                let modality = format!("m{ordinal:03}");
                let prefix = format!("vectors/{modality}/");
                manifest_ref(&modality, &prefix, ordinal + 2)
            }))
            .collect();
        snapshot.previous_snapshot_checksum = None;

        let initial = collection_snapshot_bytes(&snapshot).unwrap();
        assert!(initial.len() <= COLLECTION_CONTROL_MAX_BYTES);
        snapshot.previous_snapshot_checksum = Some(checksum('f'));
        let advanced = collection_snapshot_bytes(&snapshot).unwrap();
        assert!(advanced.len() <= COLLECTION_CONTROL_MAX_BYTES);
    }

    #[test]
    fn collection_snapshot_joint_worst_case_limits_fit_the_control_envelope() {
        fn exact_path(prefix: &str, marker: char) -> String {
            let suffix = ".parquet";
            let fill = 512_usize.checked_sub(prefix.len() + suffix.len()).unwrap();
            format!("{prefix}{}{suffix}", marker.to_string().repeat(fill))
        }

        let mut snapshot = sample_snapshot();
        snapshot.modalities = std::iter::once(manifest_ref(PRIMARY_MODALITY, "", 1))
            .chain((0..63).map(|ordinal| {
                let stem = format!("m{ordinal:03}");
                let modality = format!("{stem}{}", "n".repeat(128 - stem.len()));
                let prefix = format!("vectors/{modality}/");
                let mut reference = manifest_ref(&modality, &prefix, ordinal + 2);
                reference.manifest_path = exact_path(&prefix, 'm');
                reference.routing_path = exact_path(&prefix, 'r');
                reference.pivots_path = exact_path(&prefix, 'p');
                assert_eq!(modality.len(), 128);
                assert_eq!(reference.manifest_path.len(), 512);
                assert_eq!(reference.routing_path.len(), 512);
                assert_eq!(reference.pivots_path.len(), 512);
                reference
            }))
            .collect();
        snapshot.previous_snapshot_checksum = Some(checksum('f'));

        let bytes = collection_snapshot_bytes(&snapshot).unwrap();
        assert!(
            bytes.len() <= COLLECTION_CONTROL_MAX_BYTES,
            "{} bytes",
            bytes.len()
        );
    }

    #[test]
    fn collection_snapshot_rejects_modality_name_and_path_bound_violations_by_name() {
        let mut too_many = sample_snapshot();
        too_many.modalities = std::iter::once(manifest_ref(PRIMARY_MODALITY, "", 1))
            .chain((0..64).map(|ordinal| {
                let modality = format!("m{ordinal:03}");
                let prefix = format!("vectors/{modality}/");
                manifest_ref(&modality, &prefix, ordinal + 2)
            }))
            .collect();
        let error = collection_snapshot_bytes(&too_many)
            .unwrap_err()
            .to_string();
        assert!(error.contains("named modality count"), "{error}");

        let mut oversized_name = sample_snapshot();
        let name = "n".repeat(129);
        oversized_name.modalities = vec![
            manifest_ref(PRIMARY_MODALITY, "", 1),
            manifest_ref(&name, &format!("vectors/{name}/"), 2),
        ];
        let error = collection_snapshot_bytes(&oversized_name)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("modality name") && error.contains("128"),
            "{error}"
        );

        let mut oversized_path = sample_snapshot();
        oversized_path.modalities[0].manifest_path = format!("{}.parquet", "p".repeat(512));
        let error = collection_snapshot_bytes(&oversized_path)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("manifest path") && error.contains("512"),
            "{error}"
        );
    }

    #[test]
    fn collection_wal_frontier_head_round_trips_embedded_commits() {
        let shard = collection_wal_frontier_shard("txn-1").unwrap();
        let reservation_id = (2..)
            .map(|suffix| format!("txn-{suffix}"))
            .find(|transaction_id| collection_wal_frontier_shard(transaction_id).unwrap() == shard)
            .unwrap();
        let head = CollectionWalFrontierHead {
            generation: 9,
            reservations: vec![CollectionWalReservation {
                transaction_id: reservation_id,
                schema_fingerprint: checksum('d'),
                expires_at_ms: 123_456,
            }],
            transactions: vec![sample_commit()],
        };
        let head_bytes = collection_wal_frontier_head_bytes(&head, shard).unwrap();
        assert_eq!(
            collection_wal_frontier_head_from_slice(
                &head_bytes,
                &format!("collection/wal-frontier/{shard}/HEAD"),
                shard,
            )
            .unwrap(),
            head
        );
    }

    #[test]
    fn collection_wal_frontier_shards_transaction_ids_stably() {
        let first = collection_wal_frontier_shard("txn-1").unwrap();
        assert_eq!(first, collection_wal_frontier_shard("txn-1").unwrap());
        assert!(first < COLLECTION_WAL_FRONTIER_SHARDS);
        assert_ne!(
            collection_wal_frontier_shard("txn-1").unwrap(),
            collection_wal_frontier_shard("txn-2").unwrap()
        );
    }

    #[test]
    fn collection_snapshot_rejects_non_canonical_modalities() {
        let mut snapshot = sample_snapshot();
        snapshot.modalities.swap(1, 2);
        let error = collection_snapshot_bytes(&snapshot).unwrap_err();
        assert!(
            error.to_string().contains("canonical modality order"),
            "{error}"
        );
    }

    #[test]
    fn collection_commit_rejects_duplicate_modalities() {
        let mut commit = sample_commit();
        commit.descriptors.push(commit.descriptors[1].clone());
        let shard = collection_wal_frontier_shard(&commit.transaction_id).unwrap();
        let error = collection_wal_frontier_head_bytes(
            &CollectionWalFrontierHead {
                generation: 1,
                reservations: Vec::new(),
                transactions: vec![commit],
            },
            shard,
        )
        .unwrap_err();
        assert!(error.to_string().contains("duplicate modality"), "{error}");
    }

    #[test]
    fn collection_control_rejects_truncation_checksum_damage_and_trailing_bytes() {
        let bytes = collection_snapshot_bytes(&sample_snapshot()).unwrap();
        let truncated = bytes[..bytes.len() - 1].to_vec();
        let mut damaged = bytes.clone();
        damaged[8] ^= 1;
        let mut trailing = bytes;
        trailing.push(0);

        for invalid in [truncated, damaged, trailing] {
            assert!(collection_snapshot_from_slice(&invalid, "damaged").is_err());
        }
    }

    #[test]
    fn collection_snapshot_rejects_unsafe_prefixes_and_non_lowercase_checksums() {
        let mut unsafe_prefix = sample_snapshot();
        unsafe_prefix.modalities[1].prefix = "../dense/".to_string();
        let error = collection_snapshot_bytes(&unsafe_prefix).unwrap_err();
        assert!(error.to_string().contains("prefix"), "{error}");

        let mut uppercase_checksum = sample_snapshot();
        uppercase_checksum.modalities[0].manifest_checksum = checksum('A');
        let error = collection_snapshot_bytes(&uppercase_checksum).unwrap_err();
        assert!(error.to_string().contains("checksum"), "{error}");
    }
}
