use std::collections::BTreeSet;

use crate::{BorsukError, Result};

const COLLECTION_CODEC_VERSION: u8 = 1;
const COLLECTION_CHECKSUM_LEN: usize = 32;
const COLLECTION_HEADER_LEN: usize = 4 + 1 + 4;
const COLLECTION_SNAPSHOT_MAGIC: &[u8; 4] = b"BCSN";
const COLLECTION_COMMIT_MAGIC: &[u8; 4] = b"BCWC";

pub(crate) const PRIMARY_MODALITY: &str = "@primary";

#[derive(Debug, Clone, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionSnapshot {
    pub generation: u64,
    pub schema_fingerprint: String,
    pub previous_snapshot_checksum: Option<String>,
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

pub(crate) fn collection_snapshot_bytes(snapshot: &CollectionSnapshot) -> Result<Vec<u8>> {
    validate_collection_snapshot(snapshot)?;
    let mut writer = PackedCollectionWriter::new(COLLECTION_SNAPSHOT_MAGIC);
    writer.write_u64(snapshot.generation);
    writer.write_string(&snapshot.schema_fingerprint, "schema fingerprint")?;
    writer.write_optional_string(
        snapshot.previous_snapshot_checksum.as_deref(),
        "previous snapshot checksum",
    )?;
    writer.write_len(snapshot.modalities.len(), "snapshot modalities")?;
    for reference in &snapshot.modalities {
        write_manifest_ref(&mut writer, reference)?;
    }
    writer.finish()
}

pub(crate) fn collection_snapshot_from_slice(
    bytes: &[u8],
    path: &str,
) -> Result<CollectionSnapshot> {
    let mut reader = PackedCollectionReader::new(bytes, COLLECTION_SNAPSHOT_MAGIC, path)?;
    let generation = reader.read_u64()?;
    let schema_fingerprint = reader.read_string("schema fingerprint")?;
    let previous_snapshot_checksum = reader.read_optional_string("previous snapshot checksum")?;
    let modality_count = reader.read_len("snapshot modalities")?;
    let mut modalities = Vec::with_capacity(modality_count.min(64));
    for _ in 0..modality_count {
        modalities.push(read_manifest_ref(&mut reader)?);
    }
    reader.finish()?;
    let snapshot = CollectionSnapshot {
        generation,
        schema_fingerprint,
        previous_snapshot_checksum,
        modalities,
    };
    validate_collection_snapshot(&snapshot)?;
    Ok(snapshot)
}

pub(crate) fn collection_commit_bytes(commit: &CollectionCommit) -> Result<Vec<u8>> {
    validate_collection_commit(commit)?;
    let mut writer = PackedCollectionWriter::new(COLLECTION_COMMIT_MAGIC);
    writer.write_string(&commit.transaction_id, "transaction id")?;
    writer.write_u64(commit.snapshot_generation);
    writer.write_string(&commit.schema_fingerprint, "schema fingerprint")?;
    writer.write_len(commit.descriptors.len(), "commit descriptors")?;
    for reference in &commit.descriptors {
        write_descriptor_ref(&mut writer, reference)?;
    }
    writer.finish()
}

pub(crate) fn collection_commit_from_slice(bytes: &[u8], path: &str) -> Result<CollectionCommit> {
    let mut reader = PackedCollectionReader::new(bytes, COLLECTION_COMMIT_MAGIC, path)?;
    let transaction_id = reader.read_string("transaction id")?;
    let snapshot_generation = reader.read_u64()?;
    let schema_fingerprint = reader.read_string("schema fingerprint")?;
    let descriptor_count = reader.read_len("commit descriptors")?;
    let mut descriptors = Vec::with_capacity(descriptor_count.min(64));
    for _ in 0..descriptor_count {
        descriptors.push(read_descriptor_ref(&mut reader)?);
    }
    reader.finish()?;
    let commit = CollectionCommit {
        transaction_id,
        snapshot_generation,
        schema_fingerprint,
        descriptors,
    };
    validate_collection_commit(&commit)?;
    Ok(commit)
}

fn validate_collection_snapshot(snapshot: &CollectionSnapshot) -> Result<()> {
    validate_checksum(&snapshot.schema_fingerprint, "schema fingerprint")?;
    if let Some(checksum) = &snapshot.previous_snapshot_checksum {
        validate_checksum(checksum, "previous snapshot checksum")?;
    }
    validate_canonical_modalities(
        snapshot
            .modalities
            .iter()
            .map(|reference| reference.modality.as_str()),
        "snapshot",
    )?;
    for reference in &snapshot.modalities {
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
        validate_checksum(&reference.descriptor_checksum, "descriptor checksum")?;
    }
    Ok(())
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
    let expected = if modality == PRIMARY_MODALITY {
        String::new()
    } else {
        format!("vectors/{modality}/")
    };
    if prefix != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "collection modality `{modality}` prefix must be `{expected}`, got `{prefix}`"
        )));
    }
    Ok(())
}

fn validate_relative_path(path: &str, label: &str) -> Result<()> {
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

fn write_manifest_ref(
    writer: &mut PackedCollectionWriter,
    reference: &CollectionManifestRef,
) -> Result<()> {
    writer.write_string(&reference.modality, "modality")?;
    writer.write_string(&reference.prefix, "modality prefix")?;
    writer.write_u64(reference.version);
    writer.write_string(&reference.manifest_path, "manifest path")?;
    writer.write_string(&reference.manifest_checksum, "manifest checksum")?;
    writer.write_string(&reference.routing_path, "routing path")?;
    writer.write_string(&reference.routing_checksum, "routing checksum")?;
    writer.write_string(&reference.pivots_path, "pivots path")?;
    writer.write_string(&reference.pivots_checksum, "pivots checksum")?;
    writer.write_string(
        &reference.consumed_wal_frontier_checksum,
        "consumed WAL frontier checksum",
    )
}

fn read_manifest_ref(reader: &mut PackedCollectionReader<'_>) -> Result<CollectionManifestRef> {
    Ok(CollectionManifestRef {
        modality: reader.read_string("modality")?,
        prefix: reader.read_string("modality prefix")?,
        version: reader.read_u64()?,
        manifest_path: reader.read_string("manifest path")?,
        manifest_checksum: reader.read_string("manifest checksum")?,
        routing_path: reader.read_string("routing path")?,
        routing_checksum: reader.read_string("routing checksum")?,
        pivots_path: reader.read_string("pivots path")?,
        pivots_checksum: reader.read_string("pivots checksum")?,
        consumed_wal_frontier_checksum: reader.read_string("consumed WAL frontier checksum")?,
    })
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

    fn write_u8(&mut self, value: u8) {
        self.payload.push(value);
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

    fn write_optional_string(&mut self, value: Option<&str>, label: &str) -> Result<()> {
        match value {
            Some(value) => {
                self.write_u8(1);
                self.write_string(value, label)
            }
            None => {
                self.write_u8(0);
                Ok(())
            }
        }
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

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
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

    fn read_optional_string(&mut self, label: &str) -> Result<Option<String>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_string(label).map(Some),
            value => Err(BorsukError::InvalidStorage(format!(
                "collection optional {label} in `{}` has invalid tag {value}",
                self.path
            ))),
        }
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
        }
    }

    fn sample_snapshot() -> CollectionSnapshot {
        CollectionSnapshot {
            generation: 7,
            schema_fingerprint: checksum('e'),
            previous_snapshot_checksum: Some(checksum('f')),
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
            descriptor_path: "transactions/txn-1/descriptors/descriptor.bin".to_string(),
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

    #[test]
    fn collection_snapshot_round_trips_canonical_modalities() {
        let snapshot = sample_snapshot();
        let bytes = collection_snapshot_bytes(&snapshot).unwrap();
        assert_eq!(
            collection_snapshot_from_slice(&bytes, "collection/snapshots/test.bin").unwrap(),
            snapshot
        );
    }

    #[test]
    fn collection_commit_round_trips_canonical_modalities() {
        let commit = sample_commit();
        let bytes = collection_commit_bytes(&commit).unwrap();
        assert_eq!(
            collection_commit_from_slice(&bytes, "collection/transactions/txn-1/COMMIT",).unwrap(),
            commit
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
        let error = collection_commit_bytes(&commit).unwrap_err();
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
