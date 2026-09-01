use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v24_witness::{V24ObjectIdentity, parse_v24_decimal_source_ordinal, validate_v24_identity},
    v24_witness_graph::{V24WitnessGraph, V24WitnessSearch, normalize_v24_witness_vector},
};

const SOURCE_RECORD_BYTES: usize = 208;
const RUN_PARTITIONS: usize = 256;
const POSTING_CAP: usize = 64;
const V24_POSTING_ASSIGNMENT_EF: usize = 128;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn io_error(path: &Path, source: std::io::Error) -> BorsukError {
    BorsukError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct V24PostingPageRow {
    pub(crate) record_id: Vec<u8>,
    pub(crate) vector: [f32; 96],
}

#[derive(Debug, Clone)]
pub(crate) struct V24PostingPage {
    pub(crate) page_ordinal: u32,
    pub(crate) primary_rows: Vec<V24PostingPageRow>,
    pub(crate) replica_rows: Vec<V24PostingPageRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V24PostingRecord {
    pub(crate) witness_ordinal: u32,
    pub(crate) page_ordinal: u32,
    pub(crate) mass: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V24PostingPlane {
    postings: Vec<Vec<(u32, u32)>>,
    unique_source_rows: u64,
    physical_source_rows: u64,
}

impl V24PostingPlane {
    pub(crate) fn unique_source_rows(&self) -> u64 {
        self.unique_source_rows
    }

    pub(crate) fn physical_source_rows(&self) -> u64 {
        self.physical_source_rows
    }

    pub(crate) fn records_for(&self, witness: u32, cap: usize) -> &[(u32, u32)] {
        let Some(records) = usize::try_from(witness)
            .ok()
            .and_then(|ordinal| self.postings.get(ordinal))
        else {
            return &[];
        };
        &records[..records.len().min(cap)]
    }
}

#[derive(Debug, Clone)]
struct SourceOccurrence {
    source_ordinal: u64,
    page_ordinal: u32,
    replica: bool,
    vector: [f16; 96],
}

fn normalize_row(vector: &[f32; 96]) -> Result<[f16; 96]> {
    let normalized = normalize_v24_witness_vector(vector)?.map(f16::from_f32);
    if normalized.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V24 posting normalized row differs"));
    }
    Ok(normalized)
}

fn source_run_path(scratch: &Path, partition: usize) -> PathBuf {
    scratch.join(format!("source-{partition:03}.run"))
}

fn posting_run_path(scratch: &Path, partition: usize) -> PathBuf {
    scratch.join(format!("posting-{partition:03}.run"))
}

fn cleanup_runs(scratch: &Path) -> Result<()> {
    for partition in 0..RUN_PARTITIONS {
        for path in [
            source_run_path(scratch, partition),
            posting_run_path(scratch, partition),
        ] {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => return Err(io_error(&path, source)),
            }
        }
    }
    Ok(())
}

fn append_source(
    writers: &mut [Option<BufWriter<File>>],
    scratch: &Path,
    occurrence: &SourceOccurrence,
) -> Result<()> {
    let partition = usize::try_from(occurrence.source_ordinal & 0xff).unwrap();
    if writers[partition].is_none() {
        let path = source_run_path(scratch, partition);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        writers[partition] = Some(BufWriter::new(file));
    }
    let writer = writers[partition].as_mut().unwrap();
    writer
        .write_all(&occurrence.source_ordinal.to_le_bytes())
        .map_err(|source| io_error(scratch, source))?;
    writer
        .write_all(&occurrence.page_ordinal.to_le_bytes())
        .map_err(|source| io_error(scratch, source))?;
    writer
        .write_all(&[u8::from(occurrence.replica), 0, 0, 0])
        .map_err(|source| io_error(scratch, source))?;
    for value in occurrence.vector {
        writer
            .write_all(&value.to_bits().to_le_bytes())
            .map_err(|source| io_error(scratch, source))?;
    }
    Ok(())
}

fn decode_source_records(bytes: &[u8]) -> Result<Vec<SourceOccurrence>> {
    if !bytes.len().is_multiple_of(SOURCE_RECORD_BYTES) {
        return Err(invalid("V24 posting source run is truncated"));
    }
    bytes
        .chunks_exact(SOURCE_RECORD_BYTES)
        .map(|record| {
            if record[13..16] != [0, 0, 0] || record[12] > 1 {
                return Err(invalid("V24 posting source run flags differ"));
            }
            let mut vector = [f16::ZERO; 96];
            for (output, encoded) in vector.iter_mut().zip(record[16..].chunks_exact(2)) {
                *output = f16::from_bits(u16::from_le_bytes([encoded[0], encoded[1]]));
            }
            Ok(SourceOccurrence {
                source_ordinal: u64::from_le_bytes(record[0..8].try_into().unwrap()),
                page_ordinal: u32::from_le_bytes(record[8..12].try_into().unwrap()),
                replica: record[12] == 1,
                vector,
            })
        })
        .collect()
}

fn append_posting(
    writers: &mut [Option<BufWriter<File>>],
    scratch: &Path,
    witness: u32,
    page: u32,
) -> Result<()> {
    let partition = usize::try_from((witness >> 12) & 0xff).unwrap();
    if writers[partition].is_none() {
        let path = posting_run_path(scratch, partition);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        writers[partition] = Some(BufWriter::new(file));
    }
    let writer = writers[partition].as_mut().unwrap();
    writer
        .write_all(&witness.to_le_bytes())
        .and_then(|()| writer.write_all(&page.to_le_bytes()))
        .map_err(|source| io_error(scratch, source))
}

fn flush_run_writers(writers: &mut [Option<BufWriter<File>>], scratch: &Path) -> Result<()> {
    for writer in writers.iter_mut().flatten() {
        writer.flush().map_err(|source| io_error(scratch, source))?;
    }
    Ok(())
}

fn build_postings_inner<I>(
    graph: &V24WitnessGraph,
    source_row_count: u64,
    pages: I,
    scratch: &Path,
) -> Result<V24PostingPlane>
where
    I: IntoIterator<Item = Result<V24PostingPage>>,
{
    if source_row_count == 0 {
        return Err(invalid("V24 posting source row count differs"));
    }
    let search = V24WitnessSearch::new(graph)?;
    let source_index = graph.source_index();
    let mut source_writers = (0..RUN_PARTITIONS).map(|_| None).collect::<Vec<_>>();
    let mut page_ordinals = BTreeSet::new();
    let mut physical_source_rows = 0_u64;
    for page in pages {
        let page = page?;
        if !page_ordinals.insert(page.page_ordinal) {
            return Err(invalid("V24 posting page ordinal is duplicated"));
        }
        for (replica, row) in page
            .primary_rows
            .into_iter()
            .map(|row| (false, row))
            .chain(page.replica_rows.into_iter().map(|row| (true, row)))
        {
            let source_ordinal = parse_v24_decimal_source_ordinal(&row.record_id)?;
            if source_ordinal >= source_row_count {
                return Err(invalid(
                    "V24 posting source ordinal exceeds construction rows",
                ));
            }
            append_source(
                &mut source_writers,
                scratch,
                &SourceOccurrence {
                    source_ordinal,
                    page_ordinal: page.page_ordinal,
                    replica,
                    vector: normalize_row(&row.vector)?,
                },
            )?;
            physical_source_rows = physical_source_rows
                .checked_add(1)
                .ok_or_else(|| invalid("V24 posting physical row count overflows"))?;
        }
    }
    flush_run_writers(&mut source_writers, scratch)?;
    drop(source_writers);

    let mut posting_writers = (0..RUN_PARTITIONS).map(|_| None).collect::<Vec<_>>();
    let mut unique_source_rows = 0_u64;
    for partition in 0..RUN_PARTITIONS {
        let path = source_run_path(scratch, partition);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(io_error(&path, source)),
        };
        let mut occurrences = decode_source_records(&bytes)?;
        occurrences.sort_by_key(|row| (row.source_ordinal, row.replica, row.page_ordinal));
        let mut start = 0;
        while start < occurrences.len() {
            let source_ordinal = occurrences[start].source_ordinal;
            let mut end = start + 1;
            while end < occurrences.len() && occurrences[end].source_ordinal == source_ordinal {
                end += 1;
            }
            let group = &occurrences[start..end];
            let primaries = group.iter().filter(|row| !row.replica).collect::<Vec<_>>();
            let replicas = group.iter().filter(|row| row.replica).collect::<Vec<_>>();
            if primaries.len() != 1
                || replicas.len() > 1
                || group.iter().any(|row| row.vector != primaries[0].vector)
                || replicas
                    .first()
                    .is_some_and(|replica| replica.page_ordinal == primaries[0].page_ordinal)
            {
                return Err(invalid("V24 posting source occurrence authority differs"));
            }
            let query = primaries[0].vector.map(f32::from);
            if let Ok(position) =
                source_index.binary_search_by_key(&source_ordinal, |(registered, _)| *registered)
            {
                let witness = source_index[position].1;
                if graph.witness_vector(witness) != Some(&primaries[0].vector) {
                    return Err(invalid("V24 posting witness source vector differs"));
                }
            }
            let nearest =
                search.search(&query, 2, graph.node_count().min(V24_POSTING_ASSIGNMENT_EF))?;
            for witness in nearest {
                append_posting(
                    &mut posting_writers,
                    scratch,
                    witness,
                    primaries[0].page_ordinal,
                )?;
                if let Some(replica) = replicas.first() {
                    append_posting(&mut posting_writers, scratch, witness, replica.page_ordinal)?;
                }
            }
            unique_source_rows = unique_source_rows
                .checked_add(1)
                .ok_or_else(|| invalid("V24 posting unique row count overflows"))?;
            start = end;
        }
        fs::remove_file(&path).map_err(|source| io_error(&path, source))?;
    }
    flush_run_writers(&mut posting_writers, scratch)?;
    drop(posting_writers);

    let mut postings = vec![Vec::new(); graph.node_count()];
    for partition in 0..RUN_PARTITIONS {
        let path = posting_run_path(scratch, partition);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(io_error(&path, source)),
        };
        if !bytes.len().is_multiple_of(8) {
            return Err(invalid("V24 posting accumulation run is truncated"));
        }
        let mut raw = bytes
            .chunks_exact(8)
            .map(|record| {
                (
                    u32::from_le_bytes(record[0..4].try_into().unwrap()),
                    u32::from_le_bytes(record[4..8].try_into().unwrap()),
                )
            })
            .collect::<Vec<_>>();
        raw.sort_unstable();
        let mut start = 0;
        while start < raw.len() {
            let key = raw[start];
            let mut end = start + 1;
            while end < raw.len() && raw[end] == key {
                end += 1;
            }
            let mass =
                u32::try_from(end - start).map_err(|_| invalid("V24 posting mass overflows"))?;
            let witness = usize::try_from(key.0).unwrap();
            if witness >= postings.len() {
                return Err(invalid("V24 posting witness differs"));
            }
            postings[witness].push((key.1, mass));
            start = end;
        }
        fs::remove_file(&path).map_err(|source| io_error(&path, source))?;
    }
    for records in &mut postings {
        records.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
        records.truncate(POSTING_CAP);
    }
    let plane = V24PostingPlane {
        postings,
        unique_source_rows,
        physical_source_rows,
    };
    validate_plane(&plane)?;
    Ok(plane)
}

pub(crate) fn build_v24_witness_postings<I>(
    graph: &V24WitnessGraph,
    source_row_count: u64,
    pages: I,
    scratch: &Path,
) -> Result<V24PostingPlane>
where
    I: IntoIterator<Item = Result<V24PostingPage>>,
{
    if !scratch.is_dir()
        || fs::read_dir(scratch)
            .map_err(|source| io_error(scratch, source))?
            .next()
            .is_some()
    {
        return Err(invalid("V24 posting scratch authority differs"));
    }
    let owner = scratch.join(".v24-posting-owner");
    let mut owner_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&owner)
        .map_err(|source| io_error(&owner, source))?;
    writeln!(owner_file, "{}", std::process::id()).map_err(|source| io_error(&owner, source))?;
    owner_file
        .flush()
        .map_err(|source| io_error(&owner, source))?;
    drop(owner_file);
    let result = build_postings_inner(graph, source_row_count, pages, scratch);
    let cleanup = cleanup_runs(scratch);
    let release = fs::remove_file(&owner).map_err(|source| io_error(&owner, source));
    match (result, cleanup, release) {
        (Ok(plane), Ok(()), Ok(())) => Ok(plane),
        (Err(error), _, _) => Err(error),
        (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
    }
}

fn validate_plane(plane: &V24PostingPlane) -> Result<()> {
    if plane.postings.len() < 2 || plane.postings.iter().all(Vec::is_empty) {
        return Err(invalid("V24 posting plane cardinality differs"));
    }
    for records in &plane.postings {
        let mut pages = BTreeSet::new();
        if records.len() > POSTING_CAP
            || records.iter().any(|(_, mass)| *mass == 0)
            || records.iter().any(|(page, _)| !pages.insert(*page))
            || records.windows(2).any(|pair| {
                pair[0].1 < pair[1].1 || (pair[0].1 == pair[1].1 && pair[0].0 >= pair[1].0)
            })
        {
            return Err(invalid("V24 posting plane ordering differs"));
        }
    }
    Ok(())
}

fn posting_fields() -> Vec<Field> {
    vec![
        Field::new("witness_ordinal", DataType::UInt32, false),
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("mass", DataType::UInt32, false),
    ]
}

fn posting_schema(plane: &V24PostingPlane) -> Schema {
    Schema::new_with_metadata(
        posting_fields(),
        HashMap::from([
            (
                "v24_witness_count".to_owned(),
                plane.postings.len().to_string(),
            ),
            (
                "v24_unique_source_rows".to_owned(),
                plane.unique_source_rows.to_string(),
            ),
            (
                "v24_physical_source_rows".to_owned(),
                plane.physical_source_rows.to_string(),
            ),
        ]),
    )
}

pub(crate) fn write_v24_witness_postings(plane: &V24PostingPlane) -> Result<Vec<u8>> {
    validate_plane(plane)?;
    let records = plane
        .postings
        .iter()
        .enumerate()
        .flat_map(|(witness, records)| {
            records.iter().map(move |(page, mass)| V24PostingRecord {
                witness_ordinal: u32::try_from(witness).unwrap(),
                page_ordinal: *page,
                mass: *mass,
            })
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(posting_schema(plane));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            records.iter().map(|record| record.witness_ordinal),
        )),
        Arc::new(UInt32Array::from_iter_values(
            records.iter().map(|record| record.page_ordinal),
        )),
        Arc::new(UInt32Array::from_iter_values(
            records.iter().map(|record| record.mass),
        )),
    ];
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

pub(crate) fn read_v24_witness_postings(
    bytes: &[u8],
    identity: &V24ObjectIdentity,
    expected_witnesses: usize,
) -> Result<V24PostingPlane> {
    validate_v24_identity(identity, identity)?;
    if identity.role != "witness-postings"
        || identity.encoded_bytes != bytes.len() as u64
        || identity.digest != format!("{:x}", Sha256::digest(bytes))
        || expected_witnesses < 2
    {
        return Err(invalid("V24 posting byte authority differs"));
    }
    let mut reader = FileReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    if schema
        .fields()
        .iter()
        .map(|field| field.as_ref())
        .collect::<Vec<_>>()
        != posting_fields().iter().collect::<Vec<_>>()
        || schema.metadata().len() != 3
    {
        return Err(invalid("V24 posting Arrow schema differs"));
    }
    let metadata_u64 = |key: &str| {
        schema
            .metadata()
            .get(key)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| invalid("V24 posting Arrow metadata differs"))
    };
    let witness_count = metadata_u64("v24_witness_count")?;
    let unique_source_rows = metadata_u64("v24_unique_source_rows")?;
    let physical_source_rows = metadata_u64("v24_physical_source_rows")?;
    if usize::try_from(witness_count).ok() != Some(expected_witnesses)
        || unique_source_rows == 0
        || physical_source_rows < unique_source_rows
        || physical_source_rows > unique_source_rows.saturating_mul(2)
    {
        return Err(invalid("V24 posting Arrow count authority differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V24 posting batch is missing"))??;
    if reader.next().is_some()
        || batch.num_columns() != 3
        || batch.num_rows() == 0
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V24 posting Arrow cardinality differs"));
    }
    let witnesses = batch.columns()[0]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V24 posting witness column differs"))?;
    let pages = batch.columns()[1]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V24 posting page column differs"))?;
    let masses = batch.columns()[2]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V24 posting mass column differs"))?;
    let mut postings = vec![Vec::new(); expected_witnesses];
    let mut previous_witness = None;
    for row in 0..batch.num_rows() {
        let witness = usize::try_from(witnesses.value(row)).unwrap();
        if witness >= expected_witnesses
            || previous_witness.is_some_and(|previous| witness < previous)
        {
            return Err(invalid("V24 posting witness ordinal differs"));
        }
        previous_witness = Some(witness);
        let mass = masses.value(row);
        postings[witness].push((pages.value(row), mass));
    }
    let plane = V24PostingPlane {
        postings,
        unique_source_rows,
        physical_source_rows,
    };
    validate_plane(&plane)?;
    Ok(plane)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use half::f16;
    use sha2::{Digest, Sha256};

    use super::{
        V24PostingPage, V24PostingPageRow, build_v24_witness_postings, read_v24_witness_postings,
        write_v24_witness_postings,
    };
    use crate::{
        v24_witness::V24ObjectIdentity,
        v24_witness_graph::{V24Witness, build_v24_witness_graph},
    };

    const SEED: u64 = 0xd6e8_feb8_6659_fd93;

    fn unit_vector(axis: usize) -> [f32; 96] {
        let mut vector = [0.0_f32; 96];
        vector[axis] = 1.0;
        vector
    }

    fn witnesses() -> Vec<V24Witness> {
        (0_u32..4)
            .map(|witness_ordinal| V24Witness {
                witness_ordinal,
                source_ordinal: 10_000 + u64::from(witness_ordinal),
                vector: unit_vector(usize::try_from(witness_ordinal).unwrap()).map(f16::from_f32),
            })
            .collect()
    }

    fn row(source_ordinal: u64, axis: usize) -> V24PostingPageRow {
        V24PostingPageRow {
            record_id: source_ordinal.to_string().into_bytes(),
            vector: unit_vector(axis),
        }
    }

    fn scratch(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "borsuk-v24-postings-{label}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).unwrap();
        path
    }

    fn identity(bytes: &[u8]) -> V24ObjectIdentity {
        V24ObjectIdentity {
            role: "witness-postings".to_owned(),
            uri: "s3://borsuk-v24/witness-postings.arrow".to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: format!("{:x}", Sha256::digest(bytes)),
            encoded_bytes: bytes.len() as u64,
            generation: "generation-witness-postings".to_owned(),
        }
    }

    #[test]
    fn v24_witness_postings_bind_decimal_dataset_ids_not_page_or_leaf_position() {
        let graph = build_v24_witness_graph(&witnesses(), SEED).unwrap();
        let pages = vec![
            V24PostingPage {
                page_ordinal: 7,
                primary_rows: vec![row(200, 1), row(5, 0)],
                replica_rows: vec![],
            },
            V24PostingPage {
                page_ordinal: 3,
                primary_rows: vec![row(81, 2), row(11, 3)],
                replica_rows: vec![row(5, 0)],
            },
        ];
        let scratch = scratch("identity");
        let plane =
            build_v24_witness_postings(&graph, 201, pages.into_iter().map(Ok), &scratch).unwrap();
        assert_eq!(plane.unique_source_rows(), 4);
        assert_eq!(plane.physical_source_rows(), 5);
        assert_eq!(plane.records_for(0, 64), &[(3, 3), (7, 2)]);

        let bytes = write_v24_witness_postings(&plane).unwrap();
        let registered = identity(&bytes);
        assert_eq!(
            read_v24_witness_postings(&bytes, &registered, 4).unwrap(),
            plane
        );
        let mut changed = registered;
        changed.digest = "00".repeat(32);
        assert!(read_v24_witness_postings(&bytes, &changed, 4).is_err());

        let malformed = vec![V24PostingPage {
            page_ordinal: 0,
            primary_rows: vec![V24PostingPageRow {
                record_id: b"0005".to_vec(),
                vector: unit_vector(0),
            }],
            replica_rows: vec![],
        }];
        assert!(
            build_v24_witness_postings(&graph, 201, malformed.into_iter().map(Ok), &scratch)
                .is_err()
        );
        let out_of_range = vec![V24PostingPage {
            page_ordinal: 0,
            primary_rows: vec![row(201, 0)],
            replica_rows: vec![],
        }];
        assert!(
            build_v24_witness_postings(&graph, 201, out_of_range.into_iter().map(Ok), &scratch)
                .is_err()
        );
        let matching_witness = vec![V24PostingPage {
            page_ordinal: 0,
            primary_rows: vec![row(10_000, 0)],
            replica_rows: vec![],
        }];
        assert!(
            build_v24_witness_postings(
                &graph,
                10_001,
                matching_witness.into_iter().map(Ok),
                &scratch,
            )
            .is_ok()
        );
        let mismatched_witness = vec![V24PostingPage {
            page_ordinal: 0,
            primary_rows: vec![row(10_000, 1)],
            replica_rows: vec![],
        }];
        assert!(
            build_v24_witness_postings(
                &graph,
                10_001,
                mismatched_witness.into_iter().map(Ok),
                &scratch,
            )
            .is_err()
        );
        fs::remove_dir(&scratch).unwrap();
    }

    #[test]
    fn v24_witness_postings_stream_pages_once_and_keep_exact_top64() {
        let graph = build_v24_witness_graph(&witnesses(), SEED).unwrap();
        let decoded = Arc::new(AtomicUsize::new(0));
        let mut source_ordinal = 1_000_u64;
        let pages = (0_u32..66)
            .map(|page_ordinal| {
                let count = usize::try_from(66 - page_ordinal).unwrap();
                let mut primary_rows = (0..count)
                    .map(|_| {
                        let value = row(source_ordinal, 0);
                        source_ordinal += 1;
                        value
                    })
                    .collect::<Vec<_>>();
                primary_rows.reverse();
                V24PostingPage {
                    page_ordinal,
                    primary_rows,
                    replica_rows: vec![],
                }
            })
            .collect::<Vec<_>>();
        let observed = Arc::clone(&decoded);
        let stream = pages.into_iter().map(move |page| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(page)
        });
        let scratch = scratch("bounds");
        let plane = build_v24_witness_postings(&graph, 10_000, stream, &scratch).unwrap();
        assert_eq!(decoded.load(Ordering::SeqCst), 66);
        let top64 = plane.records_for(0, 64);
        assert_eq!(top64.len(), 64);
        assert_eq!(top64[0], (0, 66));
        assert_eq!(top64[63], (63, 3));
        assert_eq!(plane.records_for(0, 16), &top64[..16]);
        assert_eq!(plane.records_for(0, 32), &top64[..32]);

        let duplicate = vec![
            V24PostingPage {
                page_ordinal: 0,
                primary_rows: vec![row(42, 0)],
                replica_rows: vec![],
            },
            V24PostingPage {
                page_ordinal: 1,
                primary_rows: vec![row(42, 0)],
                replica_rows: vec![row(42, 0)],
            },
        ];
        assert!(
            build_v24_witness_postings(&graph, 10_000, duplicate.into_iter().map(Ok), &scratch,)
                .is_err()
        );
        let duplicate_replica = vec![
            V24PostingPage {
                page_ordinal: 0,
                primary_rows: vec![row(42, 0)],
                replica_rows: vec![],
            },
            V24PostingPage {
                page_ordinal: 1,
                primary_rows: vec![],
                replica_rows: vec![row(42, 0)],
            },
            V24PostingPage {
                page_ordinal: 2,
                primary_rows: vec![],
                replica_rows: vec![row(42, 0)],
            },
        ];
        assert!(
            build_v24_witness_postings(
                &graph,
                10_000,
                duplicate_replica.into_iter().map(Ok),
                &scratch,
            )
            .is_err()
        );
        assert!(fs::read_dir(&scratch).unwrap().next().is_none());
        fs::remove_dir(&scratch).unwrap();
    }

    #[test]
    fn v24_witness_postings_reject_occupied_scratch_without_deleting_its_owner_files() {
        let graph = build_v24_witness_graph(&witnesses(), SEED).unwrap();
        let scratch = scratch("occupied");
        let owner_file = scratch.join("source-000.run");
        fs::write(&owner_file, b"other-process-owned").unwrap();
        assert!(
            build_v24_witness_postings(
                &graph,
                10_000,
                Vec::<crate::Result<V24PostingPage>>::new(),
                &scratch,
            )
            .is_err()
        );
        assert_eq!(fs::read(&owner_file).unwrap(), b"other-process-owned");
        fs::remove_file(owner_file).unwrap();
        fs::remove_dir(scratch).unwrap();
    }
}
