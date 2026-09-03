use std::{fs, io::Read, os::unix::fs::FileExt, path::Path, sync::Arc};

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, RecordBatch,
    UInt64Array,
};
use arrow_ipc::{
    convert::fb_to_schema,
    reader::{FileReader, read_footer_length},
    root_as_footer, root_as_message,
    writer::FileWriter,
};
use arrow_schema::{DataType, Field, Schema};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    core::Pq4Codebook,
    format::{Pq4ArtifactIdentity, Pq4Manifest, canonical_manifest_bytes},
};

#[cfg(test)]
const BATCH_ROWS: usize = 65_536;

#[cfg(test)]
pub(crate) struct Pq4SnapshotWriteRequest<'a> {
    pub(crate) directory: &'a Path,
    pub(crate) generation: &'a str,
    pub(crate) source_uri: &'a str,
    pub(crate) source_sha256: &'a str,
    pub(crate) source_encoded_bytes: u64,
    pub(crate) codebook: &'a Pq4Codebook,
    pub(crate) blocks: &'a [[u8; 512]],
    pub(crate) vectors: &'a [[f32; 96]],
    pub(crate) ids: &'a [Vec<u8>],
}

#[derive(Debug, Clone, Copy)]
struct VectorBatch {
    row_start: u64,
    row_count: u64,
    values_offset: u64,
}

#[derive(Debug, Clone, Copy)]
struct IdBatch {
    row_start: u64,
    row_count: u64,
    offsets_offset: u64,
    values_offset: u64,
}

pub(crate) struct Pq4Snapshot {
    codebook: Pq4Codebook,
    blocks: Vec<[u8; 512]>,
    row_count: u64,
    vectors: fs::File,
    vector_batches: Vec<VectorBatch>,
    ids: fs::File,
    id_batches: Vec<IdBatch>,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

pub(super) fn codebook_schema() -> Schema {
    Schema::new(vec![Field::new(
        "centroids",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            1_536,
        ),
        false,
    )])
}

pub(super) fn codes_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_ordinal", DataType::UInt64, false),
        Field::new("packed_codes", DataType::FixedSizeBinary(512), false),
    ])
}

pub(super) fn vectors_schema() -> Schema {
    Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        ),
        false,
    )])
}

pub(super) fn ids_schema() -> Schema {
    Schema::new(vec![Field::new("id", DataType::Binary, false)])
}

pub(super) fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut file = fs::File::open(path)
        .map_err(|error| invalid(&format!("PQ4 snapshot identity open failed: {error}")))?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| invalid(&format!("PQ4 snapshot identity read failed: {error}")))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        bytes = bytes
            .checked_add(u64::try_from(count).unwrap())
            .ok_or_else(|| invalid("PQ4 snapshot identity length overflows"))?;
    }
    Ok((bytes, format!("{:x}", digest.finalize())))
}

fn write_batch_file(path: &Path, schema: &Schema, batches: Vec<RecordBatch>) -> Result<()> {
    let file = fs::File::create(path)
        .map_err(|error| invalid(&format!("PQ4 snapshot Arrow create failed: {error}")))?;
    let mut writer = FileWriter::try_new(file, schema)
        .map_err(|error| invalid(&format!("PQ4 snapshot Arrow writer failed: {error}")))?;
    for batch in batches {
        writer
            .write(&batch)
            .map_err(|error| invalid(&format!("PQ4 snapshot Arrow write failed: {error}")))?;
    }
    writer
        .finish()
        .map_err(|error| invalid(&format!("PQ4 snapshot Arrow finish failed: {error}")))?;
    drop(writer);
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| invalid(&format!("PQ4 snapshot Arrow fsync failed: {error}")))
}

fn identity(
    directory: &Path,
    role: &str,
    file_name: &str,
    row_count: u64,
    schema: &str,
) -> Result<Pq4ArtifactIdentity> {
    let (encoded_bytes, sha256) = sha256_file(&directory.join(file_name))?;
    Ok(Pq4ArtifactIdentity {
        role: role.to_owned(),
        file_name: file_name.to_owned(),
        sha256,
        encoded_bytes,
        row_count,
        schema: schema.to_owned(),
    })
}

pub(super) struct StreamedSnapshotAuthority<'a> {
    pub(super) directory: &'a Path,
    pub(super) temporary: &'a Path,
    pub(super) generation: &'a str,
    pub(super) source_uri: &'a str,
    pub(super) source_sha256: &'a str,
    pub(super) source_encoded_bytes: u64,
    pub(super) codebook: &'a Pq4Codebook,
    pub(super) blocks: &'a [[u8; 512]],
    pub(super) row_count: u64,
}

pub(super) fn finish_streamed_snapshot(
    authority: &StreamedSnapshotAuthority<'_>,
) -> Result<Pq4Manifest> {
    let centroid_values = authority
        .codebook
        .centroids
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    let centroids = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        1_536,
        Arc::new(Float32Array::from(centroid_values)),
        None,
    )
    .map_err(|error| invalid(&format!("PQ4 snapshot codebook array failed: {error}")))?;
    write_batch_file(
        &authority.temporary.join("codebook.arrow"),
        &codebook_schema(),
        vec![
            RecordBatch::try_new(Arc::new(codebook_schema()), vec![Arc::new(centroids)]).map_err(
                |error| invalid(&format!("PQ4 snapshot codebook batch failed: {error}")),
            )?,
        ],
    )?;
    let packed = FixedSizeBinaryArray::try_from_iter(authority.blocks.iter())
        .map_err(|error| invalid(&format!("PQ4 snapshot codes array failed: {error}")))?;
    write_batch_file(
        &authority.temporary.join("codes.arrow"),
        &codes_schema(),
        vec![
            RecordBatch::try_new(
                Arc::new(codes_schema()),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        0..u64::try_from(authority.blocks.len()).unwrap(),
                    )),
                    Arc::new(packed),
                ],
            )
            .map_err(|error| invalid(&format!("PQ4 snapshot codes batch failed: {error}")))?,
        ],
    )?;

    let block_count = u64::try_from(authority.blocks.len()).unwrap();
    let manifest = Pq4Manifest {
        schema: "borsuk-pq4-snapshot-v1".to_owned(),
        generation: authority.generation.to_owned(),
        source_uri: authority.source_uri.to_owned(),
        source_sha256: authority.source_sha256.to_owned(),
        source_encoded_bytes: authority.source_encoded_bytes,
        row_count: authority.row_count,
        dimension: 96,
        subquantizer_count: 32,
        subspace_dimensions: 3,
        centroid_count: 16,
        lloyd_iterations: 4,
        block_rows: 32,
        block_count,
        padding_rows: u32::try_from(block_count * 32 - authority.row_count).unwrap(),
        code_bytes_per_row: 16,
        byte_order: "subquantizer-major".to_owned(),
        nibble_order: "even-low-odd-high".to_owned(),
        source_order: "ascending-source-ordinal".to_owned(),
        candidate_depth: 3_072,
        codebook: identity(
            authority.temporary,
            "codebook-arrow",
            "codebook.arrow",
            1,
            "centroids:non-nullable-fixed-list-f32[1536]",
        )?,
        codes: identity(
            authority.temporary,
            "codes-arrow",
            "codes.arrow",
            block_count,
            "block_ordinal:u64,packed_codes:non-nullable-fixed-binary[512]",
        )?,
        vectors: identity(
            authority.temporary,
            "vectors-arrow",
            "vectors.arrow",
            authority.row_count,
            "vector:non-nullable-fixed-list-f32[96]",
        )?,
        ids: identity(
            authority.temporary,
            "ids-arrow",
            "ids.arrow",
            authority.row_count,
            "id:non-nullable-binary",
        )?,
    };
    fs::write(
        authority.temporary.join("manifest.json"),
        canonical_manifest_bytes(&manifest)?,
    )
    .map_err(|error| invalid(&format!("PQ4 snapshot manifest write failed: {error}")))?;
    for path in [
        authority.temporary.join("manifest.json"),
        authority.temporary.join("vectors.arrow"),
        authority.temporary.join("ids.arrow"),
    ] {
        fs::File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|error| invalid(&format!("PQ4 snapshot streamed fsync failed: {error}")))?;
    }
    fs::File::open(authority.temporary)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| invalid(&format!("PQ4 snapshot directory fsync failed: {error}")))?;
    fs::rename(authority.temporary, authority.directory)
        .map_err(|error| invalid(&format!("PQ4 snapshot rename failed: {error}")))?;
    let parent = authority
        .directory
        .parent()
        .ok_or_else(|| invalid("PQ4 snapshot parent is absent"))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| invalid(&format!("PQ4 snapshot parent fsync failed: {error}")))?;
    Ok(manifest)
}

#[cfg(test)]
pub(crate) fn write_snapshot(request: &Pq4SnapshotWriteRequest<'_>) -> Result<Pq4Manifest> {
    let row_count = request.vectors.len();
    if request.directory.exists()
        || row_count < 3_072
        || request.ids.len() != row_count
        || request.blocks.len() != row_count.div_ceil(32)
        || request.codebook.centroids.len() != 32
        || request
            .codebook
            .centroids
            .iter()
            .flatten()
            .any(|v| !v.is_finite())
        || request.vectors.iter().any(|row| {
            row.iter().any(|value| !value.is_finite())
                || row.iter().map(|value| value * value).sum::<f32>() <= 0.0
        })
    {
        return Err(invalid("PQ4 snapshot write request differs"));
    }
    let parent = request
        .directory
        .parent()
        .ok_or_else(|| invalid("PQ4 snapshot parent is absent"))?;
    let name = request
        .directory
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| invalid("PQ4 snapshot directory name differs"))?;
    let temporary = parent.join(format!(".{name}.tmp-{}", std::process::id()));
    if temporary.exists() {
        return Err(invalid("PQ4 snapshot temporary directory exists"));
    }

    let result = (|| {
        fs::create_dir(&temporary)
            .map_err(|error| invalid(&format!("PQ4 snapshot directory create failed: {error}")))?;
        let centroid_values = request
            .codebook
            .centroids
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let centroids = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            1_536,
            Arc::new(Float32Array::from(centroid_values)),
            None,
        )
        .map_err(|error| invalid(&format!("PQ4 snapshot codebook array failed: {error}")))?;
        write_batch_file(
            &temporary.join("codebook.arrow"),
            &codebook_schema(),
            vec![
                RecordBatch::try_new(Arc::new(codebook_schema()), vec![Arc::new(centroids)])
                    .map_err(|error| {
                        invalid(&format!("PQ4 snapshot codebook batch failed: {error}"))
                    })?,
            ],
        )?;

        let packed = FixedSizeBinaryArray::try_from_iter(request.blocks.iter())
            .map_err(|error| invalid(&format!("PQ4 snapshot codes array failed: {error}")))?;
        write_batch_file(
            &temporary.join("codes.arrow"),
            &codes_schema(),
            vec![
                RecordBatch::try_new(
                    Arc::new(codes_schema()),
                    vec![
                        Arc::new(UInt64Array::from_iter_values(
                            0..u64::try_from(request.blocks.len()).unwrap(),
                        )),
                        Arc::new(packed),
                    ],
                )
                .map_err(|error| invalid(&format!("PQ4 snapshot codes batch failed: {error}")))?,
            ],
        )?;

        let mut vector_batches = Vec::new();
        for rows in request.vectors.chunks(BATCH_ROWS) {
            let values = rows.iter().flatten().copied().collect::<Vec<_>>();
            let vectors = FixedSizeListArray::try_new(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
                Arc::new(Float32Array::from(values)),
                None,
            )
            .map_err(|error| invalid(&format!("PQ4 snapshot vector array failed: {error}")))?;
            vector_batches.push(
                RecordBatch::try_new(Arc::new(vectors_schema()), vec![Arc::new(vectors)]).map_err(
                    |error| invalid(&format!("PQ4 snapshot vector batch failed: {error}")),
                )?,
            );
        }
        write_batch_file(
            &temporary.join("vectors.arrow"),
            &vectors_schema(),
            vector_batches,
        )?;

        let mut id_batches = Vec::new();
        for rows in request.ids.chunks(BATCH_ROWS) {
            id_batches.push(
                RecordBatch::try_new(
                    Arc::new(ids_schema()),
                    vec![Arc::new(BinaryArray::from_iter_values(
                        rows.iter().map(Vec::as_slice),
                    ))],
                )
                .map_err(|error| invalid(&format!("PQ4 snapshot ID batch failed: {error}")))?,
            );
        }
        write_batch_file(&temporary.join("ids.arrow"), &ids_schema(), id_batches)?;

        let row_count = u64::try_from(row_count).unwrap();
        let block_count = u64::try_from(request.blocks.len()).unwrap();
        let manifest = Pq4Manifest {
            schema: "borsuk-pq4-snapshot-v1".to_owned(),
            generation: request.generation.to_owned(),
            source_uri: request.source_uri.to_owned(),
            source_sha256: request.source_sha256.to_owned(),
            source_encoded_bytes: request.source_encoded_bytes,
            row_count,
            dimension: 96,
            subquantizer_count: 32,
            subspace_dimensions: 3,
            centroid_count: 16,
            lloyd_iterations: 4,
            block_rows: 32,
            block_count,
            padding_rows: u32::try_from(block_count * 32 - row_count).unwrap(),
            code_bytes_per_row: 16,
            byte_order: "subquantizer-major".to_owned(),
            nibble_order: "even-low-odd-high".to_owned(),
            source_order: "ascending-source-ordinal".to_owned(),
            candidate_depth: 3_072,
            codebook: identity(
                &temporary,
                "codebook-arrow",
                "codebook.arrow",
                1,
                "centroids:non-nullable-fixed-list-f32[1536]",
            )?,
            codes: identity(
                &temporary,
                "codes-arrow",
                "codes.arrow",
                block_count,
                "block_ordinal:u64,packed_codes:non-nullable-fixed-binary[512]",
            )?,
            vectors: identity(
                &temporary,
                "vectors-arrow",
                "vectors.arrow",
                row_count,
                "vector:non-nullable-fixed-list-f32[96]",
            )?,
            ids: identity(
                &temporary,
                "ids-arrow",
                "ids.arrow",
                row_count,
                "id:non-nullable-binary",
            )?,
        };
        fs::write(
            temporary.join("manifest.json"),
            canonical_manifest_bytes(&manifest)?,
        )
        .map_err(|error| invalid(&format!("PQ4 snapshot manifest write failed: {error}")))?;
        fs::File::open(temporary.join("manifest.json"))
            .and_then(|file| file.sync_all())
            .map_err(|error| invalid(&format!("PQ4 snapshot manifest fsync failed: {error}")))?;
        fs::File::open(&temporary)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| invalid(&format!("PQ4 snapshot directory fsync failed: {error}")))?;
        fs::rename(&temporary, request.directory)
            .map_err(|error| invalid(&format!("PQ4 snapshot rename failed: {error}")))?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| invalid(&format!("PQ4 snapshot parent fsync failed: {error}")))?;
        Ok(manifest)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
}

fn authenticate(path: &Path, identity: &Pq4ArtifactIdentity) -> Result<FileReader<fs::File>> {
    let (encoded_bytes, sha256) = sha256_file(path)?;
    if encoded_bytes != identity.encoded_bytes || sha256 != identity.sha256 {
        return Err(invalid("PQ4 snapshot artifact bytes differ"));
    }
    FileReader::try_new(
        fs::File::open(path)
            .map_err(|error| invalid(&format!("PQ4 snapshot Arrow open failed: {error}")))?,
        None,
    )
    .map_err(|error| invalid(&format!("PQ4 snapshot Arrow metadata failed: {error}")))
}

type RawBatch = (u64, u64, Vec<(u64, u64)>);

fn raw_batches(path: &Path, schema: &Schema) -> Result<(fs::File, Vec<RawBatch>)> {
    let file = fs::File::open(path)
        .map_err(|error| invalid(&format!("PQ4 snapshot positional open failed: {error}")))?;
    let file_len = file
        .metadata()
        .map_err(|error| invalid(&format!("PQ4 snapshot positional metadata failed: {error}")))?
        .len();
    let mut trailer = [0_u8; 10];
    file.read_exact_at(
        &mut trailer,
        file_len
            .checked_sub(10)
            .ok_or_else(|| invalid("PQ4 snapshot footer length differs"))?,
    )
    .map_err(|error| invalid(&format!("PQ4 snapshot footer read failed: {error}")))?;
    let footer_len = read_footer_length(trailer)
        .map_err(|error| invalid(&format!("PQ4 snapshot footer failed: {error}")))?;
    let mut footer_bytes = vec![0_u8; footer_len];
    file.read_exact_at(
        &mut footer_bytes,
        file_len
            .checked_sub(10 + u64::try_from(footer_len).unwrap())
            .ok_or_else(|| invalid("PQ4 snapshot footer range differs"))?,
    )
    .map_err(|error| invalid(&format!("PQ4 snapshot footer read failed: {error}")))?;
    let footer = root_as_footer(&footer_bytes)
        .map_err(|error| invalid(&format!("PQ4 snapshot footer parse failed: {error}")))?;
    if footer
        .dictionaries()
        .is_some_and(|values| !values.is_empty())
        || footer.schema().map(|value| fb_to_schema(value)).as_ref() != Some(schema)
    {
        return Err(invalid("PQ4 snapshot positional schema differs"));
    }
    let blocks = footer
        .recordBatches()
        .ok_or_else(|| invalid("PQ4 snapshot record batches are absent"))?;
    let mut result = Vec::with_capacity(blocks.len());
    for block in blocks {
        let metadata_len = usize::try_from(block.metaDataLength())
            .map_err(|_| invalid("PQ4 snapshot metadata length differs"))?;
        let block_offset = u64::try_from(block.offset())
            .map_err(|_| invalid("PQ4 snapshot block offset differs"))?;
        let mut metadata = vec![0_u8; metadata_len];
        file.read_exact_at(&mut metadata, block_offset)
            .map_err(|error| invalid(&format!("PQ4 snapshot metadata read failed: {error}")))?;
        let message_start = if metadata.starts_with(&[0xff; 4]) {
            8
        } else {
            4
        };
        let message = root_as_message(&metadata[message_start..])
            .map_err(|error| invalid(&format!("PQ4 snapshot metadata parse failed: {error}")))?;
        let record = message
            .header_as_record_batch()
            .ok_or_else(|| invalid("PQ4 snapshot record batch metadata is absent"))?;
        if record.length() <= 0 || record.compression().is_some() {
            return Err(invalid("PQ4 snapshot record batch differs"));
        }
        let body_start = block_offset
            .checked_add(u64::try_from(metadata_len).unwrap())
            .ok_or_else(|| invalid("PQ4 snapshot body offset overflows"))?;
        let buffers = record
            .buffers()
            .ok_or_else(|| invalid("PQ4 snapshot buffers are absent"))?
            .iter()
            .map(|buffer| {
                let offset = u64::try_from(buffer.offset())
                    .map_err(|_| invalid("PQ4 snapshot buffer offset differs"))?;
                let length = u64::try_from(buffer.length())
                    .map_err(|_| invalid("PQ4 snapshot buffer length differs"))?;
                Ok((body_start + offset, length))
            })
            .collect::<Result<Vec<_>>>()?;
        result.push((u64::try_from(record.length()).unwrap(), body_start, buffers));
    }
    Ok((file, result))
}

impl Pq4Snapshot {
    pub(crate) fn open(directory: &Path) -> Result<Self> {
        let expected = [
            "codebook.arrow",
            "codes.arrow",
            "ids.arrow",
            "manifest.json",
            "vectors.arrow",
        ];
        let mut observed = fs::read_dir(directory)
            .map_err(|error| invalid(&format!("PQ4 snapshot directory read failed: {error}")))?
            .map(|entry| {
                entry
                    .map_err(|error| invalid(&format!("PQ4 snapshot entry failed: {error}")))?
                    .file_name()
                    .into_string()
                    .map_err(|_| invalid("PQ4 snapshot entry name differs"))
            })
            .collect::<Result<Vec<_>>>()?;
        observed.sort();
        if observed != expected {
            return Err(invalid("PQ4 snapshot file inventory differs"));
        }
        let manifest_bytes = fs::read(directory.join("manifest.json"))
            .map_err(|error| invalid(&format!("PQ4 snapshot manifest read failed: {error}")))?;
        let manifest: Pq4Manifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| invalid(&format!("PQ4 snapshot manifest parse failed: {error}")))?;
        if canonical_manifest_bytes(&manifest)? != manifest_bytes {
            return Err(invalid("PQ4 snapshot manifest bytes differ"));
        }

        let mut codebook_reader =
            authenticate(&directory.join("codebook.arrow"), &manifest.codebook)?;
        if codebook_reader.schema().as_ref() != &codebook_schema()
            || codebook_reader.num_batches() != 1
        {
            return Err(invalid("PQ4 snapshot codebook schema differs"));
        }
        let codebook_batch = codebook_reader
            .next()
            .transpose()
            .map_err(|error| invalid(&format!("PQ4 snapshot codebook read failed: {error}")))?
            .ok_or_else(|| invalid("PQ4 snapshot codebook is absent"))?;
        let centroids = codebook_batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("PQ4 snapshot codebook array differs"))?
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("PQ4 snapshot codebook values differ"))?;
        if codebook_batch.num_rows() != 1 || centroids.null_count() != 0 || centroids.len() != 1_536
        {
            return Err(invalid("PQ4 snapshot codebook dimensions differ"));
        }
        let codebook = Pq4Codebook {
            centroids: centroids.values().as_chunks::<48>().0.to_vec(),
        };
        if codebook.centroids.len() != 32
            || codebook
                .centroids
                .iter()
                .flatten()
                .any(|value| !value.is_finite())
        {
            return Err(invalid("PQ4 snapshot codebook values differ"));
        }

        let mut codes_reader = authenticate(&directory.join("codes.arrow"), &manifest.codes)?;
        if codes_reader.schema().as_ref() != &codes_schema() || codes_reader.num_batches() != 1 {
            return Err(invalid("PQ4 snapshot codes schema differs"));
        }
        let codes_batch = codes_reader
            .next()
            .transpose()
            .map_err(|error| invalid(&format!("PQ4 snapshot codes read failed: {error}")))?
            .ok_or_else(|| invalid("PQ4 snapshot codes are absent"))?;
        let ordinals = codes_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("PQ4 snapshot block ordinals differ"))?;
        let packed = codes_batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid("PQ4 snapshot packed codes differ"))?;
        if codes_batch.num_rows() != usize::try_from(manifest.block_count).unwrap()
            || ordinals.null_count() != 0
            || packed.null_count() != 0
        {
            return Err(invalid("PQ4 snapshot code block count differs"));
        }
        let mut blocks = Vec::with_capacity(codes_batch.num_rows());
        for index in 0..codes_batch.num_rows() {
            if ordinals.value(index) != u64::try_from(index).unwrap() {
                return Err(invalid("PQ4 snapshot block order differs"));
            }
            blocks.push(
                packed
                    .value(index)
                    .try_into()
                    .map_err(|_| invalid("PQ4 snapshot packed code width differs"))?,
            );
        }

        let mut vector_reader = authenticate(&directory.join("vectors.arrow"), &manifest.vectors)?;
        if vector_reader.schema().as_ref() != &vectors_schema() {
            return Err(invalid("PQ4 snapshot vector schema differs"));
        }
        let mut validated_rows = 0_u64;
        for batch in &mut vector_reader {
            let batch = batch
                .map_err(|error| invalid(&format!("PQ4 snapshot vector read failed: {error}")))?;
            let vectors = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| invalid("PQ4 snapshot vector array differs"))?;
            let values = vectors
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("PQ4 snapshot vector values differ"))?;
            if vectors.null_count() != 0
                || values.null_count() != 0
                || !values.values().as_chunks::<96>().1.is_empty()
                || values.values().as_chunks::<96>().0.iter().any(|row| {
                    row.iter().any(|value| !value.is_finite())
                        || row.iter().map(|value| value * value).sum::<f32>() <= 0.0
                })
            {
                return Err(invalid("PQ4 snapshot vector values differ"));
            }
            validated_rows += u64::try_from(batch.num_rows()).unwrap();
        }
        if validated_rows != manifest.row_count {
            return Err(invalid("PQ4 snapshot vector row count differs"));
        }

        let mut id_reader = authenticate(&directory.join("ids.arrow"), &manifest.ids)?;
        if id_reader.schema().as_ref() != &ids_schema() {
            return Err(invalid("PQ4 snapshot ID schema differs"));
        }
        let mut id_rows = 0_u64;
        for batch in &mut id_reader {
            let batch =
                batch.map_err(|error| invalid(&format!("PQ4 snapshot ID read failed: {error}")))?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("PQ4 snapshot ID array differs"))?;
            if ids.null_count() != 0 {
                return Err(invalid("PQ4 snapshot ID values differ"));
            }
            id_rows += u64::try_from(batch.num_rows()).unwrap();
        }
        if id_rows != manifest.row_count {
            return Err(invalid("PQ4 snapshot ID row count differs"));
        }

        let (vectors, vector_raw) =
            raw_batches(&directory.join("vectors.arrow"), &vectors_schema())?;
        let mut row_start = 0_u64;
        let mut vector_batches = Vec::with_capacity(vector_raw.len());
        for (row_count, _, buffers) in vector_raw {
            let (values_offset, values_len) = *buffers
                .get(2)
                .ok_or_else(|| invalid("PQ4 snapshot vector buffers differ"))?;
            if values_len != row_count * 96 * 4 {
                return Err(invalid("PQ4 snapshot vector buffer length differs"));
            }
            vector_batches.push(VectorBatch {
                row_start,
                row_count,
                values_offset,
            });
            row_start += row_count;
        }
        if row_start != manifest.row_count {
            return Err(invalid("PQ4 snapshot positional vector rows differ"));
        }

        let (ids, id_raw) = raw_batches(&directory.join("ids.arrow"), &ids_schema())?;
        let mut row_start = 0_u64;
        let mut id_batches = Vec::with_capacity(id_raw.len());
        for (row_count, _, buffers) in id_raw {
            let (offsets_offset, offsets_len) = *buffers
                .get(1)
                .ok_or_else(|| invalid("PQ4 snapshot ID offset buffer differs"))?;
            let (values_offset, _) = *buffers
                .get(2)
                .ok_or_else(|| invalid("PQ4 snapshot ID value buffer differs"))?;
            if offsets_len != (row_count + 1) * 4 {
                return Err(invalid("PQ4 snapshot ID offset length differs"));
            }
            id_batches.push(IdBatch {
                row_start,
                row_count,
                offsets_offset,
                values_offset,
            });
            row_start += row_count;
        }
        if row_start != manifest.row_count {
            return Err(invalid("PQ4 snapshot positional ID rows differ"));
        }

        Ok(Self {
            codebook,
            blocks,
            row_count: manifest.row_count,
            vectors,
            vector_batches,
            ids,
            id_batches,
        })
    }

    pub(crate) fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) fn codebook(&self) -> &Pq4Codebook {
        &self.codebook
    }

    pub(crate) fn blocks(&self) -> &[[u8; 512]] {
        &self.blocks
    }

    pub(crate) fn read_vector(&self, source_ordinal: u64) -> Result<[f32; 96]> {
        let batch = self
            .vector_batches
            .iter()
            .find(|batch| {
                source_ordinal >= batch.row_start
                    && source_ordinal < batch.row_start + batch.row_count
            })
            .ok_or_else(|| invalid("PQ4 snapshot vector ordinal is absent"))?;
        let local = source_ordinal - batch.row_start;
        let mut bytes = [0_u8; 96 * 4];
        self.vectors
            .read_exact_at(&mut bytes, batch.values_offset + local * 96 * 4)
            .map_err(|error| {
                invalid(&format!(
                    "PQ4 snapshot vector positioned read failed: {error}"
                ))
            })?;
        let mut vector = [0_f32; 96];
        for (value, encoded) in vector.iter_mut().zip(bytes.as_chunks::<4>().0) {
            *value = f32::from_le_bytes(*encoded);
        }
        if vector.iter().any(|value| !value.is_finite())
            || vector.iter().map(|value| value * value).sum::<f32>() <= 0.0
        {
            return Err(invalid("PQ4 snapshot vector differs"));
        }
        Ok(vector)
    }

    pub(crate) fn read_id(&self, source_ordinal: u64) -> Result<Vec<u8>> {
        let batch = self
            .id_batches
            .iter()
            .find(|batch| {
                source_ordinal >= batch.row_start
                    && source_ordinal < batch.row_start + batch.row_count
            })
            .ok_or_else(|| invalid("PQ4 snapshot ID ordinal is absent"))?;
        let local = source_ordinal - batch.row_start;
        let mut offsets = [0_u8; 8];
        self.ids
            .read_exact_at(&mut offsets, batch.offsets_offset + local * 4)
            .map_err(|error| invalid(&format!("PQ4 snapshot ID offset read failed: {error}")))?;
        let start = u32::from_le_bytes(offsets[..4].try_into().unwrap());
        let end = u32::from_le_bytes(offsets[4..].try_into().unwrap());
        if end < start {
            return Err(invalid("PQ4 snapshot ID offsets differ"));
        }
        let mut value = vec![0_u8; usize::try_from(end - start).unwrap()];
        self.ids
            .read_exact_at(&mut value, batch.values_offset + u64::from(start))
            .map_err(|error| {
                invalid(&format!("PQ4 snapshot ID positioned read failed: {error}"))
            })?;
        Ok(value)
    }
}
