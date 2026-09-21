use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use sha2::{Digest, Sha256};

use crate::{
    error::{BorsukError, Result},
    metric::VectorMetric,
    native_ann::{
        NativeAnnRef, NativeArtifactRef, NativeRouterRef, NativeRunRef, native_ann_root_bytes,
    },
    native_ann_read::{NativePageRef, NativeRowState},
    rotated_product_quantizer::{ProductQuantizerConfig, ProductRotation, RotatedProductQuantizer},
    storage::Storage,
};

const PAGE_ROWS: usize = 256;
const PQ_WIDTH: usize = 16;
const CODEWORDS: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeBuildRow {
    pub(crate) id: Vec<u8>,
    pub(crate) sequence: u64,
    pub(crate) version: [u8; 24],
    pub(crate) state: NativeRowState,
    pub(crate) vector: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeBuildConfig {
    pub(crate) generation: u64,
    pub(crate) previous_generation_sha256: Option<String>,
    pub(crate) source_identity: String,
    pub(crate) metric: VectorMetric,
    pub(crate) dimensions: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeBuildObject {
    pub(crate) role: String,
    pub(crate) path: String,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NativeBuildOutput {
    pub(crate) reference: NativeAnnRef,
    pub(crate) root_bytes: Vec<u8>,
    pub(crate) root_sha256: String,
    pub(crate) root_path: String,
    pub(crate) objects: Vec<NativeBuildObject>,
    pub(crate) page_directory: Vec<NativePageRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeMutationBuildEntry {
    id: Vec<u8>,
    sequence: u64,
    version: [u8; 24],
    state: NativeRowState,
}

fn invalid(message: impl Into<String>) -> BorsukError {
    BorsukError::InvalidStorage(message.into())
}

fn version_hex(version: &[u8; 24]) -> String {
    version.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn artifact(role: &str, extension: &str, bytes: Vec<u8>) -> (NativeArtifactRef, NativeBuildObject) {
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let path = format!("native-ann/objects/{sha256}.{extension}");
    (
        NativeArtifactRef {
            role: role.to_owned(),
            uri: format!("borsuk://native-ann/{path}"),
            sha256,
            encoded_bytes: bytes.len() as u64,
        },
        NativeBuildObject {
            role: role.to_owned(),
            path,
            bytes,
        },
    )
}

fn parquet_bytes(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Result<Vec<u8>> {
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let mut bytes = Vec::new();
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn compact_native_rows<'a>(
    runs: impl IntoIterator<Item = &'a [NativeBuildRow]>,
) -> Result<Vec<NativeBuildRow>> {
    let mut latest = BTreeMap::<Vec<u8>, NativeBuildRow>::new();
    for row in runs.into_iter().flatten() {
        if row.sequence == 0 || row.vector.is_empty() || row.vector.iter().any(|v| !v.is_finite()) {
            return Err(invalid("native ANN build row differs"));
        }
        match latest.get(&row.id) {
            Some(current) if current.sequence > row.sequence => {}
            Some(current) if current.sequence == row.sequence => {
                if current != row {
                    return Err(invalid("native ANN build contains a sequence tie"));
                }
            }
            _ => {
                latest.insert(row.id.clone(), row.clone());
            }
        }
    }
    Ok(latest
        .into_values()
        .filter(|row| row.state == NativeRowState::Live)
        .collect())
}

fn fit_quantizer(
    vectors: &[Vec<f32>],
    dimensions: usize,
    seed: u64,
) -> Result<RotatedProductQuantizer> {
    if vectors.is_empty() {
        return Err(invalid("native ANN quantizer training rows are absent"));
    }
    let padded;
    let fit_vectors = if vectors.len() < CODEWORDS {
        padded = vectors
            .iter()
            .cycle()
            .take(CODEWORDS)
            .cloned()
            .collect::<Vec<_>>();
        padded.as_slice()
    } else {
        vectors
    };
    let iterations = if vectors.len() < 4_096 { 1 } else { 8 };
    RotatedProductQuantizer::fit(
        ProductQuantizerConfig {
            rotation: ProductRotation::Identity,
            seed,
            dimensions,
            subspaces: PQ_WIDTH,
            centroids: CODEWORDS,
            sample_limit: fit_vectors.len().min(65_536),
            iterations,
        },
        fit_vectors,
    )
}

fn padded_codebooks(quantizer: &RotatedProductQuantizer, dimensions: usize) -> Vec<f32> {
    let state = quantizer.state();
    let width = dimensions.div_ceil(PQ_WIDTH);
    let mut padded = vec![0.0_f32; PQ_WIDTH * CODEWORDS * width];
    for subspace in 0..PQ_WIDTH {
        let active = state.subspace_offsets[subspace + 1] - state.subspace_offsets[subspace];
        for codeword in 0..CODEWORDS {
            let source = codeword * active;
            let target = (subspace * CODEWORDS + codeword) * width;
            padded[target..target + active]
                .copy_from_slice(&state.codebooks[subspace][source..source + active]);
        }
    }
    padded
}

fn encode_codebooks(row: &[f32], summary: &[f32], dimensions: usize) -> Result<Vec<u8>> {
    let width = dimensions.div_ceil(PQ_WIDTH);
    let mut kinds = Vec::with_capacity(2 * PQ_WIDTH * CODEWORDS);
    let mut subspaces = Vec::with_capacity(kinds.capacity());
    let mut codewords = Vec::with_capacity(kinds.capacity());
    let mut centroids = Vec::with_capacity(kinds.capacity() * width);
    for (kind, values) in [("row", row), ("summary", summary)] {
        for subspace in 0..PQ_WIDTH {
            for codeword in 0..CODEWORDS {
                kinds.push(kind);
                subspaces.push(subspace as u16);
                codewords.push(codeword as u16);
                let start = (subspace * CODEWORDS + codeword) * width;
                centroids.extend_from_slice(&values[start..start + width]);
            }
        }
    }
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    let schema = Arc::new(Schema::new(vec![
        Field::new("kind", DataType::Utf8, false),
        Field::new("subspace", DataType::UInt16, false),
        Field::new("codeword", DataType::UInt16, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(Arc::clone(&child), width as i32),
            false,
        ),
    ]));
    parquet_bytes(
        schema,
        vec![
            Arc::new(StringArray::from(kinds)),
            Arc::new(UInt16Array::from(subspaces)),
            Arc::new(UInt16Array::from(codewords)),
            Arc::new(FixedSizeListArray::try_new(
                child,
                width as i32,
                Arc::new(Float32Array::from(centroids)),
                None,
            )?),
        ],
    )
}

fn encode_row_codes(codes: Vec<u8>) -> Result<Vec<u8>> {
    let child = Arc::new(Field::new("element", DataType::UInt8, false));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "code",
        DataType::FixedSizeList(Arc::clone(&child), PQ_WIDTH as i32),
        false,
    )]));
    parquet_bytes(
        schema,
        vec![Arc::new(FixedSizeListArray::try_new(
            child,
            PQ_WIDTH as i32,
            Arc::new(UInt8Array::from(codes)),
            None,
        )?)],
    )
}

fn encode_summary_codes(codes: Vec<u8>, pages: usize) -> Result<Vec<u8>> {
    let child = Arc::new(Field::new("element", DataType::UInt8, false));
    let schema = Arc::new(Schema::new(vec![
        Field::new("page", DataType::UInt32, false),
        Field::new("block", DataType::UInt8, false),
        Field::new(
            "code",
            DataType::FixedSizeList(Arc::clone(&child), PQ_WIDTH as i32),
            false,
        ),
    ]));
    parquet_bytes(
        schema,
        vec![
            Arc::new(UInt32Array::from(
                (0..pages)
                    .flat_map(|page| [page as u32; 2])
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                (0..pages).flat_map(|_| [0_u8, 1]).collect::<Vec<_>>(),
            )),
            Arc::new(FixedSizeListArray::try_new(
                child,
                PQ_WIDTH as i32,
                Arc::new(UInt8Array::from(codes)),
                None,
            )?),
        ],
    )
}

fn encode_page(rows: &[NativeBuildRow], vectors: &[f32], dimensions: usize) -> Result<Vec<u8>> {
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Binary, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("state", DataType::UInt8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::clone(&child), dimensions as i32),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.id.as_slice()),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                rows.iter().map(|row| row.state as u8).collect::<Vec<_>>(),
            )),
            Arc::new(FixedSizeListArray::try_new(
                child,
                dimensions as i32,
                Arc::new(Float32Array::from(vectors.to_vec())),
                None,
            )?),
        ],
    )?;
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

fn mutation_directory_schema() -> Schema {
    let child = Arc::new(Field::new("element", DataType::UInt8, false));
    Schema::new(vec![
        Field::new("id", DataType::Binary, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("state", DataType::UInt8, false),
        Field::new("version", DataType::FixedSizeList(child, 24), false),
    ])
}

fn encode_mutation_directory(rows: &[NativeMutationBuildEntry]) -> Result<Vec<u8>> {
    let child = Arc::new(Field::new("element", DataType::UInt8, false));
    let schema = Arc::new(mutation_directory_schema());
    let versions = rows.iter().flat_map(|row| row.version).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.id.as_slice()),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                rows.iter().map(|row| row.state as u8).collect::<Vec<_>>(),
            )),
            Arc::new(FixedSizeListArray::try_new(
                child,
                24,
                Arc::new(UInt8Array::from(versions)),
                None,
            )?),
        ],
    )?;
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

fn decode_mutation_directory(bytes: &[u8]) -> Result<Vec<NativeMutationBuildEntry>> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &mutation_directory_schema() {
        return Err(invalid("native ANN mutation-directory schema differs"));
    }
    let mut rows = Vec::new();
    for batch in &mut reader {
        let batch = batch?;
        if batch.num_columns() != 4 || batch.columns().iter().any(|array| array.null_count() != 0) {
            return Err(invalid("native ANN mutation-directory batch differs"));
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("native ANN mutation-directory id differs"))?;
        let sequences = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("native ANN mutation-directory sequence differs"))?;
        let states = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("native ANN mutation-directory state differs"))?;
        let versions = batch
            .column(3)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("native ANN mutation-directory version differs"))?;
        for row in 0..batch.num_rows() {
            let state = match states.value(row) {
                0 => NativeRowState::Live,
                1 => NativeRowState::Tombstone,
                _ => return Err(invalid("native ANN mutation-directory state differs")),
            };
            let version = versions.value(row);
            let version = version
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| invalid("native ANN mutation-directory version child differs"))?;
            let version: [u8; 24] = version
                .values()
                .as_ref()
                .try_into()
                .map_err(|_| invalid("native ANN mutation-directory version width differs"))?;
            rows.push(NativeMutationBuildEntry {
                id: ids.value(row).to_vec(),
                sequence: sequences.value(row),
                version,
                state,
            });
        }
    }
    Ok(rows)
}

fn encode_page_directory(pages: &[NativePageRef]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("page", DataType::UInt32, false),
        Field::new("object", DataType::Utf8, false),
        Field::new("offset", DataType::UInt64, false),
        Field::new("length", DataType::UInt64, false),
        Field::new("sha256", DataType::Utf8, false),
        Field::new("rows", DataType::UInt32, false),
    ]));
    parquet_bytes(
        schema,
        vec![
            Arc::new(UInt32Array::from(
                pages.iter().map(|page| page.page).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                pages
                    .iter()
                    .map(|page| page.object.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                pages
                    .iter()
                    .map(|page| page.range.start)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                pages
                    .iter()
                    .map(|page| page.range.end - page.range.start)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                pages
                    .iter()
                    .map(|page| page.sha256.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                pages.iter().map(|page| page.rows).collect::<Vec<_>>(),
            )),
        ],
    )
}

pub(crate) fn build_native_generation<'a>(
    config: NativeBuildConfig,
    runs: impl IntoIterator<Item = &'a [NativeBuildRow]>,
) -> Result<NativeBuildOutput> {
    let dimensions = usize::try_from(config.dimensions)
        .map_err(|_| invalid("native ANN build dimensions exceed usize"))?;
    let rows = compact_native_rows(runs)?;
    if config.generation == 0
        || dimensions < PQ_WIDTH
        || config.source_identity.is_empty()
        || rows.is_empty()
        || rows.iter().any(|row| row.vector.len() != dimensions)
        || !matches!(
            config.metric,
            VectorMetric::Euclidean | VectorMetric::SquaredEuclidean
        )
    {
        return Err(invalid("native ANN build configuration differs"));
    }
    let vectors = rows
        .iter()
        .map(|row| row.vector.clone())
        .collect::<Vec<_>>();
    let row_quantizer = fit_quantizer(&vectors, dimensions, 0xB0_25_0001)?;
    let row_codes = vectors
        .iter()
        .map(|vector| row_quantizer.encode(vector))
        .collect::<Result<Vec<_>>>()?;
    let mut block_means = Vec::new();
    for page in vectors.chunks(PAGE_ROWS) {
        for block in 0..2 {
            let start = block * page.len().div_ceil(2);
            let end = ((block + 1) * page.len().div_ceil(2)).min(page.len());
            let slice = if start < end {
                &page[start..end]
            } else {
                &page[..1]
            };
            let mut mean = vec![0.0_f32; dimensions];
            for vector in slice {
                for (total, value) in mean.iter_mut().zip(vector) {
                    *total += *value;
                }
            }
            for value in &mut mean {
                *value /= slice.len() as f32;
            }
            block_means.push(mean);
        }
    }
    let summary_quantizer = fit_quantizer(&block_means, dimensions, 0xB0_25_0002)?;
    let summary_codes = block_means
        .iter()
        .map(|vector| summary_quantizer.encode(vector))
        .collect::<Result<Vec<_>>>()?;

    let mut objects = Vec::new();
    let (codebook_ref, codebook_object) = artifact(
        "router-codebooks",
        "parquet",
        encode_codebooks(
            &padded_codebooks(&row_quantizer, dimensions),
            &padded_codebooks(&summary_quantizer, dimensions),
            dimensions,
        )?,
    );
    objects.push(codebook_object);
    let (row_code_ref, row_code_object) = artifact(
        "router-row-codes",
        "parquet",
        encode_row_codes(row_codes.iter().flatten().copied().collect())?,
    );
    objects.push(row_code_object);
    let (summary_ref, summary_object) = artifact(
        "router-summary-codes",
        "parquet",
        encode_summary_codes(
            summary_codes.iter().flatten().copied().collect(),
            rows.len().div_ceil(PAGE_ROWS),
        )?,
    );
    objects.push(summary_object);

    let mut run_bytes = Vec::new();
    let mut pages = Vec::new();
    for (page, (page_rows, page_codes)) in rows
        .chunks(PAGE_ROWS)
        .zip(vectors.chunks(PAGE_ROWS))
        .enumerate()
    {
        let page_vectors = page_codes.iter().flatten().copied().collect::<Vec<_>>();
        let bytes = encode_page(page_rows, &page_vectors, dimensions)?;
        let start = run_bytes.len() as u64;
        run_bytes.extend_from_slice(&bytes);
        pages.push(NativePageRef {
            page: page as u32,
            object: String::new(),
            range: start..run_bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            rows: page_rows.len() as u32,
        });
    }
    let (base_ref, base_object) = artifact("base-run", "arrow", run_bytes);
    for page in &mut pages {
        page.object.clone_from(&base_object.path);
    }
    objects.push(base_object);
    let (page_directory_ref, page_directory_object) =
        artifact("page-directory", "parquet", encode_page_directory(&pages)?);
    objects.push(page_directory_object);
    let (mutation_ref, mutation_object) = artifact(
        "mutation-directory",
        "arrow",
        encode_mutation_directory(&[])?,
    );
    objects.push(mutation_object);
    objects.sort_by(|left, right| left.path.cmp(&right.path));

    let start = rows.iter().map(|row| row.version).min().unwrap();
    let end = rows.iter().map(|row| row.version).max().unwrap();
    let reference = NativeAnnRef {
        format_version: 2,
        generation: config.generation,
        previous_generation_sha256: config.previous_generation_sha256,
        source_identity: config.source_identity,
        dimensions: config.dimensions,
        metric: config.metric,
        page_rows: PAGE_ROWS as u32,
        router: NativeRouterRef {
            pq_width: PQ_WIDTH as u8,
            summary_codes_per_page: 2,
            physical_rows: rows.len() as u64,
            page_count: pages.len() as u32,
            codebooks: codebook_ref,
            row_codes: row_code_ref,
            summary_codes: summary_ref,
        },
        page_directory: page_directory_ref,
        mutation_directory: mutation_ref,
        base_runs: vec![NativeRunRef {
            ordinal: 0,
            rows: rows.len() as u64,
            version_start: version_hex(&start),
            version_end: version_hex(&end),
            artifact: base_ref,
        }],
        delta_runs: Vec::new(),
    };
    let root_bytes = native_ann_root_bytes(&reference)?;
    let root_sha256 = format!("{:x}", Sha256::digest(&root_bytes));
    let root_path = format!("native-ann/generations/{root_sha256}.json");
    Ok(NativeBuildOutput {
        reference,
        root_bytes,
        root_sha256,
        root_path,
        objects,
        page_directory: pages,
    })
}

fn read_bound_artifact(storage: &Storage, reference: &NativeArtifactRef) -> Result<Vec<u8>> {
    reference.validate(&reference.role)?;
    let uri = url::Url::parse(&reference.uri)
        .map_err(|error| invalid(format!("native ANN artifact URI is invalid: {error}")))?;
    let path = uri.path().trim_start_matches('/');
    let bytes = storage
        .read_object_fresh(path)?
        .ok_or_else(|| invalid(format!("native ANN artifact `{path}` is absent")))?;
    if reference.encoded_bytes != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || reference.sha256 != format!("{:x}", Sha256::digest(&bytes))
    {
        return Err(invalid(format!(
            "native ANN {} byte authority differs",
            reference.role
        )));
    }
    Ok(bytes)
}

pub(crate) fn build_native_delta_generation(
    storage: &Storage,
    previous: &NativeAnnRef,
    mut rows: Vec<NativeBuildRow>,
) -> Result<NativeBuildOutput> {
    previous.validate()?;
    let dimensions = usize::try_from(previous.dimensions)
        .map_err(|_| invalid("native ANN delta dimensions exceed usize"))?;
    if rows.is_empty()
        || rows.iter().any(|row| {
            row.id.is_empty()
                || row.sequence == 0
                || row.vector.len() != dimensions
                || row.vector.iter().any(|value| !value.is_finite())
        })
    {
        return Err(invalid("native ANN delta rows differ"));
    }
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    if rows.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(invalid("native ANN delta IDs are not unique"));
    }

    let vectors = rows
        .iter()
        .flat_map(|row| row.vector.iter().copied())
        .collect::<Vec<_>>();

    let mut objects = Vec::new();
    let (delta_ref, delta_object) = artifact(
        "delta-run",
        "arrow",
        encode_page(&rows, &vectors, dimensions)?,
    );
    objects.push(delta_object);

    let previous_directory = read_bound_artifact(storage, &previous.mutation_directory)?;
    let mut mutations = decode_mutation_directory(&previous_directory)?
        .into_iter()
        .map(|entry| (entry.id.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    for row in &rows {
        let entry = NativeMutationBuildEntry {
            id: row.id.clone(),
            sequence: row.sequence,
            version: row.version,
            state: row.state,
        };
        if mutations
            .get(&entry.id)
            .is_some_and(|current| current.sequence >= entry.sequence)
        {
            return Err(invalid("native ANN delta mutation order differs"));
        }
        mutations.insert(entry.id.clone(), entry);
    }
    let mutation_rows = mutations.into_values().collect::<Vec<_>>();
    let (mutation_directory, mutation_object) = artifact(
        "mutation-directory",
        "arrow",
        encode_mutation_directory(&mutation_rows)?,
    );
    objects.push(mutation_object);
    objects.sort_by(|left, right| left.path.cmp(&right.path));

    let version_start = rows.iter().map(|row| row.version).min().unwrap();
    let version_end = rows.iter().map(|row| row.version).max().unwrap();
    if previous
        .base_runs
        .last()
        .into_iter()
        .chain(previous.delta_runs.last())
        .any(|run| run.version_end >= version_hex(&version_start))
    {
        return Err(invalid("native ANN delta mutation range is not newer"));
    }
    let previous_root = native_ann_root_bytes(previous)?;
    let previous_root_sha256 = format!("{:x}", Sha256::digest(&previous_root));
    let mut source_hasher = blake3::Hasher::new();
    source_hasher.update(b"borsuk-native-ann-delta-v1\0");
    source_hasher.update(previous.source_identity.as_bytes());
    source_hasher.update(&delta_ref.sha256.as_bytes());

    let mut reference = previous.clone();
    reference.generation = previous
        .generation
        .checked_add(1)
        .ok_or_else(|| invalid("native ANN generation overflows"))?;
    reference.previous_generation_sha256 = Some(previous_root_sha256);
    reference.source_identity = source_hasher.finalize().to_hex().to_string();
    reference.mutation_directory = mutation_directory;
    reference.delta_runs.push(NativeRunRef {
        ordinal: reference.delta_runs.len() as u32,
        rows: rows.len() as u64,
        version_start: version_hex(&version_start),
        version_end: version_hex(&version_end),
        artifact: delta_ref,
    });
    reference.validate()?;
    let root_bytes = native_ann_root_bytes(&reference)?;
    let root_sha256 = format!("{:x}", Sha256::digest(&root_bytes));
    let root_path = format!("native-ann/generations/{root_sha256}.json");
    Ok(NativeBuildOutput {
        reference,
        root_bytes,
        root_sha256,
        root_path,
        objects,
        page_directory: Vec::new(),
    })
}

pub(crate) fn stage_native_generation(storage: &Storage, built: &NativeBuildOutput) -> Result<()> {
    for object in &built.objects {
        storage.write_bytes_content_addressed(&object.path, &object.bytes)?;
    }
    storage.write_bytes_content_addressed(&built.root_path, &built.root_bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{metric::VectorMetric, native_ann_read::NativeRowState, storage::Storage};

    fn version(value: u8) -> [u8; 24] {
        let mut version = [0_u8; 24];
        version[0] = value;
        version
    }

    fn row(id: impl ToString, sequence: u64, value: f32) -> NativeBuildRow {
        NativeBuildRow {
            id: id.to_string().into_bytes(),
            sequence,
            version: version(sequence as u8),
            state: NativeRowState::Live,
            vector: vec![value; 16],
        }
    }

    fn config(generation: u64) -> NativeBuildConfig {
        NativeBuildConfig {
            generation,
            previous_generation_sha256: (generation > 1).then(|| "a".repeat(64)),
            source_identity: "native-build-fixture".to_owned(),
            metric: VectorMetric::Euclidean,
            dimensions: 16,
        }
    }

    #[test]
    fn native_ann_build_is_query_blind_and_byte_deterministic() {
        let rows = (0..520)
            .map(|ordinal| row(ordinal, 1, ordinal as f32 / 17.0))
            .collect::<Vec<_>>();
        let mut reversed = rows.clone();
        reversed.reverse();

        let first = build_native_generation(config(1), [rows.as_slice()]).unwrap();
        let second = build_native_generation(config(1), [reversed.as_slice()]).unwrap();

        assert_eq!(first.root_bytes, second.root_bytes);
        assert_eq!(first.objects, second.objects);
        assert_eq!(first.reference.router.physical_rows, 520);
        assert_eq!(first.reference.router.page_count, 3);
        assert_eq!(first.page_directory.len(), 3);
        assert!(
            first
                .objects
                .iter()
                .all(|object| !object.path.contains("query"))
        );
        let roles = first
            .objects
            .iter()
            .map(|object| object.role.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            roles,
            BTreeSet::from([
                "base-run",
                "mutation-directory",
                "page-directory",
                "router-codebooks",
                "router-row-codes",
                "router-summary-codes",
            ])
        );
    }

    #[test]
    fn native_ann_build_compaction_is_latest_wins_and_enumeration_independent() {
        let mut tombstone = row(2, 3, 0.0);
        tombstone.state = NativeRowState::Tombstone;
        let runs = vec![
            vec![row(1, 1, 1.0), row(2, 1, 2.0)],
            vec![row(1, 2, 10.0)],
            vec![tombstone],
            vec![row(3, 4, 3.0)],
        ];
        let mut reversed = runs.clone();
        reversed.reverse();

        let compact = compact_native_rows(runs.iter().map(Vec::as_slice)).unwrap();
        let compact_reversed = compact_native_rows(reversed.iter().map(Vec::as_slice)).unwrap();
        assert_eq!(compact, compact_reversed);
        assert_eq!(
            compact
                .iter()
                .map(|row| row.id.as_slice())
                .collect::<Vec<_>>(),
            [b"1".as_slice(), b"3".as_slice()]
        );
        assert_eq!(compact[0].sequence, 2);

        let one = build_native_generation(config(1), [compact.as_slice()]).unwrap();
        let ten = build_native_generation(
            config(1),
            compact.chunks(1).cycle().take(10).collect::<Vec<_>>(),
        )
        .unwrap();
        let hundred = build_native_generation(
            config(1),
            compact.chunks(1).cycle().take(100).collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(one.root_bytes, ten.root_bytes);
        assert_eq!(one.root_bytes, hundred.root_bytes);
    }

    #[test]
    fn native_ann_build_staging_is_immutable_and_does_not_publish_visibility() {
        let storage = Storage::from_uri("memory:///native-ann-stage").unwrap();
        let built = build_native_generation(config(1), [[row(1, 1, 1.0)].as_slice()]).unwrap();

        stage_native_generation(&storage, &built).unwrap();

        assert_eq!(
            storage
                .read_object_fresh(&built.root_path)
                .unwrap()
                .unwrap(),
            built.root_bytes
        );
        assert!(
            storage
                .read_object_fresh("native-ann/HEAD.json")
                .unwrap()
                .is_none(),
            "immutable staging must not create a second visibility authority"
        );
    }

    #[test]
    fn native_bounded_build_is_query_blind_deterministic_and_reopens_sq8() {
        let storage = Storage::from_uri("memory:///native-bounded-build").unwrap();
        let config = NativeBuildConfig {
            generation: 1,
            previous_generation_sha256: None,
            source_identity: "native-bounded-build-fixture".to_owned(),
            metric: VectorMetric::SquaredEuclidean,
            dimensions: 64,
        };
        let rows = (0..520)
            .map(|ordinal| NativeBuildRow {
                id: format!("{ordinal:04}").into_bytes(),
                sequence: 1,
                version: version(1),
                state: NativeRowState::Live,
                vector: vec![ordinal as f32 / 17.0; 64],
            })
            .collect::<Vec<_>>();
        let mut reversed = rows.clone();
        reversed.reverse();

        let first = build_native_bounded_generation(config.clone(), [rows.as_slice()]).unwrap();
        let second = build_native_bounded_generation(config, [reversed.as_slice()]).unwrap();
        assert_eq!(first.root_bytes, second.root_bytes);
        assert_eq!(first.objects, second.objects);
        assert_eq!(first.reference.format_version, 3);
        assert_eq!(first.reference.router.pq_width, 64);
        assert_eq!(first.reference.router.physical_rows, 520);
        assert_eq!(first.reference.router.page_count, 3);
        assert_eq!(first.page_directory.len(), 3);
        assert_eq!(
            first
                .objects
                .iter()
                .map(|object| object.role.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "base-run",
                "mutation-directory",
                "page-directory",
                "route-codebooks",
                "route-row-codes",
                "route-summaries",
            ])
        );
        assert!(
            first
                .objects
                .iter()
                .all(|object| !object.path.contains("query") && !object.path.contains("truth"))
        );

        stage_native_bounded_generation(&storage, &first).unwrap();
        let snapshot = crate::native_ann_read::load_native_bounded_ann_snapshot(
            storage,
            &first.reference,
            None,
        )
        .unwrap();
        let outcome = snapshot.search(&[0.0; 64], 10).unwrap();
        assert_eq!(outcome.hits[0].id, b"0000");
        assert!(outcome.pages_read <= first.reference.router.limits.max_output_pages as usize);
        assert!(outcome.physical_gets <= 1);
    }
}
