use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap},
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use sha2::{Digest, Sha256};

use crate::{
    Result, V25ContainmentSample, V25Control, V25ObjectIdentity, V25QueryTruth, V25RankedRow,
    V25RowPages, exact_oracle_pages, hits, invalid, ppm, select_v25_rank_sharp_pages,
    validate_identity,
};

#[derive(Debug, Clone, PartialEq)]
pub struct V25ConstructionRow {
    pub source_ordinal: u64,
    pub vector: [f32; 96],
}

#[derive(Debug, Clone, PartialEq)]
pub struct V25LocalQuery {
    pub query_ordinal: u32,
    pub source_ordinal: u64,
    pub vector: [f32; 96],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V25LocalObjectPath {
    pub identity: V25ObjectIdentity,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V25ContainmentLocalRequest {
    pub construction_rows: V25LocalObjectPath,
    pub page_assignments: V25LocalObjectPath,
    pub pseudoqueries: V25LocalObjectPath,
    pub truth: V25LocalObjectPath,
    pub ranked_row_limits: Vec<u32>,
    pub page_budget: u32,
    pub expected_source_rows: u64,
    pub expected_page_count: u32,
    pub expected_queries: u32,
    pub construction_batch_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V25ContainmentLocalOutput {
    pub samples: Vec<V25ContainmentSample>,
    pub scanned_rows: u64,
    pub peak_construction_batch_rows: u64,
    pub peak_ranked_rows_retained: u64,
    pub page_body_reads: u64,
}

fn vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("element", DataType::Float32, false)),
        96,
    )
}

pub fn validate_v25_construction_schema(schema: &Schema) -> Result<()> {
    let expected = Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("vector", vector_type(), false),
    ]);
    if schema != &expected {
        return Err(invalid("V25 construction Parquet schema differs"));
    }
    Ok(())
}

pub fn validate_v25_page_assignment_schema(schema: &Schema) -> Result<()> {
    let expected = Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ]);
    if schema != &expected {
        return Err(invalid("V25 page assignment Parquet schema differs"));
    }
    Ok(())
}

pub fn validate_v25_query_schema(schema: &Schema) -> Result<()> {
    let expected = Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("vector", vector_type(), false),
    ]);
    if schema != &expected {
        return Err(invalid("V25 query Parquet schema differs"));
    }
    Ok(())
}

pub fn validate_v25_truth_schema(schema: &Schema) -> Result<()> {
    let page_list = |length| {
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::UInt32, false)),
            length,
        )
    };
    let expected = Schema::new(vec![
        Field::new("query_ordinal", DataType::UInt32, false),
        Field::new(
            "neighbor_source_ordinals",
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt64, false)), 10),
            false,
        ),
        Field::new("primary_pages", page_list(10), false),
        Field::new("replica_pages", page_list(10), false),
        Field::new("oracle_pages", page_list(8), false),
    ]);
    if schema != &expected {
        return Err(invalid("V25 truth Parquet schema differs"));
    }
    Ok(())
}

fn authenticate_local_object(
    object: &V25LocalObjectPath,
    expected_role: &str,
    generation: &str,
) -> Result<()> {
    if object.identity.role != expected_role {
        return Err(invalid("V25 local object role differs"));
    }
    validate_identity(&object.identity, generation)?;
    let mut file = fs::File::open(&object.path)
        .map_err(|error| invalid(&format!("V25 local object open failed: {error}")))?;
    let encoded_bytes = file
        .metadata()
        .map_err(|error| invalid(&format!("V25 local object metadata failed: {error}")))?
        .len();
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| invalid(&format!("V25 local object read failed: {error}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if encoded_bytes != object.identity.encoded_bytes
        || format!("{:x}", hasher.finalize()) != object.identity.digest
    {
        return Err(invalid("V25 local object bytes differ"));
    }
    Ok(())
}

fn fixed_f32_vector(list: &FixedSizeListArray, row: usize) -> Result<[f32; 96]> {
    if list.null_count() != 0 || list.offset() != 0 {
        return Err(invalid("V25 vector list differs"));
    }
    let value = list.value(row);
    let values = value
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V25 vector child differs"))?;
    if values.null_count() != 0 || values.len() != 96 {
        return Err(invalid("V25 vector width differs"));
    }
    let vector = values.values().as_ref().try_into().unwrap();
    validate_vector(&vector)?;
    Ok(vector)
}

fn open_reader(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<fs::File>> {
    let file = fs::File::open(path)
        .map_err(|error| invalid(&format!("V25 Parquet open failed: {error}")))?;
    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| invalid(&format!("V25 Parquet metadata failed: {error}")))
}

fn control_ordinal(control: V25Control) -> u8 {
    match control {
        V25Control::Layout => 0,
        V25Control::ExactGlobal => 1,
        V25Control::ExactContained => 2,
        V25Control::CodedContained => 3,
        V25Control::Bounded => 4,
    }
}

fn control_from_ordinal(ordinal: u8) -> Result<V25Control> {
    match ordinal {
        0 => Ok(V25Control::Layout),
        1 => Ok(V25Control::ExactGlobal),
        2 => Ok(V25Control::ExactContained),
        3 => Ok(V25Control::CodedContained),
        4 => Ok(V25Control::Bounded),
        _ => Err(invalid("V25 evidence control differs")),
    }
}

fn evidence_schema(generation: &str) -> Arc<Schema> {
    let metadata = HashMap::from([(
        "borsuk.authority".to_owned(),
        format!("v25-containment-evidence-v1:{generation}"),
    )]);
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new("control_ordinal", DataType::UInt8, false),
            Field::new("page_budget", DataType::UInt32, false),
            Field::new("ranked_row_limit", DataType::UInt32, false),
            Field::new("candidate_rows", DataType::UInt64, false),
            Field::new(
                "selected_pages",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    16,
                ),
                false,
            ),
            Field::new("selected_page_count", DataType::UInt32, false),
            Field::new("hits", DataType::UInt32, false),
            Field::new("oracle_hits", DataType::UInt32, false),
            Field::new("recall_ppm", DataType::UInt64, false),
            Field::new("oracle_attainment_ppm", DataType::UInt64, false),
        ],
        metadata,
    ))
}

fn validate_evidence_samples(samples: &[V25ContainmentSample], page_budget: u32) -> Result<()> {
    if samples.is_empty() || ![8, 12, 16].contains(&page_budget) {
        return Err(invalid("V25 evidence request differs"));
    }
    let mut prior = None;
    for sample in samples {
        let key = (
            sample.query_ordinal,
            control_ordinal(sample.control),
            page_budget,
            sample.ranked_row_limit,
        );
        if prior.is_some_and(|prior| prior >= key)
            || sample.selected_pages.len() > page_budget as usize
            || sample
                .selected_pages
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || sample.hits > 10
            || sample.oracle_hits > 10
            || sample.hits > sample.oracle_hits
        {
            return Err(invalid("V25 evidence sample differs"));
        }
        prior = Some(key);
    }
    Ok(())
}

fn local_identity(
    role: &str,
    uri: &str,
    generation: &str,
    path: &Path,
) -> Result<V25LocalObjectPath> {
    let bytes =
        fs::read(path).map_err(|error| invalid(&format!("V25 evidence read failed: {error}")))?;
    let identity = V25ObjectIdentity {
        role: role.to_owned(),
        uri: uri.to_owned(),
        digest_algorithm: "sha256".to_owned(),
        digest: format!("{:x}", Sha256::digest(&bytes)),
        encoded_bytes: bytes.len() as u64,
        generation: generation.to_owned(),
    };
    validate_identity(&identity, generation)?;
    Ok(V25LocalObjectPath {
        identity,
        path: path.to_owned(),
    })
}

pub fn write_v25_containment_evidence(
    path: &Path,
    uri: &str,
    generation: &str,
    page_budget: u32,
    samples: &[V25ContainmentSample],
) -> Result<V25LocalObjectPath> {
    validate_evidence_samples(samples, page_budget)?;
    let mut selected_pages = Vec::with_capacity(samples.len() * 16);
    for sample in samples {
        selected_pages.extend_from_slice(&sample.selected_pages);
        selected_pages.resize(
            selected_pages.len() + 16 - sample.selected_pages.len(),
            u32::MAX,
        );
    }
    let selected_pages = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::UInt32, false)),
        16,
        Arc::new(UInt32Array::from(selected_pages)),
        None,
    )
    .map_err(|error| invalid(&format!("V25 evidence pages failed: {error}")))?;
    let schema = evidence_schema(generation);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.query_ordinal),
            )) as ArrayRef,
            Arc::new(UInt8Array::from_iter_values(
                samples.iter().map(|sample| control_ordinal(sample.control)),
            )),
            Arc::new(UInt32Array::from_value(page_budget, samples.len())),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.ranked_row_limit),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.candidate_rows),
            )),
            Arc::new(selected_pages),
            Arc::new(UInt32Array::from_iter_values(
                samples
                    .iter()
                    .map(|sample| sample.selected_pages.len() as u32),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_hits),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.recall_ppm),
            )),
            Arc::new(UInt64Array::from_iter_values(
                samples.iter().map(|sample| sample.oracle_attainment_ppm),
            )),
        ],
    )
    .map_err(|error| invalid(&format!("V25 evidence batch failed: {error}")))?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(1_024))
        .set_data_page_size_limit(256 * 1024)
        .build();
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| invalid(&format!("V25 evidence create failed: {error}")))?;
    let write_result = (|| -> Result<()> {
        let mut writer = ArrowWriter::try_new(file, schema, Some(properties))
            .map_err(|error| invalid(&format!("V25 evidence writer failed: {error}")))?;
        writer
            .write(&batch)
            .map_err(|error| invalid(&format!("V25 evidence write failed: {error}")))?;
        writer
            .close()
            .map_err(|error| invalid(&format!("V25 evidence close failed: {error}")))?;
        fs::OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|file| file.sync_all())
            .map_err(|error| invalid(&format!("V25 evidence sync failed: {error}")))?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(path);
        return Err(error);
    }
    let object = local_identity("containment-evidence-parquet", uri, generation, path)?;
    read_v25_containment_evidence(&object, page_budget, samples)?;
    Ok(object)
}

pub fn read_v25_containment_evidence(
    object: &V25LocalObjectPath,
    page_budget: u32,
    expected: &[V25ContainmentSample],
) -> Result<()> {
    validate_evidence_samples(expected, page_budget)?;
    authenticate_local_object(
        object,
        "containment-evidence-parquet",
        &object.identity.generation,
    )?;
    let builder = open_reader(&object.path)?;
    if builder.schema().as_ref() != evidence_schema(&object.identity.generation).as_ref()
        || usize::try_from(builder.metadata().file_metadata().num_rows()).ok()
            != Some(expected.len())
    {
        return Err(invalid("V25 evidence Parquet authority differs"));
    }
    let mut observed = Vec::with_capacity(expected.len());
    for batch in builder
        .build()
        .map_err(|error| invalid(&format!("V25 evidence reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V25 evidence batch failed: {error}")))?;
        if batch.num_columns() != 11
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V25 evidence batch differs"));
        }
        let u32_column = |column: usize| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid("V25 evidence u32 column differs"))
        };
        let u64_column = |column: usize| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| invalid("V25 evidence u64 column differs"))
        };
        let controls = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("V25 evidence control column differs"))?;
        let pages = batch
            .column(5)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V25 evidence page column differs"))?;
        for row in 0..batch.num_rows() {
            if u32_column(2)?.value(row) != page_budget {
                return Err(invalid("V25 evidence page budget differs"));
            }
            let selected_count = usize::try_from(u32_column(6)?.value(row)).unwrap();
            let encoded_pages = fixed_u32_values(pages, row, 16)?;
            if selected_count > page_budget as usize
                || encoded_pages[selected_count..]
                    .iter()
                    .any(|page| *page != u32::MAX)
            {
                return Err(invalid("V25 evidence page padding differs"));
            }
            observed.push(V25ContainmentSample {
                query_ordinal: u32_column(0)?.value(row),
                control: control_from_ordinal(controls.value(row))?,
                ranked_row_limit: u32_column(3)?.value(row),
                candidate_rows: u64_column(4)?.value(row),
                selected_pages: encoded_pages[..selected_count].to_vec(),
                hits: u32_column(7)?.value(row),
                oracle_hits: u32_column(8)?.value(row),
                recall_ppm: u64_column(9)?.value(row),
                oracle_attainment_ppm: u64_column(10)?.value(row),
            });
        }
    }
    validate_evidence_samples(&observed, page_budget)?;
    if observed != expected {
        return Err(invalid("V25 evidence rows differ"));
    }
    Ok(())
}

fn read_page_assignments(
    path: &Path,
    expected_rows: u64,
    expected_page_count: u32,
) -> Result<Vec<V25RowPages>> {
    let builder = open_reader(path)?;
    validate_v25_page_assignment_schema(builder.schema())?;
    if u64::try_from(builder.metadata().file_metadata().num_rows()).ok() != Some(expected_rows) {
        return Err(invalid("V25 page assignment row count differs"));
    }
    let mut rows = Vec::with_capacity(expected_rows as usize);
    for batch in builder
        .build()
        .map_err(|error| invalid(&format!("V25 page reader failed: {error}")))?
    {
        let batch = batch.map_err(|error| invalid(&format!("V25 page batch failed: {error}")))?;
        if batch.num_columns() != 3
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V25 page assignment batch differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V25 page source ordinal differs"))?;
        let primary = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V25 primary page differs"))?;
        let replica = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V25 replica page differs"))?;
        for row in 0..batch.num_rows() {
            let primary_page = primary.value(row);
            let replica_page = replica.value(row);
            if primary_page >= expected_page_count
                || (replica_page != u32::MAX
                    && (replica_page >= expected_page_count || replica_page == primary_page))
            {
                return Err(invalid("V25 page assignment page differs"));
            }
            rows.push(V25RowPages {
                source_ordinal: ordinals.value(row),
                primary_page,
                replica_page: (replica_page != u32::MAX).then_some(replica_page),
            });
        }
    }
    if rows.len() != expected_rows as usize {
        return Err(invalid("V25 page assignment decoded row count differs"));
    }
    if rows
        .iter()
        .enumerate()
        .any(|(ordinal, row)| usize::try_from(row.source_ordinal).ok() != Some(ordinal))
    {
        return Err(invalid("V25 page assignment source inventory differs"));
    }
    Ok(rows)
}

fn read_queries(path: &Path, expected_queries: u32) -> Result<Vec<V25LocalQuery>> {
    let builder = open_reader(path)?;
    validate_v25_query_schema(builder.schema())?;
    if u32::try_from(builder.metadata().file_metadata().num_rows()).ok() != Some(expected_queries) {
        return Err(invalid("V25 query row count differs"));
    }
    let mut queries = Vec::with_capacity(expected_queries as usize);
    for batch in builder
        .build()
        .map_err(|error| invalid(&format!("V25 query reader failed: {error}")))?
    {
        let batch = batch.map_err(|error| invalid(&format!("V25 query batch failed: {error}")))?;
        if batch.num_columns() != 3
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V25 query batch differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V25 query ordinal differs"))?;
        let sources = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V25 query source differs"))?;
        let vectors = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V25 query vector differs"))?;
        for row in 0..batch.num_rows() {
            queries.push(V25LocalQuery {
                query_ordinal: ordinals.value(row),
                source_ordinal: sources.value(row),
                vector: fixed_f32_vector(vectors, row)?,
            });
        }
    }
    if queries.len() != expected_queries as usize {
        return Err(invalid("V25 query decoded row count differs"));
    }
    Ok(queries)
}

fn fixed_u32_values(list: &FixedSizeListArray, row: usize, width: usize) -> Result<Vec<u32>> {
    if list.null_count() != 0 || list.offset() != 0 {
        return Err(invalid("V25 truth list differs"));
    }
    let value = list.value(row);
    let values = value
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V25 truth list child differs"))?;
    if values.null_count() != 0 || values.len() != width {
        return Err(invalid("V25 truth list width differs"));
    }
    Ok(values.values().to_vec())
}

fn read_truth(path: &Path, expected_queries: u32) -> Result<Vec<V25QueryTruth>> {
    let builder = open_reader(path)?;
    validate_v25_truth_schema(builder.schema())?;
    if u32::try_from(builder.metadata().file_metadata().num_rows()).ok() != Some(expected_queries) {
        return Err(invalid("V25 truth row count differs"));
    }
    let mut truths = Vec::with_capacity(expected_queries as usize);
    for batch in builder
        .build()
        .map_err(|error| invalid(&format!("V25 truth reader failed: {error}")))?
    {
        let batch = batch.map_err(|error| invalid(&format!("V25 truth batch failed: {error}")))?;
        if batch.num_columns() != 5
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V25 truth batch differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("V25 truth ordinal differs"))?;
        let neighbor_lists = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V25 truth neighbor list differs"))?;
        let lists = [2_usize, 3, 4]
            .map(|column| {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| invalid("V25 truth list differs"))
            })
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            let neighbor_value = neighbor_lists.value(row);
            let neighbor_values = neighbor_value
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| invalid("V25 truth neighbor child differs"))?;
            if neighbor_lists.offset() != 0
                || neighbor_values.null_count() != 0
                || neighbor_values.len() != 10
            {
                return Err(invalid("V25 truth neighbor width differs"));
            }
            let primary = fixed_u32_values(lists[0], row, 10)?;
            let replica = fixed_u32_values(lists[1], row, 10)?;
            let mut assignments = Vec::with_capacity(10);
            for (&primary, &replica) in primary.iter().zip(&replica) {
                let mut pages = vec![primary];
                if replica != u32::MAX {
                    pages.push(replica);
                    pages.sort_unstable();
                }
                assignments.push(pages);
            }
            let oracle = fixed_u32_values(lists[2], row, 8)?;
            let oracle_length = oracle
                .iter()
                .position(|page| *page == u32::MAX)
                .unwrap_or(8);
            if oracle[oracle_length..].iter().any(|page| *page != u32::MAX) {
                return Err(invalid("V25 truth oracle padding differs"));
            }
            truths.push(V25QueryTruth {
                query_ordinal: ordinals.value(row),
                neighbor_source_ordinals: neighbor_values.values().to_vec(),
                ground_truth_page_assignments: assignments,
                oracle_pages: oracle[..oracle_length].to_vec(),
            });
        }
    }
    if truths.len() != expected_queries as usize {
        return Err(invalid("V25 truth decoded row count differs"));
    }
    Ok(truths)
}

pub fn run_v25_containment_local_request(
    request: &V25ContainmentLocalRequest,
) -> Result<V25ContainmentLocalOutput> {
    let generation = request.construction_rows.identity.generation.as_str();
    for (object, role) in [
        (&request.construction_rows, "construction-rows-parquet"),
        (&request.page_assignments, "page-assignments-parquet"),
        (&request.pseudoqueries, "pseudoqueries-parquet"),
        (&request.truth, "truth-parquet"),
    ] {
        authenticate_local_object(object, role, generation)?;
    }
    let pages = read_page_assignments(
        &request.page_assignments.path,
        request.expected_source_rows,
        request.expected_page_count,
    )?;
    let queries = read_queries(&request.pseudoqueries.path, request.expected_queries)?;
    let truths = read_truth(&request.truth.path, request.expected_queries)?;
    let (samples, peak_construction_batch_rows, peak_ranked_rows_retained) =
        evaluate_v25_exact_global_parquet(request, &pages, &queries, &truths)?;
    Ok(V25ContainmentLocalOutput {
        samples,
        scanned_rows: request.expected_source_rows,
        peak_construction_batch_rows,
        peak_ranked_rows_retained,
        page_body_reads: 0,
    })
}

#[derive(Debug, Clone, Copy)]
struct HeapRow(V25RankedRow);

impl PartialEq for HeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.0.distance.total_cmp(&other.0.distance) == Ordering::Equal
            && self.0.source_ordinal == other.0.source_ordinal
    }
}

impl Eq for HeapRow {}

impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then_with(|| self.0.source_ordinal.cmp(&other.0.source_ordinal))
    }
}

fn validate_streaming_truth(
    pages: &[V25RowPages],
    queries: &[V25LocalQuery],
    truths: &[V25QueryTruth],
    page_budget: u32,
) -> Result<()> {
    for (query_index, (query, truth)) in queries.iter().zip(truths).enumerate() {
        validate_vector(&query.vector)?;
        if usize::try_from(query.query_ordinal).ok() != Some(query_index)
            || truth.query_ordinal != query.query_ordinal
            || truth.neighbor_source_ordinals.len() != 10
            || truth.ground_truth_page_assignments.len() != 10
            || truth
                .neighbor_source_ordinals
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != 10
            || usize::try_from(query.source_ordinal)
                .ok()
                .is_none_or(|source| source >= pages.len())
            || exact_oracle_pages(&truth.ground_truth_page_assignments, page_budget as usize)?
                != truth.oracle_pages
        {
            return Err(invalid("V25 exact-global query authority differs"));
        }
        for (neighbor, expected_pages) in truth
            .neighbor_source_ordinals
            .iter()
            .zip(&truth.ground_truth_page_assignments)
        {
            let assignment = usize::try_from(*neighbor)
                .ok()
                .and_then(|source| pages.get(source))
                .ok_or_else(|| invalid("V25 truth neighbor source differs"))?;
            let mut observed = vec![assignment.primary_page];
            if let Some(replica) = assignment.replica_page {
                observed.push(replica);
                observed.sort_unstable();
            }
            if &observed != expected_pages || *neighbor == query.source_ordinal {
                return Err(invalid("V25 truth neighbor page binding differs"));
            }
        }
    }
    Ok(())
}

fn evaluate_v25_exact_global_parquet(
    request: &V25ContainmentLocalRequest,
    pages: &[V25RowPages],
    queries: &[V25LocalQuery],
    truths: &[V25QueryTruth],
) -> Result<(Vec<V25ContainmentSample>, u64, u64)> {
    if request.construction_batch_rows == 0
        || pages.len() != usize::try_from(request.expected_source_rows).unwrap_or(usize::MAX)
        || queries.is_empty()
        || queries.len() != truths.len()
        || request.page_budget != 8
        || request.ranked_row_limits.is_empty()
        || request
            .ranked_row_limits
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || request
            .ranked_row_limits
            .iter()
            .any(|limit| ![10, 32, 128, 512, 2_048, 4_096].contains(limit))
    {
        return Err(invalid("V25 exact-global streaming request differs"));
    }
    validate_streaming_truth(pages, queries, truths, request.page_budget)?;
    let retained_limit = usize::try_from(*request.ranked_row_limits.last().unwrap()).unwrap();
    let mut heaps = (0..queries.len())
        .map(|_| BinaryHeap::<HeapRow>::with_capacity(retained_limit))
        .collect::<Vec<_>>();
    let mut candidate_counts = vec![0_u64; queries.len()];
    let mut scanned_rows = 0_u64;
    let mut peak_batch_rows = 0_u64;

    let builder = open_reader(&request.construction_rows.path)?;
    validate_v25_construction_schema(builder.schema())?;
    if u64::try_from(builder.metadata().file_metadata().num_rows()).ok()
        != Some(request.expected_source_rows)
    {
        return Err(invalid("V25 construction Parquet row count differs"));
    }
    for batch in builder
        .with_batch_size(request.construction_batch_rows)
        .build()
        .map_err(|error| invalid(&format!("V25 construction reader failed: {error}")))?
    {
        let batch =
            batch.map_err(|error| invalid(&format!("V25 construction batch failed: {error}")))?;
        peak_batch_rows = peak_batch_rows.max(batch.num_rows() as u64);
        if batch.num_columns() != 2
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V25 construction batch differs"));
        }
        let ordinals = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V25 construction ordinal differs"))?;
        let vectors = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V25 construction vector differs"))?;
        for row_index in 0..batch.num_rows() {
            let source_ordinal = ordinals.value(row_index);
            if source_ordinal != scanned_rows {
                return Err(invalid("V25 construction source inventory differs"));
            }
            let vector = fixed_f32_vector(vectors, row_index)?;
            let assignment = pages
                .get(usize::try_from(source_ordinal).unwrap())
                .ok_or_else(|| invalid("V25 construction page inventory differs"))?;
            for (query_index, query) in queries.iter().enumerate() {
                let own = &pages[usize::try_from(query.source_ordinal).unwrap()];
                if source_ordinal == query.source_ordinal
                    || [Some(assignment.primary_page), assignment.replica_page]
                        .into_iter()
                        .flatten()
                        .any(|page| page == own.primary_page || Some(page) == own.replica_page)
                {
                    continue;
                }
                candidate_counts[query_index] += 1;
                let distance = 1.0
                    - query
                        .vector
                        .iter()
                        .zip(vector)
                        .map(|(left, right)| left * right)
                        .sum::<f32>();
                if !distance.is_finite() {
                    return Err(invalid("V25 exact-global distance differs"));
                }
                let candidate = HeapRow(V25RankedRow {
                    source_ordinal,
                    distance,
                    page_mass: 1,
                });
                let heap = &mut heaps[query_index];
                if heap.len() < retained_limit {
                    heap.push(candidate);
                } else if candidate < *heap.peek().unwrap() {
                    heap.pop();
                    heap.push(candidate);
                }
            }
            scanned_rows += 1;
        }
    }
    if scanned_rows != request.expected_source_rows {
        return Err(invalid("V25 construction decoded row count differs"));
    }

    let peak_retained = heaps.iter().map(BinaryHeap::len).max().unwrap_or(0) as u64;
    let mut samples = Vec::with_capacity(queries.len() * request.ranked_row_limits.len());
    for (query_index, heap) in heaps.into_iter().enumerate() {
        let mut ranked = heap.into_iter().map(|row| row.0).collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
        });
        for limit in &request.ranked_row_limits {
            let retained = &ranked[..usize::try_from(*limit).unwrap().min(ranked.len())];
            let retained_pages = retained
                .iter()
                .map(|row| pages[usize::try_from(row.source_ordinal).unwrap()])
                .collect::<Vec<_>>();
            let mut selected_pages = select_v25_rank_sharp_pages(
                retained,
                &retained_pages,
                request.page_budget as usize,
            )?;
            selected_pages.sort_unstable();
            let truth = &truths[query_index];
            let selected_hits = hits(&truth.ground_truth_page_assignments, &selected_pages);
            let oracle_hits = hits(&truth.ground_truth_page_assignments, &truth.oracle_pages);
            samples.push(V25ContainmentSample {
                query_ordinal: queries[query_index].query_ordinal,
                control: V25Control::ExactGlobal,
                ranked_row_limit: *limit,
                candidate_rows: candidate_counts[query_index],
                selected_pages,
                hits: selected_hits,
                oracle_hits,
                recall_ppm: ppm(u64::from(selected_hits), 10)?,
                oracle_attainment_ppm: ppm(u64::from(selected_hits), u64::from(oracle_hits))?,
            });
        }
    }
    Ok((samples, peak_batch_rows, peak_retained))
}

fn validate_vector(vector: &[f32; 96]) -> Result<()> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V25 vector finiteness differs"));
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>();
    if !norm.is_finite() || (norm - 1.0).abs() > 1.0e-4 {
        return Err(invalid("V25 vector normalization differs"));
    }
    Ok(())
}

pub fn evaluate_v25_exact_global(
    rows: &[V25ConstructionRow],
    assignments: &[V25RowPages],
    queries: &[V25LocalQuery],
    truths: &[V25QueryTruth],
    ranked_row_limits: &[u32],
    page_budget: u32,
) -> Result<Vec<V25ContainmentSample>> {
    if rows.is_empty()
        || rows.len() != assignments.len()
        || queries.is_empty()
        || queries.len() != truths.len()
        || page_budget != 8
        || ranked_row_limits.is_empty()
        || ranked_row_limits.windows(2).any(|pair| pair[0] >= pair[1])
        || ranked_row_limits
            .iter()
            .any(|limit| ![10, 32, 128, 512, 2_048, 4_096].contains(limit))
    {
        return Err(invalid("V25 exact-global request differs"));
    }
    let mut row_by_source = BTreeMap::new();
    for row in rows {
        validate_vector(&row.vector)?;
        if row_by_source.insert(row.source_ordinal, row).is_some() {
            return Err(invalid("V25 construction source ordinal repeats"));
        }
    }
    if row_by_source
        .keys()
        .copied()
        .ne(0..u64::try_from(rows.len()).unwrap())
    {
        return Err(invalid("V25 construction source inventory differs"));
    }
    let mut pages_by_source = BTreeMap::new();
    for assignment in assignments {
        if assignment.replica_page == Some(assignment.primary_page)
            || pages_by_source
                .insert(assignment.source_ordinal, *assignment)
                .is_some()
        {
            return Err(invalid("V25 page assignment source ordinal repeats"));
        }
    }
    if row_by_source.keys().ne(pages_by_source.keys()) {
        return Err(invalid("V25 construction page inventory differs"));
    }

    let mut output = Vec::with_capacity(queries.len() * ranked_row_limits.len());
    for (query_index, (query, truth)) in queries.iter().zip(truths).enumerate() {
        validate_vector(&query.vector)?;
        if usize::try_from(query.query_ordinal).ok() != Some(query_index)
            || truth.query_ordinal != query.query_ordinal
            || truth.neighbor_source_ordinals.len() != 10
            || truth.ground_truth_page_assignments.len() != 10
            || truth
                .neighbor_source_ordinals
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != 10
            || !row_by_source.contains_key(&query.source_ordinal)
            || exact_oracle_pages(&truth.ground_truth_page_assignments, page_budget as usize)?
                != truth.oracle_pages
        {
            return Err(invalid("V25 exact-global query authority differs"));
        }
        for (neighbor, expected_pages) in truth
            .neighbor_source_ordinals
            .iter()
            .zip(&truth.ground_truth_page_assignments)
        {
            let pages = pages_by_source
                .get(neighbor)
                .ok_or_else(|| invalid("V25 truth neighbor source differs"))?;
            let mut observed = vec![pages.primary_page];
            if let Some(replica) = pages.replica_page {
                observed.push(replica);
                observed.sort_unstable();
            }
            if &observed != expected_pages || *neighbor == query.source_ordinal {
                return Err(invalid("V25 truth neighbor page binding differs"));
            }
        }
        let own_pages = pages_by_source
            .get(&query.source_ordinal)
            .ok_or_else(|| invalid("V25 pseudoquery page binding differs"))?;
        let mut forbidden_pages = vec![own_pages.primary_page];
        if let Some(replica) = own_pages.replica_page {
            forbidden_pages.push(replica);
        }
        forbidden_pages.sort_unstable();

        let mut ranked = Vec::with_capacity(rows.len());
        for row in rows {
            let row_pages = pages_by_source.get(&row.source_ordinal).unwrap();
            let page_is_forbidden = [Some(row_pages.primary_page), row_pages.replica_page]
                .into_iter()
                .flatten()
                .any(|page| forbidden_pages.binary_search(&page).is_ok());
            if row.source_ordinal == query.source_ordinal || page_is_forbidden {
                continue;
            }
            let dot = query
                .vector
                .iter()
                .zip(row.vector)
                .map(|(left, right)| left * right)
                .sum::<f32>();
            let distance = 1.0 - dot;
            if !distance.is_finite() {
                return Err(invalid("V25 exact-global distance differs"));
            }
            ranked.push(V25RankedRow {
                source_ordinal: row.source_ordinal,
                distance,
                page_mass: 1,
            });
        }
        ranked.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
        });
        for limit in ranked_row_limits {
            let retained = ranked
                .iter()
                .copied()
                .take((*limit as usize).min(ranked.len()))
                .collect::<Vec<_>>();
            let retained_pages = retained
                .iter()
                .map(|row| *pages_by_source.get(&row.source_ordinal).unwrap())
                .collect::<Vec<_>>();
            let mut selected_pages =
                select_v25_rank_sharp_pages(&retained, &retained_pages, page_budget as usize)?;
            selected_pages.sort_unstable();
            let selected_hits = hits(&truth.ground_truth_page_assignments, &selected_pages);
            let oracle_hits = hits(&truth.ground_truth_page_assignments, &truth.oracle_pages);
            output.push(V25ContainmentSample {
                query_ordinal: query.query_ordinal,
                control: V25Control::ExactGlobal,
                ranked_row_limit: *limit,
                candidate_rows: ranked.len() as u64,
                selected_pages,
                hits: selected_hits,
                oracle_hits,
                recall_ppm: ppm(u64::from(selected_hits), 10)?,
                oracle_attainment_ppm: ppm(u64::from(selected_hits), u64::from(oracle_hits))?,
            });
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, sync::Arc};

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use sha2::{Digest, Sha256};

    use crate::{V25ObjectIdentity, V25QueryTruth, V25RowPages};

    use super::{
        V25ConstructionRow, V25ContainmentLocalRequest, V25LocalObjectPath, V25LocalQuery,
        evaluate_v25_exact_global, read_v25_containment_evidence,
        run_v25_containment_local_request, validate_v25_construction_schema,
        validate_v25_page_assignment_schema, validate_v25_query_schema, validate_v25_truth_schema,
        write_v25_containment_evidence,
    };

    fn vector(first: f32, second: f32) -> [f32; 96] {
        let norm = first.hypot(second);
        let mut vector = [0.0; 96];
        vector[0] = first / norm;
        vector[1] = second / norm;
        vector
    }

    fn vector_array(vectors: &[[f32; 96]]) -> FixedSizeListArray {
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
            Arc::new(Float32Array::from_iter_values(
                vectors.iter().flat_map(|vector| vector.iter().copied()),
            )),
            None,
        )
        .unwrap()
    }

    fn list_array(values: &[u32], width: i32) -> FixedSizeListArray {
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::UInt32, false)),
            width,
            Arc::new(UInt32Array::from(values.to_vec())),
            None,
        )
        .unwrap()
    }

    fn list_u64_array(values: &[u64], width: i32) -> FixedSizeListArray {
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::UInt64, false)),
            width,
            Arc::new(UInt64Array::from(values.to_vec())),
            None,
        )
        .unwrap()
    }

    fn fixture_primary_page(source_ordinal: u64) -> u32 {
        match source_ordinal {
            0 => 0,
            1 | 2 => 1,
            3 | 4 => 2,
            5..=10 => u32::try_from(source_ordinal - 2).unwrap(),
            _ => u32::try_from(source_ordinal).unwrap(),
        }
    }

    fn write_batch(path: &Path, batch: RecordBatch) {
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn local_object(role: &str, path: &Path) -> V25LocalObjectPath {
        let bytes = fs::read(path).unwrap();
        V25LocalObjectPath {
            identity: V25ObjectIdentity {
                role: role.to_owned(),
                uri: format!("s3://borsuk-v25/{role}"),
                digest_algorithm: "sha256".to_owned(),
                digest: format!("{:x}", Sha256::digest(&bytes)),
                encoded_bytes: bytes.len() as u64,
                generation: "v25-local-test".to_owned(),
            },
            path: path.to_owned(),
        }
    }

    #[test]
    fn v25_containment_local_schemas_are_exact_and_cross_language() {
        let vector = || {
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            )
        };
        let construction = Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("vector", vector(), false),
        ]);
        let pages = Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("primary_page", DataType::UInt32, false),
            Field::new("replica_page", DataType::UInt32, false),
        ]);
        let queries = Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("vector", vector(), false),
        ]);
        let page_list = || {
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::UInt32, false)), 10)
        };
        let truth = Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new(
                "neighbor_source_ordinals",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::UInt64, false)),
                    10,
                ),
                false,
            ),
            Field::new("primary_pages", page_list(), false),
            Field::new("replica_pages", page_list(), false),
            Field::new(
                "oracle_pages",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    8,
                ),
                false,
            ),
        ]);

        assert!(validate_v25_construction_schema(&construction).is_ok());
        assert!(validate_v25_page_assignment_schema(&pages).is_ok());
        assert!(validate_v25_query_schema(&queries).is_ok());
        assert!(validate_v25_truth_schema(&truth).is_ok());

        let wrong_child = Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 96),
                false,
            ),
        ]);
        assert!(validate_v25_construction_schema(&wrong_child).is_err());

        let nullable_replica = Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("primary_page", DataType::UInt32, false),
            Field::new("replica_page", DataType::UInt32, true),
        ]);
        assert!(validate_v25_page_assignment_schema(&nullable_replica).is_err());
    }

    #[test]
    fn v25_containment_local_exact_global_is_order_invariant_and_excludes_own_pages() {
        let query = V25LocalQuery {
            query_ordinal: 0,
            source_ordinal: 0,
            vector: vector(1.0, 0.0),
        };
        let mut rows = (0..20_u64)
            .map(|source_ordinal| V25ConstructionRow {
                source_ordinal,
                vector: vector(20.0 - source_ordinal as f32, source_ordinal as f32 + 1.0),
            })
            .collect::<Vec<_>>();
        let pages = (0..20_u64)
            .map(|source_ordinal| V25RowPages {
                source_ordinal,
                primary_page: fixture_primary_page(source_ordinal),
                replica_page: (source_ordinal == 0).then_some(19),
            })
            .collect::<Vec<_>>();
        let truth = V25QueryTruth {
            query_ordinal: 0,
            neighbor_source_ordinals: (1..=10).collect(),
            ground_truth_page_assignments: vec![
                vec![1],
                vec![1],
                vec![2],
                vec![2],
                vec![3],
                vec![4],
                vec![5],
                vec![6],
                vec![7],
                vec![8],
            ],
            oracle_pages: (1..=8).collect(),
        };

        let expected = evaluate_v25_exact_global(
            &rows,
            &pages,
            std::slice::from_ref(&query),
            std::slice::from_ref(&truth),
            &[10, 32],
            8,
        )
        .unwrap();
        rows.reverse();
        let reversed = evaluate_v25_exact_global(
            &rows,
            &pages,
            std::slice::from_ref(&query),
            std::slice::from_ref(&truth),
            &[10, 32],
            8,
        )
        .unwrap();
        assert_eq!(reversed, expected);
        assert_eq!(expected.len(), 2);
        assert!(expected.iter().all(|sample| {
            sample.selected_pages.len() == 8
                && !sample.selected_pages.contains(&0)
                && !sample.selected_pages.contains(&19)
        }));

        let mut truncated_truth = truth.clone();
        truncated_truth.neighbor_source_ordinals.pop();
        assert!(
            evaluate_v25_exact_global(
                &rows,
                &pages,
                std::slice::from_ref(&query),
                std::slice::from_ref(&truncated_truth),
                &[10, 32],
                8,
            )
            .is_err()
        );

        let mut noncontiguous_rows = rows.clone();
        let mut noncontiguous_pages = pages.clone();
        noncontiguous_rows.last_mut().unwrap().source_ordinal = 20;
        noncontiguous_pages.last_mut().unwrap().source_ordinal = 20;
        assert!(
            evaluate_v25_exact_global(
                &noncontiguous_rows,
                &noncontiguous_pages,
                std::slice::from_ref(&query),
                std::slice::from_ref(&truth),
                &[10, 32],
                8,
            )
            .is_err()
        );
    }

    #[test]
    fn v25_containment_parquet_authenticates_four_roles_and_streams_exact_global() {
        let temporary = tempfile::tempdir().unwrap();
        let construction_path = temporary.path().join("construction.parquet");
        let pages_path = temporary.path().join("pages.parquet");
        let queries_path = temporary.path().join("queries.parquet");
        let truth_path = temporary.path().join("truth.parquet");

        let vectors = (0..257_u64)
            .map(|source_ordinal| {
                vector(257.0 - source_ordinal as f32, source_ordinal as f32 + 1.0)
            })
            .collect::<Vec<_>>();
        let construction_schema = Arc::new(Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                ),
                false,
            ),
        ]));
        write_batch(
            &construction_path,
            RecordBatch::try_new(
                construction_schema,
                vec![
                    Arc::new(UInt64Array::from_iter_values(0..257_u64)) as ArrayRef,
                    Arc::new(vector_array(&vectors)),
                ],
            )
            .unwrap(),
        );

        let pages_schema = Arc::new(Schema::new(vec![
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("primary_page", DataType::UInt32, false),
            Field::new("replica_page", DataType::UInt32, false),
        ]));
        write_batch(
            &pages_path,
            RecordBatch::try_new(
                pages_schema,
                vec![
                    Arc::new(UInt64Array::from_iter_values(0..257_u64)) as ArrayRef,
                    Arc::new(UInt32Array::from_iter_values(
                        (0..257_u64).map(fixture_primary_page),
                    )),
                    Arc::new(UInt32Array::from_iter_values(
                        (0..257_u32).map(|source| if source == 0 { 256 } else { u32::MAX }),
                    )),
                ],
            )
            .unwrap(),
        );

        let query_schema = Arc::new(Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                ),
                false,
            ),
        ]));
        write_batch(
            &queries_path,
            RecordBatch::try_new(
                query_schema,
                vec![
                    Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                    Arc::new(UInt64Array::from(vec![0])),
                    Arc::new(vector_array(&[vector(1.0, 0.0)])),
                ],
            )
            .unwrap(),
        );

        let truth_schema = Arc::new(Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new(
                "neighbor_source_ordinals",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::UInt64, false)),
                    10,
                ),
                false,
            ),
            Field::new(
                "primary_pages",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    10,
                ),
                false,
            ),
            Field::new(
                "replica_pages",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    10,
                ),
                false,
            ),
            Field::new(
                "oracle_pages",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::UInt32, false)),
                    8,
                ),
                false,
            ),
        ]));
        write_batch(
            &truth_path,
            RecordBatch::try_new(
                truth_schema,
                vec![
                    Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                    Arc::new(list_u64_array(&(1..=10).collect::<Vec<_>>(), 10)),
                    Arc::new(list_array(&[1, 1, 2, 2, 3, 4, 5, 6, 7, 8], 10)),
                    Arc::new(list_array(&[u32::MAX; 10], 10)),
                    Arc::new(list_array(&(1..=8).collect::<Vec<_>>(), 8)),
                ],
            )
            .unwrap(),
        );

        let request = V25ContainmentLocalRequest {
            construction_rows: local_object("construction-rows-parquet", &construction_path),
            page_assignments: local_object("page-assignments-parquet", &pages_path),
            pseudoqueries: local_object("pseudoqueries-parquet", &queries_path),
            truth: local_object("truth-parquet", &truth_path),
            ranked_row_limits: vec![10, 32],
            page_budget: 8,
            expected_source_rows: 257,
            expected_page_count: 257,
            expected_queries: 1,
            construction_batch_rows: 64,
        };
        let output = run_v25_containment_local_request(&request).unwrap();
        assert_eq!(output.scanned_rows, 257);
        assert_eq!(output.samples.len(), 2);
        assert_eq!(output.peak_construction_batch_rows, 64);
        assert!(output.peak_ranked_rows_retained <= 32);
        assert!(output.samples.iter().all(|sample| {
            sample.candidate_rows == 255
                && sample.selected_pages == (1..=8).collect::<Vec<_>>()
                && sample.hits == 10
                && sample.oracle_hits == 10
                && sample.recall_ppm == 1_000_000
                && sample.oracle_attainment_ppm == 1_000_000
        }));
        assert_eq!(output.page_body_reads, 0);

        let evidence_path = temporary.path().join("evidence.parquet");
        let evidence_copy_path = temporary.path().join("evidence-copy.parquet");
        let evidence = write_v25_containment_evidence(
            &evidence_path,
            "s3://borsuk-v25/containment/evidence.parquet",
            "v25-local-test",
            request.page_budget,
            &output.samples,
        )
        .unwrap();
        assert_eq!(evidence.identity.role, "containment-evidence-parquet");
        read_v25_containment_evidence(&evidence, request.page_budget, &output.samples).unwrap();
        let evidence_copy = write_v25_containment_evidence(
            &evidence_copy_path,
            "s3://borsuk-v25/containment/evidence-copy.parquet",
            "v25-local-test",
            request.page_budget,
            &output.samples,
        )
        .unwrap();
        assert_eq!(
            fs::read(&evidence.path).unwrap(),
            fs::read(&evidence_copy.path).unwrap()
        );

        let mut changed_samples = output.samples.clone();
        changed_samples[0].hits = 9;
        assert!(
            read_v25_containment_evidence(&evidence, request.page_budget, &changed_samples)
                .is_err()
        );

        fs::write(&construction_path, b"changed").unwrap();
        assert!(run_v25_containment_local_request(&request).is_err());
    }
}
