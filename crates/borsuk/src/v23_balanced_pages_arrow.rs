use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float16Array, Float32Array, RecordBatch, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use parquet::arrow::{
    ArrowWriter,
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v23_balanced_pages::{V23BalancedIdentity, validate_v23_balanced_identity},
};

const DIMENSIONS: i32 = 96;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23SupercellRow {
    pub(crate) supercell_ordinal: u32,
    pub(crate) centroid: [f16; 96],
    pub(crate) cosine_radius: f32,
    pub(crate) primary_rows: u64,
    pub(crate) first_page: u32,
    pub(crate) page_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V23PageRow {
    pub(crate) page_ordinal: u32,
    pub(crate) supercell_ordinal: u32,
    pub(crate) primary_rows: u16,
    pub(crate) replica_rows: u16,
    pub(crate) centroid: [f16; 96],
    pub(crate) cosine_radius: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V23RowPage {
    pub(crate) source_ordinal: u64,
    pub(crate) primary_page: u32,
    pub(crate) replica_page: u32,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("V23 balanced page Parquet {message}"))
}

fn centroid_field() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("element", DataType::Float16, false)),
        DIMENSIONS,
    )
}

fn supercell_schema() -> Schema {
    Schema::new(vec![
        Field::new("supercell_ordinal", DataType::UInt32, false),
        Field::new("centroid", centroid_field(), false),
        Field::new("cosine_radius", DataType::Float32, false),
        Field::new("primary_rows", DataType::UInt64, false),
        Field::new("first_page", DataType::UInt32, false),
        Field::new("page_count", DataType::UInt32, false),
    ])
}

fn page_schema() -> Schema {
    Schema::new(vec![
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("supercell_ordinal", DataType::UInt32, false),
        Field::new("primary_rows", DataType::UInt16, false),
        Field::new("replica_rows", DataType::UInt16, false),
        Field::new("centroid", centroid_field(), false),
        Field::new("cosine_radius", DataType::Float32, false),
    ])
}

pub(crate) fn v23_row_page_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new("primary_page", DataType::UInt32, false),
        Field::new("replica_page", DataType::UInt32, false),
    ])
}

fn centroid_array<'a>(rows: impl Iterator<Item = &'a [f16; 96]>) -> Result<ArrayRef> {
    let values = Arc::new(Float16Array::from_iter_values(rows.flatten().copied()));
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float16, false)),
        DIMENSIONS,
        values,
        None,
    )?))
}

fn write_batch(path: &Path, batch: RecordBatch) -> Result<()> {
    let file =
        File::create(path).map_err(|error| invalid(&format!("output create failed: {error}")))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn authenticate_path(path: &Path, identity: &V23BalancedIdentity, role: &str) -> Result<()> {
    validate_v23_balanced_identity(identity)?;
    let file =
        File::open(path).map_err(|error| invalid(&format!("authority open failed: {error}")))?;
    let encoded_bytes = file
        .metadata()
        .map_err(|error| invalid(&format!("authority metadata failed: {error}")))?
        .len();
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| invalid(&format!("authority read failed: {error}")))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    if identity.role != role
        || identity.encoded_bytes != encoded_bytes
        || identity.digest != format!("{:x}", digest.finalize())
    {
        return Err(invalid("byte authority differs"));
    }
    Ok(())
}

fn read_batches(path: &Path, schema: &Schema) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).map_err(|error| invalid(&format!("input open failed: {error}")))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if builder.schema().as_ref() != schema {
        return Err(invalid("physical schema differs"));
    }
    let mut batches = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_rows() == 0
            || batch.num_columns() != schema.fields().len()
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("batch shape differs"));
        }
        batches.push(batch);
    }
    if batches.is_empty() {
        return Err(invalid("artifact is empty"));
    }
    Ok(batches)
}

fn decode_centroid(array: &ArrayRef, row: usize) -> Result<[f16; 96]> {
    let lists = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("centroid column differs"))?;
    let values = lists
        .values()
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| invalid("centroid child differs"))?;
    let start = row * 96;
    let centroid: [f16; 96] = values.values()[start..start + 96]
        .try_into()
        .map_err(|_| invalid("centroid width differs"))?;
    if centroid.iter().any(|value| !value.is_finite()) {
        return Err(invalid("centroid value differs"));
    }
    Ok(centroid)
}

fn validate_supercells(rows: &[V23SupercellRow]) -> Result<()> {
    let mut next_page = 0u32;
    for (ordinal, row) in rows.iter().enumerate() {
        if row.supercell_ordinal != ordinal as u32
            || row.primary_rows == 0
            || row.first_page != next_page
            || row.page_count == 0
            || !row.cosine_radius.is_finite()
            || row.cosine_radius < 0.0
            || row.centroid.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("supercell row differs"));
        }
        next_page = next_page
            .checked_add(row.page_count)
            .ok_or_else(|| invalid("page range overflows"))?;
    }
    if rows.is_empty() {
        return Err(invalid("supercells are empty"));
    }
    Ok(())
}

fn validate_pages(rows: &[V23PageRow], maximum_replica_rows: u16) -> Result<()> {
    if rows.is_empty() || maximum_replica_rows > 192 {
        return Err(invalid("pages are empty"));
    }
    for (ordinal, row) in rows.iter().enumerate() {
        if row.page_ordinal != ordinal as u32
            || row.primary_rows == 0
            || row.primary_rows > 384
            || row.replica_rows > maximum_replica_rows
            || u32::from(row.primary_rows) + u32::from(row.replica_rows)
                > 384 + u32::from(maximum_replica_rows)
            || !row.cosine_radius.is_finite()
            || row.cosine_radius < 0.0
            || row.centroid.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("page row differs"));
        }
    }
    Ok(())
}

fn validate_row_pages(rows: &[V23RowPage], page_count: u32) -> Result<()> {
    if rows.is_empty() || page_count == 0 || page_count == u32::MAX {
        return Err(invalid("row assignments are empty"));
    }
    for pair in rows.windows(2) {
        if pair[0].source_ordinal >= pair[1].source_ordinal {
            return Err(invalid("source ordinal order differs"));
        }
    }
    if rows.iter().any(|row| {
        row.primary_page >= page_count
            || (row.replica_page != u32::MAX && row.replica_page >= page_count)
            || (row.replica_page != u32::MAX && row.replica_page == row.primary_page)
    }) {
        return Err(invalid("row assignment differs"));
    }
    Ok(())
}

pub(crate) fn write_v23_supercells(path: &Path, rows: &[V23SupercellRow]) -> Result<()> {
    validate_supercells(rows)?;
    write_batch(
        path,
        RecordBatch::try_new(
            Arc::new(supercell_schema()),
            vec![
                Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|r| r.supercell_ordinal),
                )),
                centroid_array(rows.iter().map(|r| &r.centroid))?,
                Arc::new(Float32Array::from_iter_values(
                    rows.iter().map(|r| r.cosine_radius),
                )),
                Arc::new(UInt64Array::from_iter_values(
                    rows.iter().map(|r| r.primary_rows),
                )),
                Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|r| r.first_page),
                )),
                Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|r| r.page_count),
                )),
            ],
        )?,
    )
}

pub(crate) fn write_v23_pages(
    path: &Path,
    rows: &[V23PageRow],
    maximum_replica_rows: u16,
) -> Result<()> {
    validate_pages(rows, maximum_replica_rows)?;
    write_batch(
        path,
        RecordBatch::try_new(
            Arc::new(page_schema()),
            vec![
                Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|r| r.page_ordinal),
                )),
                Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|r| r.supercell_ordinal),
                )),
                Arc::new(UInt16Array::from_iter_values(
                    rows.iter().map(|r| r.primary_rows),
                )),
                Arc::new(UInt16Array::from_iter_values(
                    rows.iter().map(|r| r.replica_rows),
                )),
                centroid_array(rows.iter().map(|r| &r.centroid))?,
                Arc::new(Float32Array::from_iter_values(
                    rows.iter().map(|r| r.cosine_radius),
                )),
            ],
        )?,
    )
}

pub(crate) fn write_v23_row_pages(path: &Path, rows: &[V23RowPage], page_count: u32) -> Result<()> {
    validate_row_pages(rows, page_count)?;
    write_batch(
        path,
        RecordBatch::try_new(
            Arc::new(v23_row_page_schema()),
            vec![
                Arc::new(UInt64Array::from_iter_values(
                    rows.iter().map(|r| r.source_ordinal),
                )),
                Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|r| r.primary_page),
                )),
                Arc::new(UInt32Array::from_iter_values(
                    rows.iter().map(|r| r.replica_page),
                )),
            ],
        )?,
    )
}

pub(crate) fn read_v23_supercells(
    path: &Path,
    identity: &V23BalancedIdentity,
) -> Result<Vec<V23SupercellRow>> {
    authenticate_path(path, identity, "supercells-parquet")?;
    let mut rows = Vec::new();
    for batch in read_batches(path, &supercell_schema())? {
        let ordinal = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("supercell ordinal differs"))?;
        let radius = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("supercell radius differs"))?;
        let primary = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("supercell population differs"))?;
        let first = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("first page differs"))?;
        let count = batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("page count differs"))?;
        for row in 0..batch.num_rows() {
            rows.push(V23SupercellRow {
                supercell_ordinal: ordinal.value(row),
                centroid: decode_centroid(batch.column(1), row)?,
                cosine_radius: radius.value(row),
                primary_rows: primary.value(row),
                first_page: first.value(row),
                page_count: count.value(row),
            });
        }
    }
    validate_supercells(&rows)?;
    Ok(rows)
}

pub(crate) fn read_v23_pages(
    path: &Path,
    identity: &V23BalancedIdentity,
    expected_role: &str,
    maximum_replica_rows: u16,
) -> Result<Vec<V23PageRow>> {
    authenticate_path(path, identity, expected_role)?;
    let mut rows = Vec::new();
    for batch in read_batches(path, &page_schema())? {
        let ordinal = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("page ordinal differs"))?;
        let supercell = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("page supercell differs"))?;
        let primary = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("page primary count differs"))?;
        let replica = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid("page replica count differs"))?;
        let radius = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("page radius differs"))?;
        for row in 0..batch.num_rows() {
            rows.push(V23PageRow {
                page_ordinal: ordinal.value(row),
                supercell_ordinal: supercell.value(row),
                primary_rows: primary.value(row),
                replica_rows: replica.value(row),
                centroid: decode_centroid(batch.column(4), row)?,
                cosine_radius: radius.value(row),
            });
        }
    }
    validate_pages(&rows, maximum_replica_rows)?;
    Ok(rows)
}

pub(crate) fn read_v23_row_pages(
    path: &Path,
    identity: &V23BalancedIdentity,
    expected_role: &str,
    page_count: u32,
) -> Result<Vec<V23RowPage>> {
    let rows = open_v23_row_pages(path, identity, expected_role, page_count)?
        .collect::<Result<Vec<_>>>()?;
    validate_row_pages(&rows, page_count)?;
    Ok(rows)
}

pub(crate) struct V23RowPageStream {
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,
    next_source_ordinal: u64,
    page_count: u32,
    failed: bool,
}

impl V23RowPageStream {
    fn read_batch(&mut self) -> Result<bool> {
        self.batch = self.reader.next().transpose()?;
        self.row = 0;
        let Some(batch) = &self.batch else {
            return Ok(false);
        };
        if batch.num_rows() == 0
            || batch.num_columns() != 3
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(invalid("row assignment batch shape differs"));
        }
        Ok(true)
    }
}

impl Iterator for V23RowPageStream {
    type Item = Result<V23RowPage>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            if self.batch.is_none() || self.row == self.batch.as_ref().unwrap().num_rows() {
                match self.read_batch() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(error) => {
                        self.failed = true;
                        return Some(Err(error));
                    }
                }
            }
            let batch = self.batch.as_ref().unwrap();
            let decoded = (|| {
                let source = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| invalid("source ordinal differs"))?
                    .value(self.row);
                let primary = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| invalid("primary page differs"))?
                    .value(self.row);
                let replica = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| invalid("replica page differs"))?
                    .value(self.row);
                if source != self.next_source_ordinal
                    || primary >= self.page_count
                    || (replica != u32::MAX && replica >= self.page_count)
                    || replica == primary
                {
                    return Err(invalid("row assignment stream differs"));
                }
                Ok(V23RowPage {
                    source_ordinal: source,
                    primary_page: primary,
                    replica_page: replica,
                })
            })();
            self.row += 1;
            match decoded {
                Ok(row) => {
                    self.next_source_ordinal += 1;
                    return Some(Ok(row));
                }
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

pub(crate) fn open_v23_row_pages(
    path: &Path,
    identity: &V23BalancedIdentity,
    expected_role: &str,
    page_count: u32,
) -> Result<V23RowPageStream> {
    authenticate_path(path, identity, expected_role)?;
    if page_count == 0 || page_count == u32::MAX {
        return Err(invalid("row assignment page count differs"));
    }
    let file = File::open(path).map_err(|error| invalid(&format!("input open failed: {error}")))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if builder.schema().as_ref() != &v23_row_page_schema() {
        return Err(invalid("physical schema differs"));
    }
    let mut stream = V23RowPageStream {
        reader: builder.build()?,
        batch: None,
        row: 0,
        next_source_ordinal: 0,
        page_count,
        failed: false,
    };
    if !stream.read_batch()? {
        return Err(invalid("artifact is empty"));
    }
    Ok(stream)
}

pub(crate) fn reconcile_v23_balanced_arm(
    supercells: &[V23SupercellRow],
    pages: &[V23PageRow],
    assignments: &[V23RowPage],
    maximum_replica_rows: u16,
) -> Result<()> {
    validate_supercells(supercells)?;
    validate_pages(pages, maximum_replica_rows)?;
    let page_count = u32::try_from(pages.len()).map_err(|_| invalid("page count overflows"))?;
    validate_row_pages(assignments, page_count)?;
    let mut primary_counts = vec![0_u64; pages.len()];
    let mut replica_counts = vec![0_u64; pages.len()];
    for row in assignments {
        primary_counts[usize::try_from(row.primary_page).unwrap()] += 1;
        if row.replica_page != u32::MAX {
            replica_counts[usize::try_from(row.replica_page).unwrap()] += 1;
        }
    }
    for (ordinal, page) in pages.iter().enumerate() {
        let supercell = supercells
            .get(usize::try_from(page.supercell_ordinal).unwrap())
            .ok_or_else(|| invalid("page supercell is out of range"))?;
        let page_ordinal = u32::try_from(ordinal).unwrap();
        let end = supercell
            .first_page
            .checked_add(supercell.page_count)
            .ok_or_else(|| invalid("supercell page range overflows"))?;
        if page_ordinal < supercell.first_page
            || page_ordinal >= end
            || primary_counts[ordinal] != u64::from(page.primary_rows)
            || replica_counts[ordinal] != u64::from(page.replica_rows)
        {
            return Err(invalid("page assignment reconciliation differs"));
        }
    }
    for supercell in supercells {
        let start = usize::try_from(supercell.first_page).unwrap();
        let end = usize::try_from(
            supercell
                .first_page
                .checked_add(supercell.page_count)
                .ok_or_else(|| invalid("supercell page range overflows"))?,
        )
        .unwrap();
        let primary_rows = pages
            .get(start..end)
            .ok_or_else(|| invalid("supercell page range is out of bounds"))?
            .iter()
            .map(|page| u64::from(page.primary_rows))
            .sum::<u64>();
        if primary_rows != supercell.primary_rows {
            return Err(invalid("supercell population reconciliation differs"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use half::f16;
    use sha2::{Digest, Sha256};

    use super::{
        V23PageRow, V23RowPage, V23SupercellRow, open_v23_row_pages, read_v23_pages,
        read_v23_row_pages, read_v23_supercells, reconcile_v23_balanced_arm, write_v23_pages,
        write_v23_row_pages, write_v23_supercells,
    };
    use crate::v23_balanced_pages::V23BalancedIdentity;

    fn centroid(value: f32) -> [f16; 96] {
        [f16::from_f32(value); 96]
    }

    fn identity(path: &Path, role: &str) -> V23BalancedIdentity {
        let bytes = fs::read(path).unwrap();
        V23BalancedIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v23-eu-west-1/frozen/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: format!("{:x}", Sha256::digest(&bytes)),
            encoded_bytes: bytes.len() as u64,
        }
    }

    #[test]
    fn v23_balanced_arrow_round_trips_exact_bulk_roles() {
        let directory = tempfile::tempdir().unwrap();
        let supercell_path = directory.path().join("supercells.parquet");
        let page_path = directory.path().join("pages.parquet");
        let row_page_path = directory.path().join("row-pages.parquet");
        let supercells = vec![V23SupercellRow {
            supercell_ordinal: 0,
            centroid: centroid(0.125),
            cosine_radius: 0.75,
            primary_rows: 384,
            first_page: 0,
            page_count: 1,
        }];
        let pages = vec![V23PageRow {
            page_ordinal: 0,
            supercell_ordinal: 0,
            primary_rows: 384,
            replica_rows: 48,
            centroid: centroid(0.25),
            cosine_radius: 0.5,
        }];
        let row_pages = vec![
            V23RowPage {
                source_ordinal: 0,
                primary_page: 0,
                replica_page: u32::MAX,
            },
            V23RowPage {
                source_ordinal: 1,
                primary_page: 0,
                replica_page: u32::MAX,
            },
        ];

        write_v23_supercells(&supercell_path, &supercells).unwrap();
        write_v23_pages(&page_path, &pages, 48).unwrap();
        write_v23_row_pages(&row_page_path, &row_pages, 1).unwrap();

        assert_eq!(
            read_v23_supercells(
                &supercell_path,
                &identity(&supercell_path, "supercells-parquet")
            )
            .unwrap(),
            supercells
        );
        assert_eq!(
            read_v23_pages(
                &page_path,
                &identity(&page_path, "pages-amp-1125-parquet"),
                "pages-amp-1125-parquet",
                48,
            )
            .unwrap(),
            pages
        );
        assert_eq!(
            read_v23_row_pages(
                &row_page_path,
                &identity(&row_page_path, "row-pages-amp-1125-parquet"),
                "row-pages-amp-1125-parquet",
                1,
            )
            .unwrap(),
            row_pages
        );
        assert_eq!(
            open_v23_row_pages(
                &row_page_path,
                &identity(&row_page_path, "row-pages-amp-1125-parquet"),
                "row-pages-amp-1125-parquet",
                1,
            )
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
            row_pages
        );
    }

    #[test]
    fn v23_balanced_arrow_rejects_digest_order_value_and_assignment_drift() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("supercells.parquet");
        let rows = vec![
            V23SupercellRow {
                supercell_ordinal: 0,
                centroid: centroid(0.1),
                cosine_radius: 0.8,
                primary_rows: 384,
                first_page: 0,
                page_count: 1,
            },
            V23SupercellRow {
                supercell_ordinal: 1,
                centroid: centroid(0.2),
                cosine_radius: 0.7,
                primary_rows: 384,
                first_page: 1,
                page_count: 1,
            },
        ];
        write_v23_supercells(&path, &rows).unwrap();
        let mut changed_identity = identity(&path, "supercells-parquet");
        changed_identity.digest.replace_range(..2, "ff");
        assert!(read_v23_supercells(&path, &changed_identity).is_err());

        let mut invalid_radius = rows.clone();
        invalid_radius[0].cosine_radius = f32::NAN;
        assert!(
            write_v23_supercells(
                &directory.path().join("nan-supercell.parquet"),
                &invalid_radius,
            )
            .is_err()
        );

        let mut reversed = rows;
        reversed.reverse();
        assert!(
            write_v23_supercells(&directory.path().join("reversed.parquet"), &reversed).is_err()
        );
        let invalid_page = V23PageRow {
            page_ordinal: 0,
            supercell_ordinal: 0,
            primary_rows: 384,
            replica_rows: 0,
            centroid: centroid(0.2),
            cosine_radius: f32::NAN,
        };
        assert!(
            write_v23_pages(&directory.path().join("nan.parquet"), &[invalid_page], 48).is_err()
        );
        let invalid_assignment = V23RowPage {
            source_ordinal: 0,
            primary_page: 7,
            replica_page: 7,
        };
        assert!(
            write_v23_row_pages(
                &directory.path().join("same-page.parquet"),
                &[invalid_assignment],
                8,
            )
            .is_err()
        );
    }

    #[test]
    fn v23_balanced_arrow_reconciles_arm_counts_ranges_and_assignments() {
        let supercells = vec![V23SupercellRow {
            supercell_ordinal: 0,
            centroid: centroid(0.125),
            cosine_radius: 0.75,
            primary_rows: 2,
            first_page: 0,
            page_count: 1,
        }];
        let pages = vec![V23PageRow {
            page_ordinal: 0,
            supercell_ordinal: 0,
            primary_rows: 2,
            replica_rows: 0,
            centroid: centroid(0.25),
            cosine_radius: 0.5,
        }];
        let assignments = vec![
            V23RowPage {
                source_ordinal: 0,
                primary_page: 0,
                replica_page: u32::MAX,
            },
            V23RowPage {
                source_ordinal: 1,
                primary_page: 0,
                replica_page: u32::MAX,
            },
        ];
        reconcile_v23_balanced_arm(&supercells, &pages, &assignments, 48).unwrap();

        let mut bad_assignment = assignments.clone();
        bad_assignment[1].primary_page = 1;
        assert!(reconcile_v23_balanced_arm(&supercells, &pages, &bad_assignment, 48).is_err());
        let mut bad_count = pages.clone();
        bad_count[0].primary_rows = 1;
        assert!(reconcile_v23_balanced_arm(&supercells, &bad_count, &assignments, 48).is_err());
        let mut bad_range = supercells.clone();
        bad_range[0].page_count = 2;
        assert!(reconcile_v23_balanced_arm(&bad_range, &pages, &assignments, 48).is_err());
    }
}
