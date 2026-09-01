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
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
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

fn row_page_schema() -> Schema {
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

fn validate_pages(rows: &[V23PageRow]) -> Result<()> {
    if rows.is_empty() {
        return Err(invalid("pages are empty"));
    }
    for (ordinal, row) in rows.iter().enumerate() {
        if row.page_ordinal != ordinal as u32
            || row.primary_rows == 0
            || row.primary_rows > 384
            || row.replica_rows > 192
            || u32::from(row.primary_rows) + u32::from(row.replica_rows) > 576
            || !row.cosine_radius.is_finite()
            || row.cosine_radius < 0.0
            || row.centroid.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("page row differs"));
        }
    }
    Ok(())
}

fn validate_row_pages(rows: &[V23RowPage]) -> Result<()> {
    if rows.is_empty() {
        return Err(invalid("row assignments are empty"));
    }
    for pair in rows.windows(2) {
        if pair[0].source_ordinal >= pair[1].source_ordinal {
            return Err(invalid("source ordinal order differs"));
        }
    }
    if rows.iter().any(|row| {
        row.primary_page == u32::MAX
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

pub(crate) fn write_v23_pages(path: &Path, rows: &[V23PageRow]) -> Result<()> {
    validate_pages(rows)?;
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

pub(crate) fn write_v23_row_pages(path: &Path, rows: &[V23RowPage]) -> Result<()> {
    validate_row_pages(rows)?;
    write_batch(
        path,
        RecordBatch::try_new(
            Arc::new(row_page_schema()),
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
        let primary = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("supercell population differs"))?;
        let first = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("first page differs"))?;
        let count = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("page count differs"))?;
        for row in 0..batch.num_rows() {
            rows.push(V23SupercellRow {
                supercell_ordinal: ordinal.value(row),
                centroid: decode_centroid(batch.column(1), row)?,
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
) -> Result<Vec<V23PageRow>> {
    authenticate_path(path, identity, "pages-parquet")?;
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
    validate_pages(&rows)?;
    Ok(rows)
}

pub(crate) fn read_v23_row_pages(
    path: &Path,
    identity: &V23BalancedIdentity,
) -> Result<Vec<V23RowPage>> {
    authenticate_path(path, identity, "row-pages-parquet")?;
    let mut rows = Vec::new();
    for batch in read_batches(path, &row_page_schema())? {
        let source = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("source ordinal differs"))?;
        let primary = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("primary page differs"))?;
        let replica = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid("replica page differs"))?;
        for row in 0..batch.num_rows() {
            rows.push(V23RowPage {
                source_ordinal: source.value(row),
                primary_page: primary.value(row),
                replica_page: replica.value(row),
            });
        }
    }
    validate_row_pages(&rows)?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use half::f16;
    use sha2::{Digest, Sha256};

    use super::{
        V23PageRow, V23RowPage, V23SupercellRow, read_v23_pages, read_v23_row_pages,
        read_v23_supercells, write_v23_pages, write_v23_row_pages, write_v23_supercells,
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
        write_v23_pages(&page_path, &pages).unwrap();
        write_v23_row_pages(&row_page_path, &row_pages).unwrap();

        assert_eq!(
            read_v23_supercells(
                &supercell_path,
                &identity(&supercell_path, "supercells-parquet")
            )
            .unwrap(),
            supercells
        );
        assert_eq!(
            read_v23_pages(&page_path, &identity(&page_path, "pages-parquet")).unwrap(),
            pages
        );
        assert_eq!(
            read_v23_row_pages(
                &row_page_path,
                &identity(&row_page_path, "row-pages-parquet")
            )
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
                primary_rows: 384,
                first_page: 0,
                page_count: 1,
            },
            V23SupercellRow {
                supercell_ordinal: 1,
                centroid: centroid(0.2),
                primary_rows: 384,
                first_page: 1,
                page_count: 1,
            },
        ];
        write_v23_supercells(&path, &rows).unwrap();
        let mut changed_identity = identity(&path, "supercells-parquet");
        changed_identity.digest.replace_range(..2, "ff");
        assert!(read_v23_supercells(&path, &changed_identity).is_err());

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
        assert!(write_v23_pages(&directory.path().join("nan.parquet"), &[invalid_page]).is_err());
        let invalid_assignment = V23RowPage {
            source_ordinal: 0,
            primary_page: 7,
            replica_page: 7,
        };
        assert!(
            write_v23_row_pages(
                &directory.path().join("same-page.parquet"),
                &[invalid_assignment]
            )
            .is_err()
        );
    }
}
