//! Bounded Parquet-to-V27 page builder for reduced and production qualification.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    BorsukError, V27BuildConfig, V27HierarchyConfig, V27PageBuilder, V27PageIdentity, V27PageRow,
    V27PageSink, encode_v27_hierarchy, encode_v27_layout, encode_v27_page_manifest,
    fit_v27_hierarchy,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};

#[derive(Debug)]
struct Args {
    train: PathBuf,
    train_sha256: String,
    train_bytes: u64,
    row_limit: usize,
    roots: usize,
    leaves: usize,
    iterations: usize,
    workers: usize,
    page_rows: usize,
    output: PathBuf,
}

fn argument_error(message: &str) -> String {
    format!("V27 build arguments {message}")
}

fn take(map: &mut BTreeMap<String, String>, name: &str) -> Result<String, String> {
    map.remove(name)
        .ok_or_else(|| argument_error(&format!("missing --{name}")))
}

fn number<T: std::str::FromStr>(
    map: &mut BTreeMap<String, String>,
    name: &str,
) -> Result<T, String> {
    take(map, name)?
        .parse()
        .map_err(|_| argument_error(&format!("--{name} type differs")))
}

fn parse_args(values: Vec<String>) -> Result<Args, String> {
    let mut values = values.into_iter();
    values
        .next()
        .ok_or_else(|| argument_error("program is missing"))?;
    let mut execute = false;
    let mut map = BTreeMap::new();
    while let Some(flag) = values.next() {
        if flag == "--execute" {
            if execute {
                return Err(argument_error("duplicate --execute"));
            }
            execute = true;
            continue;
        }
        let name = flag
            .strip_prefix("--")
            .ok_or_else(|| argument_error("flag syntax differs"))?;
        let value = values
            .next()
            .ok_or_else(|| argument_error(&format!("--{name} value is missing")))?;
        if map.insert(name.to_owned(), value).is_some() {
            return Err(argument_error(&format!("duplicate --{name}")));
        }
    }
    if !execute {
        return Err(argument_error("--execute is required"));
    }
    let args = Args {
        train: PathBuf::from(take(&mut map, "train-parquet")?),
        train_sha256: take(&mut map, "train-sha256")?,
        train_bytes: number(&mut map, "train-bytes")?,
        row_limit: number(&mut map, "row-limit")?,
        roots: number(&mut map, "roots")?,
        leaves: number(&mut map, "leaves")?,
        iterations: number(&mut map, "iterations")?,
        workers: number(&mut map, "workers")?,
        page_rows: number(&mut map, "page-rows")?,
        output: PathBuf::from(take(&mut map, "output-dir")?),
    };
    if !map.is_empty()
        || args.train.as_os_str().is_empty()
        || args.output.as_os_str().is_empty()
        || args.train_sha256.len() != 64
        || !args
            .train_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || args.train_bytes == 0
        || args.row_limit == 0
        || args.roots == 0
        || args.leaves < args.roots
        || args.iterations == 0
        || args.workers == 0
        || args.page_rows == 0
        || args.page_rows > 1_024
    {
        return Err(argument_error("authority or numeric bound differs"));
    }
    Ok(args)
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn io_error(path: &Path, source: std::io::Error) -> BorsukError {
    BorsukError::Io {
        path: path.to_owned(),
        source,
    }
}

fn authenticate_file(
    path: &Path,
    expected_bytes: u64,
    expected_sha256: &str,
) -> borsuk::Result<()> {
    let metadata = fs::metadata(path).map_err(|source| io_error(path, source))?;
    let mut file = File::open(path).map_err(|source| io_error(path, source))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|source| io_error(path, source))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if metadata.len() != expected_bytes || format!("{:x}", digest.finalize()) != expected_sha256 {
        return Err(invalid("V27 build training byte authority differs"));
    }
    Ok(())
}

fn train_schema() -> Schema {
    let child = std::sync::Arc::new(Field::new("element", DataType::Float32, false));
    Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(child, 96),
        false,
    )])
}

fn read_rows(args: &Args) -> borsuk::Result<Vec<V27PageRow>> {
    authenticate_file(&args.train, args.train_bytes, &args.train_sha256)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        File::open(&args.train).map_err(|source| io_error(&args.train, source))?,
    )?;
    if builder.schema().as_ref() != &train_schema() {
        return Err(invalid("V27 build training Parquet schema differs"));
    }
    let mut rows = Vec::with_capacity(args.row_limit);
    for batch in builder.with_batch_size(8_192).build()? {
        let batch = batch?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V27 build training nullability differs"));
        }
        let vectors = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V27 build training vector type differs"))?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V27 build training value type differs"))?;
        for row in 0..batch.num_rows() {
            if rows.len() == args.row_limit {
                return Ok(rows);
            }
            let start = row * 96;
            let vector: [f32; 96] = values.values()[start..start + 96]
                .try_into()
                .map_err(|_| invalid("V27 build training dimension differs"))?;
            if vector.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V27 build training value differs"));
            }
            rows.push(V27PageRow {
                source_ordinal: rows.len() as u64,
                vector,
            });
        }
    }
    if rows.len() != args.row_limit {
        return Err(invalid("V27 build training row limit exceeds artifact"));
    }
    Ok(rows)
}

struct FileSink {
    scratch: PathBuf,
    pages: PathBuf,
}

impl FileSink {
    fn scratch_path(&self, key: &str) -> PathBuf {
        self.scratch.join(key)
    }
}

impl V27PageSink for FileSink {
    fn write_scratch(&mut self, key: &str, bytes: &[u8]) -> borsuk::Result<()> {
        let path = self.scratch_path(key);
        fs::write(&path, bytes).map_err(|source| io_error(&path, source))
    }

    fn write_scratch_stream(
        &mut self,
        key: &str,
        write: &mut dyn FnMut(&mut dyn Write) -> borsuk::Result<()>,
    ) -> borsuk::Result<()> {
        let path = self.scratch_path(key);
        let mut file = File::create(&path).map_err(|source| io_error(&path, source))?;
        write(&mut file)?;
        file.flush().map_err(|source| io_error(&path, source))
    }

    fn open_scratch(&self, key: &str) -> borsuk::Result<Box<dyn Read + Send>> {
        let path = self.scratch_path(key);
        Ok(Box::new(
            File::open(&path).map_err(|source| io_error(&path, source))?,
        ))
    }

    fn remove_scratch(&mut self, key: &str) -> borsuk::Result<()> {
        let path = self.scratch_path(key);
        fs::remove_file(&path).map_err(|source| io_error(&path, source))
    }

    fn write_page(&mut self, identity: &V27PageIdentity, bytes: &[u8]) -> borsuk::Result<()> {
        let path = self.pages.join(format!("{}.arrow", identity.sha256));
        fs::write(&path, bytes).map_err(|source| io_error(&path, source))
    }
}

fn write_file(path: &Path, bytes: &[u8]) -> borsuk::Result<()> {
    fs::write(path, bytes).map_err(|source| io_error(path, source))
}

fn execute(args: Args) -> borsuk::Result<Vec<u8>> {
    if args.output.exists() {
        return Err(invalid("V27 build output already exists"));
    }
    let rows = read_rows(&args)?;
    let hierarchy = fit_v27_hierarchy(
        &rows,
        &V27HierarchyConfig {
            roots: args.roots,
            leaves: args.leaves,
            iterations: args.iterations,
            seed: 0x6a09_e667_f3bc_c909,
            worker_count: args.workers,
            batch_rows: 8_192,
        },
    )?;
    fs::create_dir(&args.output).map_err(|source| io_error(&args.output, source))?;
    let scratch = args.output.join(".scratch");
    let pages = args.output.join("pages");
    fs::create_dir(&scratch).map_err(|source| io_error(&scratch, source))?;
    fs::create_dir(&pages).map_err(|source| io_error(&pages, source))?;
    let mut sink = FileSink { scratch, pages };
    let receipt = V27PageBuilder::build(
        rows,
        &hierarchy,
        &V27BuildConfig {
            page_rows: args.page_rows,
            replica_margin_ppm: 50_000,
            replica_ceiling_ppm: 150_000,
            sort_memory_bytes: 8 * 1024 * 1024,
        },
        &mut sink,
    )?;
    fs::remove_dir(&sink.scratch).map_err(|source| io_error(&sink.scratch, source))?;
    let hierarchy_artifacts = encode_v27_hierarchy(&hierarchy)?;
    let layout = encode_v27_layout(&receipt)?;
    let (manifest, manifest_bytes) = encode_v27_page_manifest(&receipt)?;
    write_file(
        &args.output.join("roots.arrow"),
        &hierarchy_artifacts.roots_bytes,
    )?;
    write_file(
        &args.output.join("leaves.arrow"),
        &hierarchy_artifacts.leaves_bytes,
    )?;
    write_file(
        &args.output.join("postings.parquet"),
        &layout.postings_parquet,
    )?;
    write_file(&args.output.join("modes.arrow"), &layout.modes_arrow)?;
    write_file(&args.output.join("pages.json"), &manifest_bytes)?;
    let value = serde_json::json!({
        "artifacts": {
            "leaves": {
                "encoded_bytes": hierarchy_artifacts.leaves.encoded_bytes,
                "role": hierarchy_artifacts.leaves.role,
                "sha256": hierarchy_artifacts.leaves.sha256,
            },
            "manifest": {
                "encoded_bytes": manifest.encoded_bytes,
                "role": manifest.role,
                "sha256": manifest.sha256,
            },
            "modes": {
                "encoded_bytes": layout.modes.encoded_bytes,
                "role": layout.modes.role,
                "sha256": layout.modes.sha256,
            },
            "postings": {
                "encoded_bytes": layout.postings.encoded_bytes,
                "role": layout.postings.role,
                "sha256": layout.postings.sha256,
            },
            "roots": {
                "encoded_bytes": hierarchy_artifacts.roots.encoded_bytes,
                "role": hierarchy_artifacts.roots.role,
                "sha256": hierarchy_artifacts.roots.sha256,
            },
        },
        "claim_eligible": false,
        "pages": receipt.pages.len(),
        "primary_rows": receipt.primary_rows,
        "replica_rows": receipt.replica_rows,
        "schema": "borsuk-v27-s3-build-receipt-v1",
        "source_rows": receipt.source_rows,
        "stored_rows": receipt.stored_rows,
        "train_sha256": args.train_sha256,
    });
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|_| invalid("V27 build receipt serialization failed"))?;
    bytes.push(b'\n');
    write_file(&args.output.join("BUILD_COMPLETE.json"), &bytes)?;
    Ok(bytes)
}

#[cfg(not(test))]
fn main() {
    match parse_args(std::env::args().collect())
        .map_err(|error| invalid(&error))
        .and_then(execute)
    {
        Ok(bytes) => {
            if let Err(error) = std::io::stdout().write_all(&bytes) {
                eprintln!("v27_s3_build: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("v27_s3_build: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{execute, parse_args};

    #[test]
    fn v27_s3_build_streams_a_bounded_parquet_subset_into_page_artifacts() {
        // Break caught: the reduced campaign copies a corpus tree or retains row vectors instead
        // of consuming one explicit bounded Parquet input and emitting page/index artifacts.
        let directory = tempdir().unwrap();
        let train = directory.path().join("train.parquet");
        let output = directory.path().join("index");
        write_train(&train, 128);
        let bytes = fs::read(&train).unwrap();
        let args = parse_args(vec![
            "v27_s3_build".to_owned(),
            "--execute".to_owned(),
            "--train-parquet".to_owned(),
            train.display().to_string(),
            "--train-sha256".to_owned(),
            format!("{:x}", Sha256::digest(&bytes)),
            "--train-bytes".to_owned(),
            bytes.len().to_string(),
            "--row-limit".to_owned(),
            "128".to_owned(),
            "--roots".to_owned(),
            "4".to_owned(),
            "--leaves".to_owned(),
            "16".to_owned(),
            "--iterations".to_owned(),
            "1".to_owned(),
            "--workers".to_owned(),
            "2".to_owned(),
            "--page-rows".to_owned(),
            "16".to_owned(),
            "--output-dir".to_owned(),
            output.display().to_string(),
        ])
        .unwrap();
        let receipt = execute(args).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&receipt).unwrap();
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["source_rows"], 128);
        assert!(value["pages"].as_u64().unwrap() >= 8);
        for name in [
            "roots.arrow",
            "leaves.arrow",
            "postings.parquet",
            "modes.arrow",
            "pages.json",
            "BUILD_COMPLETE.json",
        ] {
            assert!(output.join(name).is_file(), "missing {name}");
        }
        assert!(!output.join("train.parquet").exists());
        assert_eq!(
            fs::read(output.join("BUILD_COMPLETE.json")).unwrap(),
            receipt
        );
    }

    fn write_train(path: &std::path::Path, rows: usize) {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let values = (0..rows).flat_map(|row| {
            (0..96).map(move |dimension| {
                if dimension == row % 16 {
                    1.0 + row as f32 / rows as f32
                } else {
                    (dimension as f32 + 1.0) * 0.0001
                }
            })
        });
        let vectors = FixedSizeListArray::try_new(
            child.clone(),
            96,
            Arc::new(Float32Array::from_iter_values(values)),
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "emb",
            DataType::FixedSizeList(child, 96),
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors) as ArrayRef]).unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}
