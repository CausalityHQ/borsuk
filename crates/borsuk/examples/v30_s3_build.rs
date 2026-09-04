//! Query-blind V30 constructor that streams authenticated S3 corpus shards on a Spot builder.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    BorsukError, V27HierarchyConfig, V27PageIdentity, V27PageRow, V30ConstructionBuilder,
    V30ConstructionConfig, V30PageSink, V30Scratch,
};
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::runtime::Runtime;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Args {
    corpus_manifest_s3: String,
    s3_region: String,
    corpus_manifest_sha256: String,
    corpus_manifest_bytes: u64,
    source_commit: String,
    expected_rows: u64,
    roots: usize,
    leaves: usize,
    training_rows: usize,
    page_rows: usize,
    output_s3_prefix: String,
    scratch_dir: PathBuf,
}

fn argument_error(message: &str) -> String {
    format!("V30 build arguments {message}")
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
        corpus_manifest_s3: take(&mut map, "corpus-manifest-s3")?,
        s3_region: take(&mut map, "s3-region")?,
        corpus_manifest_sha256: take(&mut map, "corpus-manifest-sha256")?,
        corpus_manifest_bytes: number(&mut map, "corpus-manifest-bytes")?,
        source_commit: take(&mut map, "source-commit")?,
        expected_rows: number(&mut map, "expected-rows")?,
        roots: number(&mut map, "roots")?,
        leaves: number(&mut map, "leaves")?,
        training_rows: number(&mut map, "training-rows")?,
        page_rows: number(&mut map, "page-rows")?,
        output_s3_prefix: take(&mut map, "output-s3-prefix")?,
        scratch_dir: PathBuf::from(take(&mut map, "scratch-dir")?),
    };
    if !map.is_empty()
        || !args.corpus_manifest_s3.starts_with("s3://")
        || args.s3_region != "eu-central-1"
        || !args.output_s3_prefix.starts_with("s3://")
        || !args.output_s3_prefix.ends_with('/')
        || args.corpus_manifest_sha256.len() != 64
        || !args
            .corpus_manifest_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || args.corpus_manifest_bytes == 0
        || args.source_commit.len() != 40
        || !args
            .source_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || args.expected_rows == 0
        || args.roots == 0
        || args.leaves < args.roots
        || !args.roots.is_power_of_two()
        || !args.leaves.is_power_of_two()
        || !args.leaves.is_multiple_of(args.roots)
        || args.training_rows < args.leaves.saturating_mul(2)
        || args.page_rows == 0
        || args.page_rows > 512
        || !args.scratch_dir.is_absolute()
    {
        return Err(argument_error("authority or numeric bound differs"));
    }
    Ok(args)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusShard {
    encoded_bytes: u64,
    physical_row_count: u64,
    row_count: u64,
    row_start: u64,
    sha256: String,
    uri: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusManifest {
    dataset_id: String,
    schema_version: u8,
    shards: Vec<CorpusShard>,
    source_rows: u64,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn io_error(path: &FsPath, source: std::io::Error) -> BorsukError {
    BorsukError::Io {
        path: path.to_owned(),
        source,
    }
}

fn object_path(uri: &str) -> borsuk::Result<Path> {
    let url = Url::parse(uri).map_err(|_| invalid("V30 build S3 URI differs"))?;
    if url.scheme() != "s3" || url.host_str().is_none() {
        return Err(invalid("V30 build S3 URI differs"));
    }
    Path::from_url_path(url.path()).map_err(|_| invalid("V30 build S3 object path differs"))
}

fn bucket(uri: &str) -> borsuk::Result<String> {
    Url::parse(uri)
        .map_err(|_| invalid("V30 build S3 URI differs"))?
        .host_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("V30 build S3 bucket differs"))
}

fn canonical_bytes(value: serde_json::Value) -> borsuk::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|_| invalid("V30 build canonical JSON serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn read_manifest(
    args: &Args,
    runtime: &Runtime,
    store: &Arc<dyn ObjectStore>,
) -> borsuk::Result<CorpusManifest> {
    let bytes = runtime.block_on(async {
        store
            .get(&object_path(&args.corpus_manifest_s3)?)
            .await?
            .bytes()
            .await
            .map_err(BorsukError::from)
    })?;
    if bytes.len() as u64 != args.corpus_manifest_bytes
        || format!("{:x}", Sha256::digest(&bytes)) != args.corpus_manifest_sha256
        || !bytes.ends_with(b"\n")
    {
        return Err(invalid("V30 build corpus manifest byte authority differs"));
    }
    let manifest: CorpusManifest = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("V30 build corpus manifest JSON differs"))?;
    if manifest.schema_version != 1
        || manifest.dataset_id != "deep-image-96"
        || manifest.source_rows != args.expected_rows
        || manifest.shards.is_empty()
    {
        return Err(invalid("V30 build corpus manifest authority differs"));
    }
    let mut next = 0_u64;
    for shard in &manifest.shards {
        if shard.row_start != next
            || shard.row_count == 0
            || shard.physical_row_count < shard.row_count
            || shard.encoded_bytes == 0
            || shard.sha256.len() != 64
            || !shard
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !shard.uri.starts_with("s3://")
            || bucket(&shard.uri)? != bucket(&args.corpus_manifest_s3)?
        {
            return Err(invalid("V30 build corpus shard authority differs"));
        }
        next = next
            .checked_add(shard.row_count)
            .ok_or_else(|| invalid("V30 build corpus row count overflows"))?;
    }
    if next != manifest.source_rows {
        return Err(invalid("V30 build corpus shard coverage differs"));
    }
    Ok(manifest)
}

fn query_schema() -> Schema {
    Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        ),
        false,
    )])
}

struct S3CorpusRows {
    runtime: Arc<Runtime>,
    store: Arc<dyn ObjectStore>,
    shards: Vec<CorpusShard>,
    scratch_dir: PathBuf,
    next_shard: usize,
    current_shard: Option<CorpusShard>,
    current_rows: u64,
    reader: Option<ParquetRecordBatchReader>,
    buffered: std::vec::IntoIter<V27PageRow>,
    error: Option<BorsukError>,
}

impl S3CorpusRows {
    fn open_next_shard(&mut self) -> borsuk::Result<bool> {
        let Some(shard) = self.shards.get(self.next_shard).cloned() else {
            return Ok(false);
        };
        let path = self.scratch_dir.join("current-shard.parquet");
        let mut file = File::create(&path).map_err(|source| io_error(&path, source))?;
        let mut digest = Sha256::new();
        let mut encoded_bytes = 0_u64;
        self.runtime.block_on(async {
            let mut stream = self
                .store
                .get(&object_path(&shard.uri)?)
                .await?
                .into_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk)
                    .map_err(|source| io_error(&path, source))?;
                digest.update(&chunk);
                encoded_bytes = encoded_bytes
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| invalid("V30 build shard bytes overflow"))?;
            }
            file.flush().map_err(|source| io_error(&path, source))
        })?;
        drop(file);
        if encoded_bytes != shard.encoded_bytes
            || format!("{:x}", digest.finalize()) != shard.sha256
        {
            let _ = fs::remove_file(&path);
            return Err(invalid("V30 build corpus shard byte authority differs"));
        }
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            File::open(&path).map_err(|source| io_error(&path, source))?,
        )?;
        if u64::try_from(builder.metadata().file_metadata().num_rows())
            .map_err(|_| invalid("V30 build corpus physical rows overflow"))?
            != shard.physical_row_count
        {
            let _ = fs::remove_file(&path);
            return Err(invalid("V30 build corpus physical row count differs"));
        }
        if builder.schema().as_ref() != &query_schema() {
            let _ = fs::remove_file(&path);
            return Err(invalid("V30 build corpus Parquet schema differs"));
        }
        self.reader = Some(builder.with_batch_size(8_192).build()?);
        self.current_shard = Some(shard);
        self.current_rows = 0;
        Ok(true)
    }

    fn load_next_batch(&mut self) -> borsuk::Result<bool> {
        if self
            .current_shard
            .as_ref()
            .is_some_and(|shard| self.current_rows == shard.row_count)
        {
            let path = self.scratch_dir.join("current-shard.parquet");
            fs::remove_file(&path).map_err(|source| io_error(&path, source))?;
            self.current_shard = None;
            self.reader = None;
            self.next_shard += 1;
            return Ok(false);
        }
        let Some(reader) = self.reader.as_mut() else {
            return Ok(false);
        };
        let Some(batch) = reader.next() else {
            let shard = self
                .current_shard
                .take()
                .ok_or_else(|| invalid("V30 build corpus shard state differs"))?;
            self.reader = None;
            if self.current_rows != shard.row_count {
                return Err(invalid("V30 build corpus shard row count differs"));
            }
            let path = self.scratch_dir.join("current-shard.parquet");
            fs::remove_file(&path).map_err(|source| io_error(&path, source))?;
            self.next_shard += 1;
            return Ok(false);
        };
        let batch = batch?;
        if batch.num_columns() != 1 || batch.column(0).null_count() != 0 {
            return Err(invalid("V30 build corpus Parquet nullability differs"));
        }
        let vectors = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| invalid("V30 build corpus vector type differs"))?;
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V30 build corpus value type differs"))?;
        let shard = self
            .current_shard
            .as_ref()
            .ok_or_else(|| invalid("V30 build corpus shard state differs"))?;
        let remaining = usize::try_from(shard.row_count - self.current_rows)
            .map_err(|_| invalid("V30 build corpus selected rows overflow"))?;
        let selected_rows = batch.num_rows().min(remaining);
        let mut rows = Vec::with_capacity(selected_rows);
        for row in 0..selected_rows {
            let start = row * 96;
            let vector = values.values()[start..start + 96]
                .try_into()
                .map_err(|_| invalid("V30 build corpus dimension differs"))?;
            rows.push(V27PageRow {
                source_ordinal: shard
                    .row_start
                    .checked_add(self.current_rows)
                    .and_then(|value| value.checked_add(row as u64))
                    .ok_or_else(|| invalid("V30 build source ordinal overflows"))?,
                vector,
            });
        }
        self.current_rows = self
            .current_rows
            .checked_add(selected_rows as u64)
            .ok_or_else(|| invalid("V30 build corpus shard rows overflow"))?;
        if self.current_rows > shard.row_count {
            return Err(invalid("V30 build corpus shard row count differs"));
        }
        self.buffered = rows.into_iter();
        Ok(true)
    }
}

impl Iterator for S3CorpusRows {
    type Item = V27PageRow;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.buffered.next() {
                return Some(row);
            }
            let next = if self.reader.is_some() {
                self.load_next_batch()
            } else {
                self.open_next_shard()
            };
            match next {
                Ok(true) => {}
                Ok(false) if self.next_shard < self.shards.len() => {}
                Ok(false) => return None,
                Err(error) => {
                    self.error = Some(error);
                    return None;
                }
            }
        }
    }
}

fn cleanup_scratch(path: &FsPath) -> borsuk::Result<()> {
    fs::remove_dir_all(path).map_err(|source| io_error(path, source))
}

struct FileScratch {
    directory: PathBuf,
}

impl V30Scratch for FileScratch {
    fn write_scratch(
        &mut self,
        key: &str,
        write: &mut dyn FnMut(&mut dyn Write) -> borsuk::Result<()>,
    ) -> borsuk::Result<()> {
        let path = self.directory.join(key);
        let mut file = File::create(&path).map_err(|source| io_error(&path, source))?;
        write(&mut file)?;
        file.flush().map_err(|source| io_error(&path, source))
    }

    fn open_scratch(&self, key: &str) -> borsuk::Result<Box<dyn Read + Send>> {
        let path = self.directory.join(key);
        Ok(Box::new(
            File::open(&path).map_err(|source| io_error(&path, source))?,
        ))
    }

    fn remove_scratch(&mut self, key: &str) -> borsuk::Result<()> {
        let path = self.directory.join(key);
        fs::remove_file(&path).map_err(|source| io_error(&path, source))
    }
}

struct S3PageSink {
    runtime: Arc<Runtime>,
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl V30PageSink for S3PageSink {
    fn write_page(&mut self, identity: &V27PageIdentity, bytes: &[u8]) -> borsuk::Result<()> {
        let path = Path::from(format!("{}pages/{}.arrow", self.prefix, identity.sha256));
        self.runtime.block_on(async {
            self.store
                .put(&path, PutPayload::from(Bytes::copy_from_slice(bytes)))
                .await
                .map(|_| ())
                .map_err(BorsukError::from)
        })
    }
}

fn put_bytes(
    runtime: &Runtime,
    store: &Arc<dyn ObjectStore>,
    path: String,
    bytes: Vec<u8>,
) -> borsuk::Result<()> {
    runtime.block_on(async {
        store
            .put(&Path::from(path), PutPayload::from(Bytes::from(bytes)))
            .await
            .map(|_| ())
            .map_err(BorsukError::from)
    })
}

fn execute_with_store(args: Args, store: Arc<dyn ObjectStore>) -> borsuk::Result<Vec<u8>> {
    if args.scratch_dir.exists() {
        return Err(invalid("V30 build scratch already exists"));
    }
    fs::create_dir_all(&args.scratch_dir).map_err(|source| io_error(&args.scratch_dir, source))?;
    let runtime = Arc::new(Runtime::new().map_err(|_| invalid("V30 build runtime differs"))?);
    let result = (|| {
        if bucket(&args.corpus_manifest_s3)? != bucket(&args.output_s3_prefix)? {
            return Err(invalid("V30 build input/output bucket differs"));
        }
        let manifest = read_manifest(&args, &runtime, &store)?;
        let mut rows = S3CorpusRows {
            runtime: Arc::clone(&runtime),
            store: Arc::clone(&store),
            shards: manifest.shards,
            scratch_dir: args.scratch_dir.clone(),
            next_shard: 0,
            current_shard: None,
            current_rows: 0,
            reader: None,
            buffered: Vec::new().into_iter(),
            error: None,
        };
        let mut scratch = FileScratch {
            directory: args.scratch_dir.clone(),
        };
        let output_path = object_path(&args.output_s3_prefix)?.to_string();
        let output_path = format!("{}/", output_path.trim_end_matches('/'));
        let mut pages = S3PageSink {
            runtime: Arc::clone(&runtime),
            store: Arc::clone(&store),
            prefix: output_path.clone(),
        };
        let built = V30ConstructionBuilder::build(
            &mut rows,
            V30ConstructionConfig {
                hierarchy: V27HierarchyConfig {
                    roots: args.roots,
                    leaves: args.leaves,
                    iterations: 4,
                    seed: 0x6a09_e667_f3bc_c909,
                    worker_count: std::thread::available_parallelism()
                        .map(usize::from)
                        .unwrap_or(1),
                    batch_rows: 8_192,
                },
                training_rows: args.training_rows,
                page_rows: args.page_rows,
                sort_memory_rows: 1_000_000,
                fidelity_ppm: 50_000,
            },
            &mut scratch,
            &mut pages,
        );
        if let Some(error) = rows.error {
            return Err(error);
        }
        let artifacts = built?.into_artifacts()?;
        let hierarchy_files = [
            (
                "roots.arrow",
                &artifacts.hierarchy.roots,
                artifacts.hierarchy.roots_bytes,
            ),
            (
                "leaves.arrow",
                &artifacts.hierarchy.leaves,
                artifacts.hierarchy.leaves_bytes,
            ),
        ];
        for (file, _, bytes) in hierarchy_files {
            put_bytes(&runtime, &store, format!("{output_path}{file}"), bytes)?;
        }
        let pq_files = [
            "pq24-codebook.arrow",
            "pq48-codebook.arrow",
            "pq-base-codes.arrow",
            "pq-fidelity.arrow",
            "pq-high-codes.arrow",
        ];
        for (file, bytes) in pq_files.iter().zip(artifacts.pq.bytes.iter()) {
            put_bytes(
                &runtime,
                &store,
                format!("{output_path}{file}"),
                bytes.clone(),
            )?;
        }
        put_bytes(
            &runtime,
            &store,
            format!("{output_path}leaf-ranges.arrow"),
            artifacts.layout.leaf_ranges_arrow.clone(),
        )?;
        put_bytes(
            &runtime,
            &store,
            format!("{output_path}page-offsets.parquet"),
            artifacts.layout.page_ranges_parquet.clone(),
        )?;
        let disk_artifact = |file: &str, role: &str, sha256: &str, encoded_bytes: u64| {
            serde_json::json!({
                "encoded_bytes": encoded_bytes,
                "file": file,
                "role": role,
                "sha256": sha256,
            })
        };
        let pq = artifacts
            .pq
            .identities
            .iter()
            .zip(pq_files)
            .map(|(identity, file)| {
                serde_json::json!({
                    "dependencies": identity.dependencies,
                    "encoded_bytes": identity.encoded_bytes,
                    "file": file,
                    "role": identity.role,
                    "row_count": identity.row_count,
                    "sha256": identity.sha256,
                    "width_bytes": identity.width_bytes,
                })
            })
            .collect::<Vec<_>>();
        let manifest_bytes = canonical_bytes(serde_json::json!({
            "hierarchy": {
                "leaves": disk_artifact("leaves.arrow", &artifacts.hierarchy.leaves.role, &artifacts.hierarchy.leaves.sha256, artifacts.hierarchy.leaves.encoded_bytes),
                "roots": disk_artifact("roots.arrow", &artifacts.hierarchy.roots.role, &artifacts.hierarchy.roots.sha256, artifacts.hierarchy.roots.encoded_bytes),
            },
            "layout": {
                "leaf_ranges": disk_artifact("leaf-ranges.arrow", &artifacts.layout.leaf_ranges.role, &artifacts.layout.leaf_ranges.sha256, artifacts.layout.leaf_ranges.encoded_bytes),
                "page_ranges": disk_artifact("page-offsets.parquet", &artifacts.layout.page_ranges.role, &artifacts.layout.page_ranges.sha256, artifacts.layout.page_ranges.encoded_bytes),
                "source_rows": artifacts.source_rows,
            },
            "page_key_suffix": ".arrow",
            "pq": {"artifacts": pq},
            "schema_version": 1,
            "source": {
                "commit": args.source_commit,
                "corpus_manifest_bytes": args.corpus_manifest_bytes,
                "corpus_manifest_sha256": args.corpus_manifest_sha256,
                "corpus_manifest_uri": args.corpus_manifest_s3,
                "dataset_id": "deep-image-96",
            },
        }))?;
        put_bytes(
            &runtime,
            &store,
            format!("{output_path}manifest.json"),
            manifest_bytes.clone(),
        )?;
        let first_page = artifacts
            .pages
            .first()
            .ok_or_else(|| invalid("V30 build emitted no pages"))?;
        canonical_bytes(serde_json::json!({
            "claim_eligible": false,
            "first_page_sha256": first_page.sha256,
            "manifest_bytes": manifest_bytes.len(),
            "manifest_sha256": format!("{:x}", Sha256::digest(&manifest_bytes)),
            "pages": artifacts.pages.len(),
            "source_commit": args.source_commit,
            "source_rows": artifacts.source_rows,
            "status": "passed",
            "training_rows": artifacts.training_rows,
        }))
    })();
    let cleanup = cleanup_scratch(&args.scratch_dir);
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(bytes), Ok(())) => Ok(bytes),
    }
}

#[cfg(not(test))]
fn main() {
    let result = parse_args(std::env::args().collect())
        .map_err(|error| invalid(&error))
        .and_then(|args| {
            let url = Url::parse(&args.corpus_manifest_s3)
                .map_err(|_| invalid("V30 build corpus manifest URI differs"))?;
            let (store, _) =
                object_store::parse_url_opts(&url, [("aws_region", args.s3_region.as_str())])?;
            execute_with_store(args, Arc::from(store))
        });
    match result {
        Ok(bytes) => {
            if let Err(error) = std::io::stdout().write_all(&bytes) {
                eprintln!("v30_s3_build: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("v30_s3_build: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Cursor, sync::Arc};

    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
    use parquet::arrow::ArrowWriter;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{Args, CorpusShard, S3CorpusRows, cleanup_scratch, execute_with_store, parse_args};

    fn args() -> Vec<String> {
        let digest = "a".repeat(64);
        vec![
            "v30_s3_build",
            "--execute",
            "--corpus-manifest-s3",
            "s3://bucket/deep-10m/corpus.json",
            "--s3-region",
            "eu-central-1",
            "--corpus-manifest-sha256",
            &digest,
            "--corpus-manifest-bytes",
            "4096",
            "--source-commit",
            "b701eada33a5d6782f9ebb0adaac5fd7573da40f",
            "--expected-rows",
            "9990000",
            "--roots",
            "1024",
            "--leaves",
            "32768",
            "--training-rows",
            "262144",
            "--page-rows",
            "512",
            "--output-s3-prefix",
            "s3://bucket/v30/build-a0001/",
            "--scratch-dir",
            "/data/v30-build-a0001",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn v30_s3_build_requires_one_explicit_query_blind_s3_construction() {
        let parsed = parse_args(args()).unwrap();
        assert_eq!(parsed.expected_rows, 9_990_000);
        assert_eq!(parsed.s3_region, "eu-central-1");
        assert_eq!(parsed.training_rows, 262_144);
        assert_eq!(parsed.page_rows, 512);
        assert_eq!(
            parsed.source_commit,
            "b701eada33a5d6782f9ebb0adaac5fd7573da40f"
        );

        for forbidden in ["--query", "--truth", "--latest", "--legacy", "--d3"] {
            let mut values = args();
            values.extend([forbidden.to_owned(), "value".to_owned()]);
            assert!(parse_args(values).is_err(), "accepted {forbidden}");
        }
        let mut duplicate = args();
        duplicate.extend(["--roots".to_owned(), "1024".to_owned()]);
        assert!(parse_args(duplicate).is_err());
    }

    #[test]
    fn v30_s3_build_streams_authenticated_shards_and_uploads_only_pages_and_artifacts() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parquet = parquet_rows(320);
        let shard_sha = format!("{:x}", Sha256::digest(&parquet));
        let manifest_value = serde_json::json!({
            "dataset_id": "deep-image-96",
            "schema_version": 1,
            "shards": [{
                "encoded_bytes": parquet.len(),
                "physical_row_count": 320,
                "row_count": 320,
                "row_start": 0,
                "sha256": shard_sha,
                "uri": "s3://bucket/train-00000000.parquet",
            }],
            "source_rows": 320,
        });
        let mut manifest = serde_json::to_vec(&manifest_value).unwrap();
        manifest.push(b'\n');
        let corpus_manifest_sha = format!("{:x}", Sha256::digest(&manifest));
        runtime.block_on(async {
            store
                .put(
                    &Path::from("train-00000000.parquet"),
                    PutPayload::from(parquet),
                )
                .await
                .unwrap();
            store
                .put(
                    &Path::from("corpus.json"),
                    PutPayload::from(Bytes::from(manifest.clone())),
                )
                .await
                .unwrap();
        });
        let directory = tempdir().unwrap();
        let scratch = directory.path().join("scratch");
        let args = Args {
            corpus_manifest_s3: "s3://bucket/corpus.json".to_owned(),
            s3_region: "eu-central-1".to_owned(),
            corpus_manifest_sha256: format!("{:x}", Sha256::digest(&manifest)),
            corpus_manifest_bytes: manifest.len() as u64,
            source_commit: "b701eada33a5d6782f9ebb0adaac5fd7573da40f".to_owned(),
            expected_rows: 320,
            roots: 2,
            leaves: 4,
            training_rows: 256,
            page_rows: 32,
            output_s3_prefix: "s3://bucket/v30/build-a0001/".to_owned(),
            scratch_dir: scratch.clone(),
        };
        let terminal = execute_with_store(args, Arc::clone(&store)).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&terminal).unwrap();
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["source_rows"], 320);
        assert_eq!(value["training_rows"], 256);
        assert_eq!(value["status"], "passed");
        assert!(!scratch.exists());
        runtime.block_on(async {
            let manifest_bytes = store
                .get(&Path::from("v30/build-a0001/manifest.json"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
            assert_eq!(
                manifest["source"]["commit"],
                "b701eada33a5d6782f9ebb0adaac5fd7573da40f"
            );
            assert_eq!(
                manifest["source"]["corpus_manifest_sha256"],
                corpus_manifest_sha
            );
            store
                .get(&Path::from(
                    "v30/build-a0001/pages/".to_owned()
                        + &value["first_page_sha256"].as_str().unwrap().to_owned()
                        + ".arrow",
                ))
                .await
                .unwrap();
        });
    }

    #[test]
    fn v30_s3_build_decodes_each_corpus_shard_in_bounded_batches() {
        // Break caught: one Parquet shard was decoded into a corpus-sized Vec
        // before the construction builder could apply its own spill bound.
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parquet = parquet_rows(20_000);
        let sha256 = format!("{:x}", Sha256::digest(&parquet));
        runtime.block_on(async {
            store
                .put(
                    &Path::from("train-00000000.parquet"),
                    PutPayload::from(parquet.clone()),
                )
                .await
                .unwrap();
        });
        let directory = tempdir().unwrap();
        let mut rows = S3CorpusRows {
            runtime,
            store,
            shards: vec![CorpusShard {
                encoded_bytes: parquet.len() as u64,
                physical_row_count: 20_000,
                row_count: 20_000,
                row_start: 0,
                sha256,
                uri: "s3://bucket/train-00000000.parquet".to_owned(),
            }],
            scratch_dir: directory.path().to_owned(),
            next_shard: 0,
            current_shard: None,
            current_rows: 0,
            reader: None,
            buffered: Vec::new().into_iter(),
            error: None,
        };
        assert_eq!(rows.next().unwrap().source_ordinal, 0);
        assert!(rows.buffered.len() <= 8_191);
    }

    #[test]
    fn v30_s3_build_selects_a_registered_prefix_without_copying_the_shard() {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parquet = parquet_rows(20_000);
        let sha256 = format!("{:x}", Sha256::digest(&parquet));
        runtime.block_on(async {
            store
                .put(
                    &Path::from("train-00000000.parquet"),
                    PutPayload::from(parquet.clone()),
                )
                .await
                .unwrap();
        });
        let directory = tempdir().unwrap();
        let mut rows = S3CorpusRows {
            runtime,
            store,
            shards: vec![CorpusShard {
                encoded_bytes: parquet.len() as u64,
                physical_row_count: 20_000,
                row_count: 10_000,
                row_start: 0,
                sha256,
                uri: "s3://bucket/train-00000000.parquet".to_owned(),
            }],
            scratch_dir: directory.path().to_owned(),
            next_shard: 0,
            current_shard: None,
            current_rows: 0,
            reader: None,
            buffered: Vec::new().into_iter(),
            error: None,
        };
        let selected = rows.by_ref().collect::<Vec<_>>();
        assert_eq!(selected.len(), 10_000);
        assert_eq!(selected.last().unwrap().source_ordinal, 9_999);
        assert!(rows.error.is_none());
        assert!(!directory.path().join("current-shard.parquet").exists());
    }

    #[test]
    fn v30_s3_build_cleanup_removes_only_the_owned_scratch_tree() {
        // Break caught: failed builds retained spill runs, while successful
        // builds could be marked failed after uploading because remove_dir
        // rejects a non-empty owned scratch directory.
        let directory = tempdir().unwrap();
        let scratch = directory.path().join("owned-scratch");
        fs::create_dir(&scratch).unwrap();
        fs::write(scratch.join("spill-run"), b"owned").unwrap();
        cleanup_scratch(&scratch).unwrap();
        assert!(!scratch.exists());
    }

    fn parquet_rows(rows: usize) -> Vec<u8> {
        let mut values = Vec::with_capacity(rows * 96);
        for row in 0..rows {
            for dimension in 0..96 {
                values.push(if dimension == row % 96 { 1.0 } else { 0.01 });
            }
        }
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let vectors = FixedSizeListArray::try_new(
            Arc::clone(&child),
            96,
            Arc::new(Float32Array::from(values)) as ArrayRef,
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "emb",
            DataType::FixedSizeList(child, 96),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(vectors)]).unwrap();
        let mut bytes = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes.into_inner()
    }
}
