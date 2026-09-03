//! Explicit local/S3 qualification boundary for the V27 page index.

use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    BorsukError, V27HierarchyArtifactIdentity, V27LayoutArtifactIdentity, V27LayoutArtifacts,
    V27PageIdentity, V27PageStore, V27Router, V27SearchArm, V27SearchIndex, decode_v27_hierarchy,
    decode_v27_layout, decode_v27_page_manifest,
};
use bytes::Bytes;
use futures_util::future::try_join_all;
use object_store::{ObjectStore, ObjectStoreExt, parse_url_opts, path::Path as ObjectPath};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArtifactArg {
    path: PathBuf,
    sha256: String,
    encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PageSource {
    Local(PathBuf),
    S3(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Args {
    roots: ArtifactArg,
    leaves: ArtifactArg,
    postings: ArtifactArg,
    modes: ArtifactArg,
    manifest: ArtifactArg,
    query: ArtifactArg,
    query_row: usize,
    root_beam: usize,
    leaf_beam: usize,
    page_count: usize,
    k: usize,
    page_source: PageSource,
}

fn argument_error(message: &str) -> String {
    format!("V27 qualifier arguments {message}")
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

fn artifact_path(
    map: &mut BTreeMap<String, String>,
    path_flag: &str,
    role: &str,
) -> Result<ArtifactArg, String> {
    let path = PathBuf::from(take(map, path_flag)?);
    let sha256 = take(map, &format!("{role}-sha256"))?;
    let encoded_bytes = number(map, &format!("{role}-bytes"))?;
    if path.as_os_str().is_empty()
        || encoded_bytes == 0
        || sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(argument_error(&format!("--{role} authority differs")));
    }
    Ok(ArtifactArg {
        path,
        sha256,
        encoded_bytes,
    })
}

fn artifact(map: &mut BTreeMap<String, String>, role: &str) -> Result<ArtifactArg, String> {
    artifact_path(map, role, role)
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
    let roots = artifact(&mut map, "roots")?;
    let leaves = artifact(&mut map, "leaves")?;
    let postings = artifact(&mut map, "postings")?;
    let modes = artifact(&mut map, "modes")?;
    let manifest = artifact(&mut map, "manifest")?;
    let query = artifact_path(&mut map, "query-parquet", "query")?;
    let query_row = number(&mut map, "query-row")?;
    let root_beam = number(&mut map, "root-beam")?;
    let leaf_beam = number(&mut map, "leaf-beam")?;
    let page_count = number(&mut map, "page-count")?;
    let k = number(&mut map, "k")?;
    let local = map.remove("local-page-dir").map(PathBuf::from);
    let s3 = map.remove("s3-page-prefix");
    let page_source = match (local, s3) {
        (Some(path), None) if !path.as_os_str().is_empty() => PageSource::Local(path),
        (None, Some(uri)) if uri.starts_with("s3://") && !uri.ends_with('/') => PageSource::S3(uri),
        _ => {
            return Err(argument_error(
                "exactly one explicit page source is required",
            ));
        }
    };
    if !map.is_empty()
        || root_beam == 0
        || leaf_beam == 0
        || page_count == 0
        || page_count > 10
        || k == 0
        || k > 10_240
    {
        return Err(argument_error("unknown flag or numeric bound differs"));
    }
    Ok(Args {
        roots,
        leaves,
        postings,
        modes,
        manifest,
        query,
        query_row,
        root_beam,
        leaf_beam,
        page_count,
        k,
        page_source,
    })
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn read_artifact(argument: &ArtifactArg) -> borsuk::Result<Vec<u8>> {
    let bytes = fs::read(&argument.path).map_err(|source| BorsukError::Io {
        path: argument.path.clone(),
        source,
    })?;
    if bytes.len() as u64 != argument.encoded_bytes
        || format!("{:x}", Sha256::digest(&bytes)) != argument.sha256
    {
        return Err(invalid("V27 qualifier artifact byte authority differs"));
    }
    Ok(bytes)
}

fn hierarchy_identity(role: &str, argument: &ArtifactArg) -> V27HierarchyArtifactIdentity {
    V27HierarchyArtifactIdentity {
        role: role.to_owned(),
        sha256: argument.sha256.clone(),
        encoded_bytes: argument.encoded_bytes,
    }
}

fn layout_identity(role: &str, argument: &ArtifactArg) -> V27LayoutArtifactIdentity {
    V27LayoutArtifactIdentity {
        role: role.to_owned(),
        sha256: argument.sha256.clone(),
        encoded_bytes: argument.encoded_bytes,
    }
}

fn query_schema() -> Schema {
    let child = std::sync::Arc::new(Field::new("element", DataType::Float32, false));
    Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(child, 96),
        false,
    )])
}

fn read_query(argument: &ArtifactArg, query_row: usize) -> borsuk::Result<[f32; 96]> {
    let bytes = read_artifact(argument)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
    if builder.schema().as_ref() != &query_schema() {
        return Err(invalid("V27 qualifier query Parquet schema differs"));
    }
    let mut offset = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V27 qualifier query nullability differs"));
        }
        if query_row < offset + batch.num_rows() {
            let vectors = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| invalid("V27 qualifier query vector type differs"))?;
            let values = vectors
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V27 qualifier query value type differs"))?;
            let start = (query_row - offset) * 96;
            let mut query: [f32; 96] = values.values()[start..start + 96]
                .try_into()
                .map_err(|_| invalid("V27 qualifier query dimension differs"))?;
            if query.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V27 qualifier query value differs"));
            }
            let norm = query
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt();
            if !norm.is_finite() || norm <= 0.0 {
                return Err(invalid("V27 qualifier query norm differs"));
            }
            for value in &mut query {
                *value = (f64::from(*value) / norm) as f32;
            }
            return Ok(query);
        }
        offset += batch.num_rows();
    }
    Err(invalid("V27 qualifier query row differs"))
}

struct LocalPageStore {
    directory: PathBuf,
}

impl V27PageStore for LocalPageStore {
    fn read_wave(&self, pages: &[V27PageIdentity]) -> borsuk::Result<Vec<Vec<u8>>> {
        pages
            .iter()
            .map(|page| {
                let path = self.directory.join(format!("{}.arrow", page.sha256));
                fs::read(&path).map_err(|source| BorsukError::Io { path, source })
            })
            .collect()
    }
}

struct ObjectPageStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

impl V27PageStore for ObjectPageStore {
    fn read_wave(&self, pages: &[V27PageIdentity]) -> borsuk::Result<Vec<Vec<u8>>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| BorsukError::Io {
                path: PathBuf::from("tokio-runtime"),
                source,
            })?;
        runtime.block_on(async {
            let reads = pages.iter().map(|page| {
                let store = Arc::clone(&self.store);
                let path =
                    ObjectPath::from(format!("{}/{}.arrow", self.prefix.as_ref(), page.sha256));
                async move { Ok::<_, BorsukError>(store.get(&path).await?.bytes().await?.to_vec()) }
            });
            try_join_all(reads).await
        })
    }
}

fn run_search<S: V27PageStore>(
    router: V27Router,
    store: S,
    arm: V27SearchArm,
    query: &[f32; 96],
    k: usize,
) -> borsuk::Result<borsuk::V27SearchResult> {
    V27SearchIndex::new(router, store, arm)?.search(query, k)
}

fn execute(args: Args) -> borsuk::Result<Vec<u8>> {
    let roots = read_artifact(&args.roots)?;
    let leaves = read_artifact(&args.leaves)?;
    let postings = read_artifact(&args.postings)?;
    let modes = read_artifact(&args.modes)?;
    let manifest_bytes = read_artifact(&args.manifest)?;
    let hierarchy = decode_v27_hierarchy(
        &hierarchy_identity("v27-roots-arrow", &args.roots),
        &roots,
        &hierarchy_identity("v27-leaves-arrow", &args.leaves),
        &leaves,
    )?;
    let manifest = decode_v27_page_manifest(
        &layout_identity("v27-page-manifest-json", &args.manifest),
        &manifest_bytes,
    )?;
    let layout = V27LayoutArtifacts {
        postings: layout_identity("v27-page-postings-parquet", &args.postings),
        modes: layout_identity("v27-page-modes-arrow", &args.modes),
        postings_parquet: postings,
        modes_arrow: modes,
    };
    let router = V27Router::new(hierarchy, decode_v27_layout(&manifest.pages, &layout)?)?;
    let query = read_query(&args.query, args.query_row)?;
    let arm = V27SearchArm {
        root_beam: args.root_beam,
        leaf_beam: args.leaf_beam,
        page_count: args.page_count,
    };
    let result = match args.page_source {
        PageSource::Local(directory) => {
            run_search(router, LocalPageStore { directory }, arm, &query, args.k)?
        }
        PageSource::S3(uri) => {
            let url = Url::parse(&uri).map_err(|_| invalid("V27 qualifier S3 URI differs"))?;
            let options = std::env::vars().filter(|(key, _)| {
                matches!(
                    key.as_str(),
                    "AWS_ACCESS_KEY_ID"
                        | "AWS_SECRET_ACCESS_KEY"
                        | "AWS_SESSION_TOKEN"
                        | "AWS_REGION"
                )
            });
            let (store, prefix) = parse_url_opts(&url, options)?;
            let store: Arc<dyn ObjectStore> = store.into();
            run_search(
                router,
                ObjectPageStore { store, prefix },
                arm,
                &query,
                args.k,
            )?
        }
    };
    let matches = result
        .matches
        .iter()
        .map(|item| {
            serde_json::json!({
                "source_ordinal": item.source_ordinal,
                "squared_distance": item.squared_distance,
            })
        })
        .collect::<Vec<_>>();
    let mut bytes = serde_json::to_vec(&serde_json::json!({
        "claim_eligible": false,
        "matches": matches,
        "schema_version": 1,
        "work": {
            "decoded_rows": result.work.decoded_rows,
            "encoded_bytes": result.work.encoded_bytes,
            "get_count": result.work.get_count,
            "routing": {
                "leaf_centroids_scored": result.work.routing.leaves_scored,
                "page_modes_scored": result.work.routing.pages_scored,
                "peak_page_candidates": result.work.routing.peak_page_candidates,
                "postings_visited": result.work.routing.postings_visited,
                "root_centroids_scored": result.work.routing.roots_scored,
                "selected_pages": result.work.routing.selected_pages,
            },
            "unique_rows": result.work.unique_rows,
        },
    }))
    .map_err(|_| invalid("V27 qualifier result serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(not(test))]
fn main() {
    match parse_args(std::env::args().collect())
        .map_err(|error| invalid(&error))
        .and_then(execute)
    {
        Ok(bytes) => {
            use std::io::Write;
            if let Err(error) = std::io::stdout().write_all(&bytes) {
                eprintln!("v27_s3_qualify: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("v27_s3_qualify: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use object_store::{
        ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath,
    };
    use parquet::arrow::ArrowWriter;
    use sha2::Digest;
    use tempfile::tempdir;

    use super::{ArtifactArg, ObjectPageStore, execute, read_query};
    use super::{PageSource, parse_args};
    use borsuk::{
        V27BuildReceipt, V27Hierarchy, V27PagePosting, V27PageRow, V27PageStore,
        encode_v27_hierarchy, encode_v27_layout, encode_v27_page, encode_v27_page_manifest,
    };

    fn args() -> Vec<String> {
        [
            "v27_s3_qualify",
            "--execute",
            "--roots",
            "/tmp/roots.arrow",
            "--roots-sha256",
            &"1".repeat(64),
            "--roots-bytes",
            "196608",
            "--leaves",
            "/tmp/leaves.arrow",
            "--leaves-sha256",
            &"2".repeat(64),
            "--leaves-bytes",
            "12582912",
            "--postings",
            "/tmp/postings.parquet",
            "--postings-sha256",
            &"3".repeat(64),
            "--postings-bytes",
            "4096",
            "--modes",
            "/tmp/modes.arrow",
            "--modes-sha256",
            &"4".repeat(64),
            "--modes-bytes",
            "8192",
            "--manifest",
            "/tmp/pages.json",
            "--manifest-sha256",
            &"5".repeat(64),
            "--manifest-bytes",
            "2048",
            "--query-parquet",
            "/tmp/query.parquet",
            "--query-sha256",
            &"6".repeat(64),
            "--query-bytes",
            "4096",
            "--query-row",
            "7",
            "--root-beam",
            "8",
            "--leaf-beam",
            "128",
            "--page-count",
            "10",
            "--k",
            "10",
            "--s3-page-prefix",
            "s3://bucket/frozen/pages",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
    }

    #[test]
    fn v27_s3_qualify_cli_requires_explicit_authority_and_one_page_source() {
        let parsed = parse_args(args()).unwrap();
        assert_eq!(parsed.query_row, 7);
        assert_eq!(parsed.root_beam, 8);
        assert_eq!(parsed.leaf_beam, 128);
        assert_eq!(parsed.page_count, 10);
        assert_eq!(parsed.k, 10);
        assert_eq!(
            parsed.page_source,
            PageSource::S3("s3://bucket/frozen/pages".to_owned())
        );

        let mut local = args();
        local.truncate(local.len() - 2);
        local.extend(["--local-page-dir".to_owned(), "/tmp/pages".to_owned()]);
        assert_eq!(
            parse_args(local).unwrap().page_source,
            PageSource::Local("/tmp/pages".into())
        );
    }

    #[test]
    fn v27_s3_qualify_cli_rejects_implicit_mixed_and_unknown_modes() {
        let mut missing_execute = args();
        missing_execute.retain(|value| value != "--execute");
        assert!(parse_args(missing_execute).is_err());

        let mut mixed = args();
        mixed.extend(["--local-page-dir".to_owned(), "/tmp/pages".to_owned()]);
        assert!(parse_args(mixed).is_err());

        let mut duplicate = args();
        duplicate.extend(["--k".to_owned(), "9".to_owned()]);
        assert!(parse_args(duplicate).is_err());

        let mut unknown = args();
        unknown.extend(["--bucket".to_owned(), "implicit".to_owned()]);
        assert!(parse_args(unknown).is_err());

        let mut invalid_digest = args();
        let digest = invalid_digest
            .iter()
            .position(|value| value == "--roots-sha256")
            .unwrap()
            + 1;
        invalid_digest[digest] = "not-a-digest".to_owned();
        assert!(parse_args(invalid_digest).is_err());
    }

    #[test]
    fn v27_s3_qualify_executes_one_authenticated_local_page_wave() {
        let directory = tempdir().unwrap();
        let hierarchy = V27Hierarchy {
            roots: vec![centroid(0, 1.0)],
            leaves: vec![centroid(0, 1.0), centroid(1, 1.0)],
            leaf_roots: vec![0, 0],
        };
        let hierarchy_artifacts = encode_v27_hierarchy(&hierarchy).unwrap();
        let page_rows = [
            vec![row(7, 0, 1.0), row(9, 0, 0.8)],
            vec![row(11, 1, 1.0), row(13, 1, 0.8)],
        ];
        let pages = page_rows
            .iter()
            .enumerate()
            .map(|(ordinal, rows)| encode_v27_page(ordinal as u32, 2, 0, rows).unwrap())
            .collect::<Vec<_>>();
        let receipt = V27BuildReceipt {
            source_rows: 4,
            primary_rows: 4,
            replica_rows: 0,
            stored_rows: 4,
            pages: pages.iter().map(|page| page.0.clone()).collect(),
            postings: pages
                .iter()
                .enumerate()
                .map(|(ordinal, page)| V27PagePosting {
                    leaf_ordinal: ordinal as u32,
                    page: page.0.clone(),
                    modes: vec![hierarchy.leaves[ordinal]],
                })
                .collect(),
        };
        let layout = encode_v27_layout(&receipt).unwrap();
        let (manifest_identity, manifest_bytes) = encode_v27_page_manifest(&receipt).unwrap();

        let roots = directory.path().join("roots.arrow");
        let leaves = directory.path().join("leaves.arrow");
        let postings = directory.path().join("postings.parquet");
        let modes = directory.path().join("modes.arrow");
        let manifest = directory.path().join("pages.json");
        let query = directory.path().join("query.parquet");
        fs::write(&roots, &hierarchy_artifacts.roots_bytes).unwrap();
        fs::write(&leaves, &hierarchy_artifacts.leaves_bytes).unwrap();
        fs::write(&postings, &layout.postings_parquet).unwrap();
        fs::write(&modes, &layout.modes_arrow).unwrap();
        fs::write(&manifest, &manifest_bytes).unwrap();
        write_query(&query, &row(99, 0, 1.0).vector);
        for (identity, bytes) in &pages {
            fs::write(
                directory.path().join(format!("{}.arrow", identity.sha256)),
                bytes,
            )
            .unwrap();
        }

        let query_bytes = fs::read(&query).unwrap();
        let command = vec![
            "v27_s3_qualify".to_owned(),
            "--execute".to_owned(),
            "--roots".to_owned(),
            roots.display().to_string(),
            "--roots-sha256".to_owned(),
            hierarchy_artifacts.roots.sha256,
            "--roots-bytes".to_owned(),
            hierarchy_artifacts.roots.encoded_bytes.to_string(),
            "--leaves".to_owned(),
            leaves.display().to_string(),
            "--leaves-sha256".to_owned(),
            hierarchy_artifacts.leaves.sha256,
            "--leaves-bytes".to_owned(),
            hierarchy_artifacts.leaves.encoded_bytes.to_string(),
            "--postings".to_owned(),
            postings.display().to_string(),
            "--postings-sha256".to_owned(),
            layout.postings.sha256,
            "--postings-bytes".to_owned(),
            layout.postings.encoded_bytes.to_string(),
            "--modes".to_owned(),
            modes.display().to_string(),
            "--modes-sha256".to_owned(),
            layout.modes.sha256,
            "--modes-bytes".to_owned(),
            layout.modes.encoded_bytes.to_string(),
            "--manifest".to_owned(),
            manifest.display().to_string(),
            "--manifest-sha256".to_owned(),
            manifest_identity.sha256,
            "--manifest-bytes".to_owned(),
            manifest_identity.encoded_bytes.to_string(),
            "--query-parquet".to_owned(),
            query.display().to_string(),
            "--query-sha256".to_owned(),
            format!("{:x}", sha2::Sha256::digest(&query_bytes)),
            "--query-bytes".to_owned(),
            query_bytes.len().to_string(),
            "--query-row".to_owned(),
            "0".to_owned(),
            "--root-beam".to_owned(),
            "1".to_owned(),
            "--leaf-beam".to_owned(),
            "2".to_owned(),
            "--page-count".to_owned(),
            "2".to_owned(),
            "--k".to_owned(),
            "3".to_owned(),
            "--local-page-dir".to_owned(),
            directory.path().display().to_string(),
        ];
        let output = execute(parse_args(command).unwrap()).unwrap();
        assert_eq!(output.last(), Some(&b'\n'));
        let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["matches"][0]["source_ordinal"], 7);
        assert_eq!(value["work"]["get_count"], 2);
        assert_eq!(value["work"]["decoded_rows"], 4);
    }

    #[test]
    fn v27_s3_qualify_object_store_reads_only_the_selected_page_wave() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            store
                .put(
                    &ObjectPath::from("frozen/pages/aaa.arrow"),
                    PutPayload::from(Bytes::from_static(b"page-a")),
                )
                .await
                .unwrap();
            store
                .put(
                    &ObjectPath::from("frozen/pages/bbb.arrow"),
                    PutPayload::from(Bytes::from_static(b"page-b")),
                )
                .await
                .unwrap();
        });
        let pages = [page_identity(0, "aaa", 6), page_identity(1, "bbb", 6)];
        let bodies = ObjectPageStore {
            store,
            prefix: ObjectPath::from("frozen/pages"),
        }
        .read_wave(&pages)
        .unwrap();
        assert_eq!(bodies, [b"page-a".to_vec(), b"page-b".to_vec()]);
    }

    #[test]
    fn v27_s3_qualify_normalizes_the_angular_query_before_search() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("query.parquet");
        let mut query = [0.0; 96];
        query[3] = 2.0;
        write_query(&path, &query);
        let bytes = fs::read(&path).unwrap();
        let actual = read_query(
            &ArtifactArg {
                path,
                sha256: format!("{:x}", sha2::Sha256::digest(&bytes)),
                encoded_bytes: bytes.len() as u64,
            },
            0,
        )
        .unwrap();
        assert_eq!(actual[3], 1.0);
        assert_eq!(actual.iter().map(|value| value * value).sum::<f32>(), 1.0);
    }

    fn page_identity(ordinal: u32, sha256: &str, encoded_bytes: u64) -> borsuk::V27PageIdentity {
        borsuk::V27PageIdentity {
            ordinal,
            sha256: sha256.to_owned(),
            encoded_bytes,
            primary_rows: 1,
            replica_rows: 0,
        }
    }

    fn centroid(axis: usize, value: f32) -> [half::f16; 96] {
        let mut centroid = [half::f16::from_f32(0.0); 96];
        centroid[axis] = half::f16::from_f32(value);
        centroid
    }

    fn row(source_ordinal: u64, axis: usize, value: f32) -> V27PageRow {
        let mut vector = [0.0; 96];
        vector[axis] = value;
        V27PageRow {
            source_ordinal,
            vector,
        }
    }

    fn write_query(path: &std::path::Path, query: &[f32; 96]) {
        let child = Arc::new(Field::new("element", DataType::Float32, false));
        let values = Arc::new(Float32Array::from_iter_values(query.iter().copied()));
        let vectors = FixedSizeListArray::try_new(child.clone(), 96, values, None).unwrap();
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
