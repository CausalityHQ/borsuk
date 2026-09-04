//! Explicit authenticated local/S3 qualification boundary for the V30 page index.

use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    BorsukError, V27HierarchyArtifactIdentity, V27HierarchyArtifacts, V30Index,
    V30LayoutArtifactIdentity, V30LayoutArtifacts, V30PageStore, V30PqArtifactIdentity,
    V30PqArtifacts, V30Router, V30SearchArm, V30SearchResult,
};
use bytes::Bytes;
use futures_util::future::try_join_all;
use object_store::{ObjectStore, ObjectStoreExt, parse_url_opts, path::Path as ObjectPath};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
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
    manifest: ArtifactArg,
    artifact_dir: PathBuf,
    query: ArtifactArg,
    query_row: usize,
    root_beam: usize,
    leaf_beam: usize,
    candidate_depth: usize,
    page_count: usize,
    k: usize,
    page_source: PageSource,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskArtifact {
    encoded_bytes: u64,
    file: String,
    role: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskPqArtifact {
    dependencies: Vec<String>,
    encoded_bytes: u64,
    file: String,
    role: String,
    row_count: u64,
    sha256: String,
    width_bytes: u8,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskHierarchy {
    leaves: DiskArtifact,
    roots: DiskArtifact,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskLayout {
    leaf_ranges: DiskArtifact,
    page_ranges: DiskArtifact,
    source_rows: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskPq {
    artifacts: Vec<DiskPqArtifact>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiskManifest {
    hierarchy: DiskHierarchy,
    layout: DiskLayout,
    page_key_suffix: String,
    pq: DiskPq,
    schema_version: u8,
}

#[derive(Debug)]
struct Manifest {
    hierarchy: Vec<(String, V30LayoutArtifactIdentity)>,
    layout: Vec<(String, V30LayoutArtifactIdentity)>,
    pq: Vec<(String, V30PqArtifactIdentity)>,
    source_rows: u64,
    page_key_suffix: String,
}

fn argument_error(message: &str) -> String {
    format!("V30 qualifier arguments {message}")
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn canonical(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical(value)))
                .collect(),
        ),
        value => value,
    }
}

fn read_bytes(argument: &ArtifactArg, role: &str) -> borsuk::Result<Vec<u8>> {
    let bytes = fs::read(&argument.path).map_err(|source| BorsukError::Io {
        path: argument.path.clone(),
        source,
    })?;
    if bytes.len() as u64 != argument.encoded_bytes
        || format!("{:x}", Sha256::digest(&bytes)) != argument.sha256
    {
        return Err(invalid(&format!(
            "V30 qualifier {role} byte authority differs"
        )));
    }
    Ok(bytes)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\\')
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn disk_identity(
    artifact: DiskArtifact,
    role: &str,
) -> borsuk::Result<(String, V30LayoutArtifactIdentity)> {
    if artifact.role != role
        || artifact.encoded_bytes == 0
        || !valid_name(&artifact.file)
        || !valid_digest(&artifact.sha256)
    {
        return Err(invalid("V30 qualifier manifest artifact authority differs"));
    }
    Ok((
        artifact.file,
        V30LayoutArtifactIdentity {
            role: artifact.role,
            sha256: artifact.sha256,
            encoded_bytes: artifact.encoded_bytes,
        },
    ))
}

fn read_manifest(argument: &ArtifactArg) -> borsuk::Result<Manifest> {
    let bytes = read_bytes(argument, "manifest")?;
    if bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n') {
        return Err(invalid("V30 qualifier manifest canonical bytes differ"));
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("V30 qualifier manifest JSON differs"))?;
    let mut expected = serde_json::to_vec(&canonical(value.clone()))
        .map_err(|_| invalid("V30 qualifier manifest serialization failed"))?;
    expected.push(b'\n');
    if bytes != expected {
        return Err(invalid("V30 qualifier manifest canonical bytes differ"));
    }
    let disk: DiskManifest = serde_json::from_value(value)
        .map_err(|_| invalid("V30 qualifier manifest schema differs"))?;
    if disk.schema_version != 1
        || disk.page_key_suffix != ".arrow"
        || disk.layout.source_rows == 0
        || disk.pq.artifacts.len() != 5
    {
        return Err(invalid("V30 qualifier manifest constants differ"));
    }
    let hierarchy = vec![
        disk_identity(disk.hierarchy.roots, "v27-roots-arrow")?,
        disk_identity(disk.hierarchy.leaves, "v27-leaves-arrow")?,
    ];
    let layout = vec![
        disk_identity(disk.layout.leaf_ranges, "v30-leaf-ranges-arrow")?,
        disk_identity(disk.layout.page_ranges, "v30-page-ranges-parquet")?,
    ];
    let roles = [
        "pq24-codebook",
        "pq48-codebook",
        "pq-base-codes",
        "pq-fidelity",
        "pq-high-codes",
    ];
    let pq = disk
        .pq
        .artifacts
        .into_iter()
        .zip(roles)
        .map(|(artifact, role)| {
            if artifact.role != role
                || artifact.encoded_bytes == 0
                || artifact.row_count == 0
                || !valid_name(&artifact.file)
                || !valid_digest(&artifact.sha256)
                || artifact
                    .dependencies
                    .iter()
                    .any(|value| !valid_digest(value))
            {
                return Err(invalid("V30 qualifier PQ manifest authority differs"));
            }
            Ok((
                artifact.file,
                V30PqArtifactIdentity {
                    role: artifact.role,
                    sha256: artifact.sha256,
                    encoded_bytes: artifact.encoded_bytes,
                    row_count: artifact.row_count,
                    width_bytes: artifact.width_bytes,
                    dependencies: artifact.dependencies,
                },
            ))
        })
        .collect::<borsuk::Result<Vec<_>>>()?;
    Ok(Manifest {
        hierarchy,
        layout,
        pq,
        source_rows: disk.layout.source_rows,
        page_key_suffix: disk.page_key_suffix,
    })
}

struct LocalPageStore {
    directory: PathBuf,
    suffix: String,
}

impl V30PageStore for LocalPageStore {
    fn read_wave(&self, pages: &[borsuk::V27PageIdentity]) -> borsuk::Result<Vec<Vec<u8>>> {
        pages
            .iter()
            .map(|page| {
                let path = self
                    .directory
                    .join(format!("{}{}", page.sha256, self.suffix));
                fs::read(&path).map_err(|source| BorsukError::Io { path, source })
            })
            .collect()
    }
}

struct ObjectPageStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    suffix: String,
}

impl V30PageStore for ObjectPageStore {
    fn read_wave(&self, pages: &[borsuk::V27PageIdentity]) -> borsuk::Result<Vec<Vec<u8>>> {
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
                let path = ObjectPath::from(format!(
                    "{}/{}{}",
                    self.prefix.as_ref(),
                    page.sha256,
                    self.suffix
                ));
                async move { Ok::<_, BorsukError>(store.get(&path).await?.bytes().await?.to_vec()) }
            });
            try_join_all(reads).await
        })
    }
}

fn result_bytes(result: &V30SearchResult) -> borsuk::Result<Vec<u8>> {
    if result
        .matches
        .iter()
        .any(|item| !item.squared_distance.is_finite())
    {
        return Err(invalid("V30 qualifier result distance differs"));
    }
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
    let value = serde_json::json!({
        "claim_eligible": false,
        "matches": matches,
        "schema_version": 1,
        "work": {
            "decoded_rows": result.work.decoded_rows,
            "encoded_bytes": result.work.encoded_bytes,
            "get_count": result.work.get_count,
            "routing": {
                "candidates_retained": result.work.routing.candidates_retained,
                "codes_scanned": result.work.routing.codes_scanned,
                "leaves_scored": result.work.routing.leaves_scored,
                "pages_considered": result.work.routing.pages_considered,
                "roots_scored": result.work.routing.roots_scored,
                "selected_pages": result.work.routing.selected_pages,
            },
            "unique_rows": result.work.unique_rows,
        },
    });
    let mut bytes = serde_json::to_vec(&canonical(value))
        .map_err(|_| invalid("V30 qualifier result serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn read_resident(
    directory: &std::path::Path,
    file: &str,
    sha256: &str,
    encoded_bytes: u64,
) -> borsuk::Result<Vec<u8>> {
    read_bytes(
        &ArtifactArg {
            path: directory.join(file),
            sha256: sha256.to_owned(),
            encoded_bytes,
        },
        "resident artifact",
    )
}

fn query_schema() -> Schema {
    let child = Arc::new(Field::new("element", DataType::Float32, false));
    Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(child, 96),
        false,
    )])
}

fn read_query(argument: &ArtifactArg, query_row: usize) -> borsuk::Result<[f32; 96]> {
    let bytes = read_bytes(argument, "query")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
    if builder.schema().as_ref() != &query_schema() {
        return Err(invalid("V30 qualifier query Parquet schema differs"));
    }
    let mut offset = 0_usize;
    for batch in builder.build()? {
        let batch = batch?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V30 qualifier query nullability differs"));
        }
        if query_row < offset + batch.num_rows() {
            let vectors = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| invalid("V30 qualifier query vector type differs"))?;
            let values = vectors
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V30 qualifier query value type differs"))?;
            let start = (query_row - offset) * 96;
            let mut query: [f32; 96] = values.values()[start..start + 96]
                .try_into()
                .map_err(|_| invalid("V30 qualifier query dimension differs"))?;
            if query.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V30 qualifier query value differs"));
            }
            let norm = query
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt();
            if !norm.is_finite() || norm <= 0.0 {
                return Err(invalid("V30 qualifier query norm differs"));
            }
            for value in &mut query {
                *value = (f64::from(*value) / norm) as f32;
            }
            return Ok(query);
        }
        offset += batch.num_rows();
    }
    Err(invalid("V30 qualifier query row differs"))
}

fn run_search<S: V30PageStore>(
    router: V30Router,
    store: S,
    arm: V30SearchArm,
    query: &[f32; 96],
    k: usize,
) -> borsuk::Result<V30SearchResult> {
    V30Index::new(router, store, arm)?.search(query, k)
}

fn execute(args: Args) -> borsuk::Result<Vec<u8>> {
    let manifest = read_manifest(&args.manifest)?;
    let hierarchy_bytes = manifest
        .hierarchy
        .iter()
        .map(|(file, identity)| {
            read_resident(
                &args.artifact_dir,
                file,
                &identity.sha256,
                identity.encoded_bytes,
            )
        })
        .collect::<borsuk::Result<Vec<_>>>()?;
    let hierarchy = V27HierarchyArtifacts {
        roots: V27HierarchyArtifactIdentity {
            role: manifest.hierarchy[0].1.role.clone(),
            sha256: manifest.hierarchy[0].1.sha256.clone(),
            encoded_bytes: manifest.hierarchy[0].1.encoded_bytes,
        },
        leaves: V27HierarchyArtifactIdentity {
            role: manifest.hierarchy[1].1.role.clone(),
            sha256: manifest.hierarchy[1].1.sha256.clone(),
            encoded_bytes: manifest.hierarchy[1].1.encoded_bytes,
        },
        roots_bytes: hierarchy_bytes[0].clone(),
        leaves_bytes: hierarchy_bytes[1].clone(),
    };
    let pq_bytes = manifest
        .pq
        .iter()
        .map(|(file, identity)| {
            read_resident(
                &args.artifact_dir,
                file,
                &identity.sha256,
                identity.encoded_bytes,
            )
        })
        .collect::<borsuk::Result<Vec<_>>>()?;
    let pq = V30PqArtifacts {
        identities: manifest
            .pq
            .iter()
            .map(|(_, identity)| identity.clone())
            .collect(),
        bytes: pq_bytes,
    };
    let layout_bytes = manifest
        .layout
        .iter()
        .map(|(file, identity)| {
            read_resident(
                &args.artifact_dir,
                file,
                &identity.sha256,
                identity.encoded_bytes,
            )
        })
        .collect::<borsuk::Result<Vec<_>>>()?;
    let layout = V30LayoutArtifacts {
        source_rows: manifest.source_rows,
        leaf_ranges: manifest.layout[0].1.clone(),
        page_ranges: manifest.layout[1].1.clone(),
        leaf_ranges_arrow: layout_bytes[0].clone(),
        page_ranges_parquet: layout_bytes[1].clone(),
    };
    let router = V30Router::from_artifacts(&hierarchy, &pq, &layout)?;
    let query = read_query(&args.query, args.query_row)?;
    let arm = V30SearchArm {
        root_beam: args.root_beam,
        leaf_beam: args.leaf_beam,
        candidate_depth: args.candidate_depth,
        page_count: args.page_count,
    };
    let result = match args.page_source {
        PageSource::Local(directory) => run_search(
            router,
            LocalPageStore {
                directory,
                suffix: manifest.page_key_suffix,
            },
            arm,
            &query,
            args.k,
        )?,
        PageSource::S3(uri) => {
            let url = Url::parse(&uri).map_err(|_| invalid("V30 qualifier S3 URI differs"))?;
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
                ObjectPageStore {
                    store,
                    prefix,
                    suffix: manifest.page_key_suffix,
                },
                arm,
                &query,
                args.k,
            )?
        }
    };
    result_bytes(&result)
}

fn take(values: &mut BTreeMap<String, String>, name: &str) -> Result<String, String> {
    values
        .remove(name)
        .ok_or_else(|| argument_error(&format!("missing --{name}")))
}

fn number<T: std::str::FromStr>(
    values: &mut BTreeMap<String, String>,
    name: &str,
) -> Result<T, String> {
    take(values, name)?
        .parse()
        .map_err(|_| argument_error(&format!("--{name} type differs")))
}

fn artifact(values: &mut BTreeMap<String, String>, role: &str) -> Result<ArtifactArg, String> {
    let path_flag = if role == "query" {
        "query-parquet"
    } else {
        role
    };
    let artifact = ArtifactArg {
        path: PathBuf::from(take(values, path_flag)?),
        sha256: take(values, &format!("{role}-sha256"))?,
        encoded_bytes: number(values, &format!("{role}-bytes"))?,
    };
    if !artifact.path.is_absolute()
        || artifact.encoded_bytes == 0
        || artifact.sha256.len() != 64
        || artifact
            .sha256
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(argument_error(&format!("--{role} authority differs")));
    }
    Ok(artifact)
}

fn parse_args(arguments: Vec<String>) -> Result<Args, String> {
    let mut arguments = arguments.into_iter();
    arguments
        .next()
        .ok_or_else(|| argument_error("program is missing"))?;
    let mut execute = false;
    let mut values = BTreeMap::new();
    while let Some(flag) = arguments.next() {
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
        let value = arguments
            .next()
            .ok_or_else(|| argument_error(&format!("--{name} value is missing")))?;
        if values.insert(name.to_owned(), value).is_some() {
            return Err(argument_error(&format!("duplicate --{name}")));
        }
    }
    if !execute {
        return Err(argument_error("--execute is required"));
    }
    let manifest = artifact(&mut values, "manifest")?;
    let artifact_dir = PathBuf::from(take(&mut values, "artifact-dir")?);
    let query = artifact(&mut values, "query")?;
    let query_row = number(&mut values, "query-row")?;
    let root_beam = number(&mut values, "root-beam")?;
    let leaf_beam = number(&mut values, "leaf-beam")?;
    let candidate_depth = number(&mut values, "candidate-depth")?;
    let page_count = number(&mut values, "page-count")?;
    let k = number(&mut values, "k")?;
    let local = values.remove("local-page-dir").map(PathBuf::from);
    let s3 = values.remove("s3-page-prefix");
    let page_source = match (local, s3) {
        (Some(path), None) if path.is_absolute() => PageSource::Local(path),
        (None, Some(uri)) if uri.starts_with("s3://") && !uri.ends_with('/') => PageSource::S3(uri),
        _ => return Err(argument_error("exactly one page source is required")),
    };
    if !artifact_dir.is_absolute()
        || root_beam == 0
        || leaf_beam == 0
        || candidate_depth == 0
        || candidate_depth > 12_288
        || page_count == 0
        || page_count > 10
        || k == 0
        || k > 10
        || !values.is_empty()
    {
        return Err(argument_error("unknown flag or numeric bound differs"));
    }
    Ok(Args {
        manifest,
        artifact_dir,
        query,
        query_row,
        root_beam,
        leaf_beam,
        candidate_depth,
        page_count,
        k,
        page_source,
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use borsuk::{
        V27PageIdentity, V30Match, V30PageStore, V30RoutingWork, V30SearchResult, V30SearchWork,
    };
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{
        Args, ArtifactArg, LocalPageStore, PageSource, execute, parse_args, read_manifest,
        result_bytes,
    };

    fn arguments() -> Vec<String> {
        [
            "v30_s3_qualify",
            "--execute",
            "--manifest",
            "/tmp/manifest.json",
            "--manifest-sha256",
            &"1".repeat(64),
            "--manifest-bytes",
            "1234",
            "--artifact-dir",
            "/tmp/artifacts",
            "--query-parquet",
            "/tmp/query.parquet",
            "--query-sha256",
            &"2".repeat(64),
            "--query-bytes",
            "4003585",
            "--query-row",
            "0",
            "--root-beam",
            "8",
            "--leaf-beam",
            "64",
            "--candidate-depth",
            "12288",
            "--page-count",
            "10",
            "--k",
            "10",
            "--s3-page-prefix",
            "s3://frozen/pages",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
    }

    #[test]
    fn v30_s3_qualify_parser_requires_explicit_authority_and_one_page_source() {
        // Break caught: qualification discovers latest artifacts, accepts an ETag,
        // or silently switches between local/cache/S3 page bodies.
        let parsed = parse_args(arguments()).unwrap();
        assert_eq!(parsed.manifest.path, PathBuf::from("/tmp/manifest.json"));
        assert_eq!(parsed.manifest.sha256, "1".repeat(64));
        assert_eq!(parsed.query.sha256, "2".repeat(64));
        assert_eq!(parsed.query_row, 0);
        assert_eq!(parsed.page_count, 10);
        assert_eq!(
            parsed.page_source,
            PageSource::S3("s3://frozen/pages".to_owned())
        );

        for forbidden in [
            "--latest",
            "--etag",
            "--version",
            "--legacy",
            "--d3",
            "--page-bucket",
        ] {
            let mut values = arguments();
            values.extend([forbidden.to_owned(), "value".to_owned()]);
            assert!(parse_args(values).is_err(), "accepted {forbidden}");
        }

        let mut missing_execute = arguments();
        missing_execute.remove(1);
        assert!(parse_args(missing_execute).is_err());

        let mut both_sources = arguments();
        both_sources.extend(["--local-page-dir".to_owned(), "/tmp/pages".to_owned()]);
        assert!(parse_args(both_sources).is_err());
    }

    fn manifest_bytes() -> Vec<u8> {
        let artifact = |role: &str, file: &str, digit: char| {
            format!(
                r#"{{"encoded_bytes":123,"file":"{file}","role":"{role}","sha256":"{}"}}"#,
                digit.to_string().repeat(64)
            )
        };
        let pq = |role: &str,
                  file: &str,
                  digit: char,
                  row_count: u64,
                  width_bytes: u8,
                  dependencies: &str| {
            format!(
                r#"{{"dependencies":{dependencies},"encoded_bytes":123,"file":"{file}","role":"{role}","row_count":{row_count},"sha256":"{}","width_bytes":{width_bytes}}}"#,
                digit.to_string().repeat(64)
            )
        };
        let base_sha = "3".repeat(64);
        let high_sha = "4".repeat(64);
        format!(
            concat!(
                "{{\"hierarchy\":{{\"leaves\":{},\"roots\":{}}},",
                "\"layout\":{{\"leaf_ranges\":{},\"page_ranges\":{},\"source_rows\":40}},",
                "\"page_key_suffix\":\".arrow\",",
                "\"pq\":{{\"artifacts\":[{},{},{},{},{}]}},",
                "\"schema_version\":1}}\n"
            ),
            artifact("v27-leaves-arrow", "leaves.arrow", '2'),
            artifact("v27-roots-arrow", "roots.arrow", '1'),
            artifact("v30-leaf-ranges-arrow", "leaf-ranges.arrow", '8'),
            artifact("v30-page-ranges-parquet", "page-ranges.parquet", '9'),
            pq("pq24-codebook", "pq24.arrow", '3', 1, 24, "[]"),
            pq("pq48-codebook", "pq48.arrow", '4', 1, 48, "[]"),
            pq(
                "pq-base-codes",
                "base.arrow",
                '5',
                38,
                24,
                &format!(r#"["{base_sha}"]"#),
            ),
            pq(
                "pq-fidelity",
                "fidelity.arrow",
                '6',
                40,
                0,
                &format!(r#"["{base_sha}","{high_sha}"]"#),
            ),
            pq(
                "pq-high-codes",
                "high.arrow",
                '7',
                2,
                48,
                &format!(r#"["{high_sha}"]"#),
            ),
        )
        .into_bytes()
    }

    #[test]
    fn v30_s3_qualify_manifest_binds_every_resident_artifact_before_use() {
        // Break caught: serving combines hierarchy, PQ, and layout objects from
        // different constructions or discovers an artifact name from storage.
        let directory = tempdir().unwrap();
        let path = directory.path().join("manifest.json");
        let bytes = manifest_bytes();
        fs::write(&path, &bytes).unwrap();
        let argument = ArtifactArg {
            path,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            encoded_bytes: bytes.len() as u64,
        };
        let manifest = read_manifest(&argument).unwrap();
        assert_eq!(manifest.source_rows, 40);
        assert_eq!(manifest.hierarchy[0].1.role, "v27-roots-arrow");
        assert_eq!(manifest.pq.len(), 5);
        assert_eq!(manifest.layout[1].1.role, "v30-page-ranges-parquet");

        let mut corrupted = argument.clone();
        corrupted.sha256.replace_range(0..1, "f");
        assert!(read_manifest(&corrupted).is_err());

        let drifted = bytes
            .windows(b"pq24-codebook".len())
            .position(|window| window == b"pq24-codebook")
            .unwrap();
        let mut drifted_bytes = bytes;
        drifted_bytes[drifted] = b'x';
        fs::write(&argument.path, &drifted_bytes).unwrap();
        let drifted_argument = ArtifactArg {
            sha256: format!("{:x}", Sha256::digest(&drifted_bytes)),
            encoded_bytes: drifted_bytes.len() as u64,
            ..argument
        };
        assert!(read_manifest(&drifted_argument).is_err());
    }

    #[test]
    fn v30_s3_qualify_local_store_and_stdout_are_content_addressed() {
        // Break caught: the qualifier discovers page names, performs more than
        // one wave, or emits noncanonical/claim-eligible output.
        let directory = tempdir().unwrap();
        let page = V27PageIdentity {
            ordinal: 7,
            sha256: "a".repeat(64),
            encoded_bytes: 3,
            primary_rows: 1,
            replica_rows: 0,
        };
        fs::write(
            directory.path().join(format!("{}.arrow", page.sha256)),
            b"abc",
        )
        .unwrap();
        let bodies = LocalPageStore {
            directory: directory.path().to_path_buf(),
            suffix: ".arrow".to_owned(),
        }
        .read_wave(std::slice::from_ref(&page))
        .unwrap();
        assert_eq!(bodies, vec![b"abc".to_vec()]);

        let result = V30SearchResult {
            matches: vec![V30Match {
                source_ordinal: 9,
                squared_distance: 0.25,
            }],
            work: V30SearchWork {
                routing: V30RoutingWork {
                    roots_scored: 16,
                    leaves_scored: 64,
                    codes_scanned: 40,
                    candidates_retained: 12,
                    pages_considered: 3,
                    selected_pages: 1,
                },
                get_count: 1,
                encoded_bytes: 3,
                decoded_rows: 1,
                unique_rows: 1,
            },
        };
        assert_eq!(
            String::from_utf8(result_bytes(&result).unwrap()).unwrap(),
            concat!(
                "{\"claim_eligible\":false,\"matches\":[{\"source_ordinal\":9,",
                "\"squared_distance\":0.25}],\"schema_version\":1,\"work\":{",
                "\"decoded_rows\":1,\"encoded_bytes\":3,\"get_count\":1,",
                "\"routing\":{\"candidates_retained\":12,\"codes_scanned\":40,",
                "\"leaves_scored\":64,\"pages_considered\":3,\"roots_scored\":16,",
                "\"selected_pages\":1},\"unique_rows\":1}}\n"
            )
        );
    }

    #[test]
    fn v30_s3_qualify_execution_reads_only_explicit_authenticated_artifacts() {
        // Break caught: the executable bypasses the serving manifest or begins
        // page access before all resident artifacts and the query authenticate.
        let directory = tempdir().unwrap();
        let manifest_path = directory.path().join("manifest.json");
        let bytes = manifest_bytes();
        fs::write(&manifest_path, &bytes).unwrap();
        let args = Args {
            manifest: ArtifactArg {
                path: manifest_path,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                encoded_bytes: bytes.len() as u64,
            },
            artifact_dir: directory.path().to_path_buf(),
            query: ArtifactArg {
                path: directory.path().join("query.parquet"),
                sha256: "f".repeat(64),
                encoded_bytes: 1,
            },
            query_row: 0,
            root_beam: 1,
            leaf_beam: 1,
            candidate_depth: 1,
            page_count: 1,
            k: 1,
            page_source: PageSource::Local(directory.path().to_path_buf()),
        };
        let error = execute(args).unwrap_err().to_string();
        assert!(error.contains("roots.arrow"), "unexpected error: {error}");
    }
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
                eprintln!("v30_s3_qualify: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("v30_s3_qualify: {error}");
            std::process::exit(1);
        }
    }
}
