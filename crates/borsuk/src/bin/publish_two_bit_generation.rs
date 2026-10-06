//! Publish one authenticated local two-bit generation as a fresh LocalFileSystem head.
use borsuk::{
    two_bit_generation::TwoBitGenerationLimits,
    two_bit_store::{publish_two_bit_generation, read_two_bit_head},
};
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const CONFIG_CAP: u64 = 65_536;
const CONFIG_SCHEMA: &str = "borsuk-two-bit-local-publication-config-v1";
const RECEIPT_SCHEMA: &str = "borsuk-two-bit-local-publication-receipt-v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema: String,
    root: Root,
    store_root: PathBuf,
    prefix: String,
    limits: Limits,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    path: PathBuf,
    sha256: String,
}
// Every field is mandatory: the caller states the admission, nothing is defaulted.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    max_memory_bytes: u64,
    max_active_queries: usize,
    max_query_bytes: usize,
    max_query_gets: usize,
    max_parallel_gets: usize,
    max_source_bytes: usize,
    max_source_gets: usize,
    max_parallel_source_gets: usize,
    max_query_scratch_bytes: usize,
    already_pinned_bytes: u64,
}
impl From<&Limits> for TwoBitGenerationLimits {
    fn from(l: &Limits) -> Self {
        Self {
            max_memory_bytes: l.max_memory_bytes,
            max_active_queries: l.max_active_queries,
            max_query_bytes: l.max_query_bytes,
            max_query_gets: l.max_query_gets,
            max_parallel_gets: l.max_parallel_gets,
            max_source_bytes: l.max_source_bytes,
            max_source_gets: l.max_source_gets,
            max_parallel_source_gets: l.max_parallel_source_gets,
            max_query_scratch_bytes: l.max_query_scratch_bytes,
            already_pinned_bytes: l.already_pinned_bytes,
        }
    }
}

fn sha_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn is_sha(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}
fn config(path: &Path, trusted_sha: &str) -> Result<Config> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(CONFIG_CAP + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > CONFIG_CAP || sha_hex(&bytes) != trusted_sha {
        return Err("config identity or cap".into());
    }
    let c: Config = serde_json::from_slice(&bytes)?;
    if c.schema != CONFIG_SCHEMA {
        return Err("config schema".into());
    }
    if !is_sha(&c.root.sha256) {
        return Err("invalid root sha".into());
    }
    if !c.store_root.is_dir() {
        return Err("store_root not a directory".into());
    }
    Ok(c)
}
/// Same-directory temp file, fsync, then hard link: atomic and never overwrites.
fn write_receipt(path: &Path, body: &[u8]) -> Result<()> {
    let parent = receipt_parent(path)?;
    let name = path.file_name().ok_or("receipt name")?.to_string_lossy();
    let tmp = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(body)?;
    file.sync_all()?;
    drop(file);
    let linked = fs::hard_link(&tmp, path);
    fs::remove_file(&tmp)?;
    linked.map_err(|_| "receipt exists")?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
fn receipt_parent(path: &Path) -> Result<&Path> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !parent.is_dir() {
        return Err("receipt parent not a directory".into());
    }
    Ok(parent)
}
async fn publish(config_path: &Path, config_sha: &str, receipt: &Path) -> Result<()> {
    let c = config(config_path, config_sha)?;
    receipt_parent(receipt)?;
    if receipt.symlink_metadata().is_ok() {
        return Err("receipt exists".into());
    }
    let prefix = ObjectPath::parse(&c.prefix)?;
    if prefix.as_ref().is_empty() {
        return Err("invalid prefix".into());
    }
    let store = LocalFileSystem::new_with_prefix(&c.store_root)?;
    if read_two_bit_head(&store, &prefix).await?.is_some() {
        return Err("head not fresh".into());
    }
    let published = publish_two_bit_generation(
        &store,
        &prefix,
        &c.root.path,
        &c.root.sha256,
        (&c.limits).into(),
        None,
    )
    .await?;
    let head = read_two_bit_head(&store, &prefix)
        .await?
        .ok_or("published head missing")?;
    if head.root_sha256() != c.root.sha256
        || head.root_sha256() != published.root_sha256()
        || head.generation() != published.generation()
        || head.control_epoch() != published.control_epoch()
        || head.metadata_prefix() != published.metadata_prefix()
    {
        return Err("published head authority changed".into());
    }
    let body = serde_json::to_vec_pretty(&json!({
        "schema": RECEIPT_SCHEMA,
        "config_sha256": config_sha,
        "prefix": c.prefix,
        "metadata_prefix": head.metadata_prefix().to_string(),
        "root_sha256": head.root_sha256(),
        "generation": head.generation(),
        "control_epoch": head.control_epoch(),
    }))?;
    write_receipt(receipt, &body)
}
fn run(args: &[String]) -> Result<()> {
    if args.len() != 4 {
        return Err("usage: publish_two_bit_generation CONFIG CONFIG_SHA NEW_RECEIPT".into());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(publish(Path::new(&args[1]), &args[2], Path::new(&args[3])))
}
fn main() {
    if let Err(e) = run(&std::env::args().collect::<Vec<_>>()) {
        eprintln!("INVALID: {e}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borsuk::{
        two_bit_build::TwoBitGenerationBuilder, two_bit_generation::DiscoveryMode,
        two_bit_source::TwoBitSource,
    };
    use object_store::ObjectStoreExt;
    use serde_json::Value;

    const ROWS: usize = 257;
    const D: usize = 768;

    /// Real builder output plus the SQ8 object staged in a real LocalFileSystem store.
    async fn fixture() -> (tempfile::TempDir, Value) {
        let dir = tempfile::tempdir().unwrap();
        let store_root = dir.path().join("store");
        fs::create_dir(&store_root).unwrap();
        let (mut raw, mut sq8) = (Vec::new(), Vec::new());
        for row in 0..ROWS {
            let value = (1 + row % 7) as u8;
            for _ in 0..D {
                raw.extend_from_slice(&(value as f32).to_le_bytes());
            }
            sq8.extend_from_slice(&((ROWS - row + 1000) as i64).to_le_bytes());
            sq8.extend_from_slice(&(D as f32 * (value as f32).powi(2)).to_le_bytes());
            sq8.extend(std::iter::repeat_n(value, D));
        }
        let (raw_path, sq8_path) = (dir.path().join("raw"), dir.path().join("sq8"));
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let sq8_sha = sha_hex(&sq8);
        let key = ObjectPath::from(format!("semantic/objects/{sq8_sha}"));
        let store = LocalFileSystem::new_with_prefix(&store_root).unwrap();
        store.put(&key, sq8.into()).await.unwrap();
        let etag = store.head(&key).await.unwrap().e_tag.unwrap();
        let root = dir.path().join("generation");
        let order = (0..ROWS as u64).collect::<Vec<_>>();
        let root_sha = TwoBitGenerationBuilder {
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &sha_hex(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows: ROWS,
                dimensions: D,
            },
            base_epoch: 0,
            generation: 1,
            low: &[0.; D],
            step: &[1.; D],
            sq8_object_key: key.as_ref(),
            sq8_etag: &etag,
        }
        .build_with_discovery(Some(&order), DiscoveryMode::Semantic, &root, 128_000_000)
        .unwrap();
        let config = json!({
            "schema": CONFIG_SCHEMA,
            "root": {"path": root, "sha256": root_sha},
            "store_root": store_root,
            "prefix": "semantic/index",
            "limits": {
                "max_memory_bytes": 512 * 1024 * 1024,
                "max_active_queries": 1,
                "max_query_bytes": 16_773_120,
                "max_query_gets": 32,
                "max_parallel_gets": 16,
                "max_source_bytes": 64 * 1024 * 1024,
                "max_source_gets": 128,
                "max_parallel_source_gets": 16,
                "max_query_scratch_bytes": 400_000,
                "already_pinned_bytes": 0,
            },
        });
        (dir, config)
    }
    fn write_config(dir: &Path, value: &Value) -> (PathBuf, String) {
        let path = dir.join("config.json");
        let body = serde_json::to_vec(value).unwrap();
        fs::write(&path, &body).unwrap();
        (path, sha_hex(&body))
    }
    async fn head_exists(c: &Value) -> bool {
        let store = LocalFileSystem::new_with_prefix(c["store_root"].as_str().unwrap()).unwrap();
        let prefix = ObjectPath::from(c["prefix"].as_str().unwrap());
        read_two_bit_head(&store, &prefix).await.unwrap().is_some()
    }

    #[tokio::test]
    async fn publishes_real_generation_and_receipt_then_refuses_replay() {
        let (dir, c) = fixture().await;
        let (path, sha) = write_config(dir.path(), &c);
        let receipt = dir.path().join("receipt.json");
        // An existing receipt is refused before anything is published.
        fs::write(&receipt, b"x").unwrap();
        assert!(publish(&path, &sha, &receipt).await.is_err());
        assert!(!head_exists(&c).await);
        fs::remove_file(&receipt).unwrap();

        publish(&path, &sha, &receipt).await.unwrap();
        assert!(head_exists(&c).await);
        let body: Value = serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
        assert_eq!(body["schema"], RECEIPT_SCHEMA);
        assert_eq!(body["config_sha256"], sha);
        assert_eq!(body["root_sha256"], c["root"]["sha256"]);
        assert_eq!(body["generation"], 1);
        assert_eq!(body["prefix"], "semantic/index");
        assert!(
            body["metadata_prefix"]
                .as_str()
                .unwrap()
                .starts_with("semantic/")
        );
        // Independent readback of the head against the receipt and config.
        let store = LocalFileSystem::new_with_prefix(c["store_root"].as_str().unwrap()).unwrap();
        let head = read_two_bit_head(&store, &ObjectPath::from("semantic/index"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.root_sha256(), c["root"]["sha256"].as_str().unwrap());
        assert_eq!(head.generation(), 1);
        assert_eq!(
            head.control_epoch(),
            body["control_epoch"].as_u64().unwrap()
        );
        assert_eq!(head.metadata_prefix().to_string(), body["metadata_prefix"]);

        // Replay: the head is no longer fresh, and the receipt is never overwritten.
        let first = fs::read(&receipt).unwrap();
        let again = dir.path().join("again.json");
        let error = publish(&path, &sha, &again).await.unwrap_err();
        assert_eq!(error.to_string(), "head not fresh");
        assert!(!again.exists());
        assert!(publish(&path, &sha, &receipt).await.is_err());
        assert_eq!(fs::read(&receipt).unwrap(), first);
    }

    #[tokio::test]
    async fn rejects_wrong_root_cap_existing_head_and_bad_config() {
        let (dir, good) = fixture().await;
        let receipt = dir.path().join("receipt.json");
        let mut wrong_root = good.clone();
        wrong_root["root"]["sha256"] = json!("0".repeat(64));
        let mut capped = good.clone();
        capped["limits"]["max_memory_bytes"] = json!(131_071);
        for bad in [wrong_root, capped] {
            let (path, sha) = write_config(dir.path(), &bad);
            assert!(publish(&path, &sha, &receipt).await.is_err());
            assert!(!receipt.exists());
            assert!(!head_exists(&good).await);
        }

        // Config identity, cap, schema, unknown/missing fields and sha shape.
        let (path, sha) = write_config(dir.path(), &good);
        assert!(config(&path, &sha).is_ok());
        assert!(config(&path, &"0".repeat(64)).is_err());
        let mut edits: Vec<Value> = Vec::new();
        for (field, value) in [
            ("unknown", json!(1)),
            ("schema", json!("other")),
            ("prefix", json!("../index")),
            ("store_root", json!(dir.path().join("missing"))),
        ] {
            let mut v = good.clone();
            v[field] = value;
            edits.push(v);
        }
        let mut v = good.clone();
        v["root"]["sha256"] = json!("A".repeat(64));
        edits.push(v);
        let mut v = good.clone();
        v["limits"]
            .as_object_mut()
            .unwrap()
            .remove("max_source_gets");
        edits.push(v);
        for v in edits {
            let (path, sha) = write_config(dir.path(), &v);
            assert!(config(&path, &sha).is_err() || publish(&path, &sha, &receipt).await.is_err());
            assert!(!receipt.exists());
        }
        fs::write(&path, [b' '; 65_537]).unwrap();
        assert!(config(&path, &sha_hex(&[b' '; 65_537])).is_err());

        // A published head blocks a second publication to the same prefix.
        let (path, sha) = write_config(dir.path(), &good);
        publish(&path, &sha, &receipt).await.unwrap();
        let other = dir.path().join("other.json");
        assert!(publish(&path, &sha, &other).await.is_err());
        assert!(!other.exists());
    }
}
