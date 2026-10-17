//! Assemble approved source/SQ8 snapshots or repackage authenticated flat source layouts.
use borsuk::{
    semantic_unit_router::SemanticProfile,
    two_bit_build::{TwoBitGenerationBuilder, repackage_source_layout},
    two_bit_generation::{DiscoveryMode, SourceLayoutMode, TwoBitGenerationLimits},
    two_bit_source::TwoBitSource,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::Read,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    discovery: DiscoveryMode,
    semantic_profile: Option<SemanticProfile>,
    order: Option<Order>,
    raw: PathBuf,
    raw_sha256: String,
    sq8: PathBuf,
    sq8_sha256: String,
    rows: usize,
    dimensions: usize,
    generation: u64,
    base_epoch: u64,
    low: Vec<f32>,
    step: Vec<f32>,
    sq8_object_key: String,
    sq8_etag: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Order {
    path: PathBuf,
    sha256: String,
}
fn read_order(order: &Order, rows: usize, cap: usize) -> Result<Vec<u64>, Box<dyn Error>> {
    let bytes = rows.checked_mul(8).ok_or("order geometry")?;
    if bytes.checked_mul(2).is_none_or(|n| n > cap) {
        return Err("order payload admission".into());
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
        .open(&order.path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() != bytes as u64 {
        return Err("order exact length/type".into());
    }
    let mut body = vec![0; bytes];
    file.read_exact(&mut body)?;
    if file.read(&mut [0])? != 0 || format!("{:x}", Sha256::digest(&body)) != order.sha256 {
        return Err("order EOF/identity".into());
    }
    Ok(body
        .chunks_exact(8)
        .map(|word| u64::from_le_bytes(word.try_into().unwrap()))
        .collect())
}
fn config(path: &Path, trusted_sha: &str) -> Result<Config, Box<dyn Error>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() > 65_536 || format!("{:x}", Sha256::digest(&bytes)) != trusted_sha {
        return Err("builder config identity or cap".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn positive_u64(value: &str) -> Result<u64, Box<dyn Error>> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("expected positive decimal integer".into());
    }
    let value = value.parse::<u64>()?;
    if value == 0 {
        return Err("expected positive decimal integer".into());
    }
    Ok(value)
}
fn main() -> Result<(), Box<dyn Error>> {
    let sha = run(&std::env::args().collect::<Vec<_>>())?;
    println!("{sha}");
    Ok(())
}
fn run(args: &[String]) -> Result<String, Box<dyn Error>> {
    if args.get(1).is_some_and(|arg| arg == "--repackage") {
        if args.len() != 7 {
            return Err("usage: build_two_bit_generation --repackage FLAT_DIR EXPECTED_ROOT_SHA NEW_GENERATION MAX_MEMORY_BYTES NEW_OUTPUT".into());
        }
        if args[3].len() != 64
            || !args[3]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("expected lowercase root SHA256".into());
        }
        let generation = positive_u64(&args[4])?;
        let max_memory_bytes = positive_u64(&args[5])?;
        // Publication uses the library's full model with minimal nonzero query allowances.
        let limits = TwoBitGenerationLimits {
            max_memory_bytes,
            max_active_queries: 1,
            max_query_bytes: 1,
            max_query_gets: 1,
            max_parallel_gets: 1,
            max_source_bytes: 1,
            max_source_gets: 1,
            max_parallel_source_gets: 1,
            max_query_scratch_bytes: 1,
            already_pinned_bytes: 0,
        };
        return Ok(repackage_source_layout(
            Path::new(&args[2]),
            &args[3],
            Path::new(&args[6]),
            generation,
            SourceLayoutMode::LeafCapsulesV2,
            limits,
        )?);
    }
    if args.len() != 5 {
        return Err(
            "usage: build_two_bit_generation CONFIG CONFIG_SHA MAX_MEMORY_BYTES NEW_OUTPUT".into(),
        );
    }
    let input = config(Path::new(&args[1]), &args[2])?;
    if input.discovery == DiscoveryMode::Semantic
        && input
            .semantic_profile
            .is_none_or(|profile| !profile.valid_geometry(input.rows, input.dimensions))
    {
        return Err("semantic profile/geometry required".into());
    }
    let cap: usize = args[3].parse()?;
    let order = input
        .order
        .as_ref()
        .map(|order| read_order(order, input.rows, cap))
        .transpose()?;
    let builder = TwoBitGenerationBuilder {
        base_epoch: input.base_epoch,
        source: TwoBitSource {
            raw: &input.raw,
            raw_sha256: &input.raw_sha256,
            sq8: &input.sq8,
            sq8_sha256: &input.sq8_sha256,
            rows: input.rows,
            dimensions: input.dimensions,
        },
        generation: input.generation,
        low: &input.low,
        step: &input.step,
        sq8_object_key: &input.sq8_object_key,
        sq8_etag: &input.sq8_etag,
    };
    let output = Path::new(&args[4]);
    let sha = match (input.discovery, input.semantic_profile) {
        (DiscoveryMode::Graph, None) => {
            builder.build_with_discovery(order.as_deref(), DiscoveryMode::Graph, output, cap)?
        }
        (DiscoveryMode::Semantic, Some(profile)) => {
            builder.build_with_semantic_profile(order.as_deref(), profile, output, cap)?
        }
        _ => return Err("explicit discovery/profile agreement required".into()),
    };
    Ok(sha)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn order_authentication_charges_both_live_buffers_before_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("order");
        let body = 0_u64.to_le_bytes();
        std::fs::write(&path, body).unwrap();
        let order = Order {
            path,
            sha256: format!("{:x}", Sha256::digest(body)),
        };
        assert_eq!(read_order(&order, 1, 16).unwrap(), [0]);
        assert!(read_order(&order, 1, 15).is_err());
        assert!(read_order(&order, usize::MAX, usize::MAX).is_err());
    }

    #[test]
    fn config_rejects_wrong_hash_oversize_and_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.json");
        let valid = br#"{"discovery":"graph","semantic_profile":null,"order":null,"raw":"raw","raw_sha256":"a","sq8":"sq8","sq8_sha256":"b","rows":1,"dimensions":2,"generation":1,"base_epoch":0,"low":[0,0],"step":[1,1],"sq8_object_key":"key","sq8_etag":"etag"}"#;
        std::fs::write(&file, valid).unwrap();
        let sha = format!("{:x}", Sha256::digest(valid));
        assert!(config(&file, &sha).is_ok());
        assert!(config(&file, &"0".repeat(64)).is_err());
        let mut value: serde_json::Value = serde_json::from_slice(valid).unwrap();
        value["truth"] = serde_json::json!("forbidden");
        let body = serde_json::to_vec(&value).unwrap();
        std::fs::write(&file, &body).unwrap();
        assert!(config(&file, &format!("{:x}", Sha256::digest(&body))).is_err());
        let body = vec![b' '; 65_537];
        std::fs::write(&file, &body).unwrap();
        assert!(config(&file, &format!("{:x}", Sha256::digest(&body))).is_err());
    }

    #[test]
    fn repackage_arguments_are_strict_before_io() {
        assert_eq!(positive_u64("1").unwrap(), 1);
        assert_eq!(positive_u64("18446744073709551615").unwrap(), u64::MAX);
        let valid = vec![
            "build_two_bit_generation".into(),
            "--repackage".into(),
            "missing".into(),
            "a".repeat(64),
            "2".into(),
            "134217728".into(),
            "output".into(),
        ];
        for count in 2..valid.len() {
            assert!(
                run(&valid[..count])
                    .unwrap_err()
                    .to_string()
                    .contains("usage:")
            );
        }
        let mut extra = valid.clone();
        extra.push("extra".into());
        assert!(run(&extra).unwrap_err().to_string().contains("usage:"));
        for sha in [
            "a".repeat(63),
            "a".repeat(65),
            "G".repeat(64),
            "A".repeat(64),
        ] {
            let mut args = valid.clone();
            args[3] = sha;
            assert!(run(&args).unwrap_err().to_string().contains("root SHA"));
        }
        for index in [4, 5] {
            for value in [
                "0",
                "-1",
                "+1",
                " 1",
                "1 ",
                "1.0",
                "",
                "18446744073709551616",
            ] {
                assert!(positive_u64(value).is_err(), "{value}");
                let mut args = valid.clone();
                args[index] = value.into();
                assert!(run(&args).is_err(), "{index}: {value}");
            }
        }
    }

    #[test]
    fn repackage_current_flat_preserves_authority_and_refuses_bad_inputs() {
        use borsuk::two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits};
        use object_store::{ObjectStoreExt, memory::InMemory, path::Path as ObjectPath};
        let dir = tempfile::tempdir().unwrap();
        let rows = 65_usize;
        let dimensions = 257_usize;
        let cap = 128 * 1024 * 1024;
        let hash = |body: &[u8]| format!("{:x}", Sha256::digest(body));
        let order = (0..rows as u64).rev().collect::<Vec<_>>();
        let values = |row: usize| {
            (0..dimensions)
                .map(|d| (1 + (row + 3 * d) % 13) as u8)
                .collect::<Vec<_>>()
        };
        let mut raw = Vec::new();
        for row in 0..rows {
            raw.extend(values(row).iter().flat_map(|v| f32::from(*v).to_le_bytes()));
        }
        let mut sq8 = Vec::new();
        for &row in &order {
            let vector = values(row as usize);
            let norm = vector.iter().map(|v| f32::from(*v).powi(2)).sum::<f32>();
            sq8.extend_from_slice(&(1000 + row as i64).to_le_bytes());
            sq8.extend_from_slice(&norm.to_le_bytes());
            sq8.extend_from_slice(&vector);
        }
        let order_body = order
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>();
        let raw_path = dir.path().join("raw");
        let sq8_path = dir.path().join("sq8");
        let order_path = dir.path().join("order");
        std::fs::write(&raw_path, &raw).unwrap();
        std::fs::write(&sq8_path, &sq8).unwrap();
        std::fs::write(&order_path, &order_body).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = InMemory::new();
            let key = ObjectPath::from(format!("test/objects/{}", hash(&sq8)));
            store.put(&key, sq8.clone().into()).await.unwrap();
            let etag = store.head(&key).await.unwrap().e_tag.unwrap();
            let body = serde_json::to_vec(&serde_json::json!({
                "discovery": "semantic", "semantic_profile": "scale1m",
                "order": {"path": order_path, "sha256": hash(&order_body)},
                "raw": raw_path, "raw_sha256": hash(&raw),
                "sq8": sq8_path, "sq8_sha256": hash(&sq8),
                "rows": rows, "dimensions": dimensions, "generation": 1, "base_epoch": 0,
                "low": vec![0.; dimensions], "step": vec![1.; dimensions],
                "sq8_object_key": key.as_ref(), "sq8_etag": etag,
            }))
            .unwrap();
            let config_path = dir.path().join("config.json");
            std::fs::write(&config_path, &body).unwrap();
            let current = dir.path().join("flat");
            let root_sha = run(&[
                "build_two_bit_generation".into(),
                config_path.display().to_string(),
                hash(&body),
                cap.to_string(),
                current.display().to_string(),
            ])
            .unwrap();
            let unchanged = [
                "manifest.json",
                "plane/manifest.json",
                "plane/records.bin",
                "plane/mean.bin",
                "plane/page_digests.bin",
                "router/root.bin",
                "router/membership.bin",
                "router/leaves.bin",
                "canonical.bin",
                "centroids.bin",
                "page_digests.bin",
                "page_manifest.json",
            ]
            .map(|name| (name, std::fs::read(current.join(name)).unwrap()));
            let output = dir.path().join("capsules");
            let args = vec![
                "build_two_bit_generation".into(),
                "--repackage".into(),
                current.display().to_string(),
                root_sha.clone(),
                "2".into(),
                cap.to_string(),
                output.display().to_string(),
            ];
            // One byte below the library's mandatory metadata/copy admission.
            let short_cap = 3 * 65536 * 8 + 65536 + 131072 - 1;
            for (index, value) in [
                (3, "0".repeat(64)),
                (4, "1".into()),
                (5, short_cap.to_string()),
            ] {
                let mut bad = args.clone();
                bad[index] = value;
                assert!(run(&bad).is_err());
                assert!(!output.exists());
            }
            std::fs::create_dir(&output).unwrap();
            std::fs::write(output.join("foreign-scratch"), b"keep").unwrap();
            assert!(run(&args).is_err());
            assert_eq!(
                std::fs::read(output.join("foreign-scratch")).unwrap(),
                b"keep"
            );
            assert!(!output.join("manifest.json").exists());
            // The CLI must leave failed destinations alone; use a fresh destination.
            let mut args = args;
            let output = dir.path().join("capsules-new");
            args[6] = output.display().to_string();
            let new_sha = run(&args).unwrap();
            assert_ne!(new_sha, root_sha);
            assert_eq!(
                hash(&std::fs::read(output.join("manifest.json")).unwrap()),
                new_sha
            );
            assert!(!output.join("plane/records.bin").exists());
            for (name, bytes) in &unchanged {
                assert_eq!(std::fs::read(current.join(name)).unwrap(), *bytes, "{name}");
                if !matches!(
                    *name,
                    "manifest.json" | "plane/records.bin" | "page_manifest.json"
                ) {
                    assert_eq!(std::fs::read(output.join(name)).unwrap(), *bytes, "{name}");
                }
            }
            let read_root = |path: &Path| -> serde_json::Value {
                serde_json::from_slice(&std::fs::read(path.join("manifest.json")).unwrap()).unwrap()
            };
            let before = read_root(&current);
            let after = read_root(&output);
            assert_eq!(after["schema"], "borsuk-two-bit-generation-v11");
            assert_eq!(after["generation"], 2);
            assert_eq!(after["source_layout"]["layout"], "leaf_capsules_v2");
            for field in [
                "discovery",
                "canonical",
                "plane_manifest_sha256",
                "sq8_object_key",
                "sq8_object_sha256",
                "sq8_etag",
                "low",
                "step",
                "base_epoch",
            ] {
                assert!(!before[field].is_null(), "{field}");
                assert_eq!(after[field], before[field], "{field}");
            }
            assert!(run(&args).is_err());
            assert_eq!(read_root(&output), after);
            let limits = TwoBitGenerationLimits {
                max_memory_bytes: cap,
                max_active_queries: 1,
                max_query_bytes: sq8.len(),
                max_query_gets: 32,
                max_parallel_gets: 1,
                max_source_bytes: 1024 * 1024,
                max_source_gets: 128,
                max_parallel_source_gets: 1,
                max_query_scratch_bytes: 1024 * 1024,
                already_pinned_bytes: 0,
            };
            let flat = TwoBitGeneration::open(&current, &root_sha, limits).unwrap();
            let local = TwoBitGeneration::open(&output, &new_sha, limits).unwrap();
            let head = borsuk::two_bit_store::publish_two_bit_generation(
                &store,
                &ObjectPath::from("test/index"),
                &output,
                &new_sha,
                limits,
                None,
            )
            .await
            .unwrap();
            let remote = TwoBitGeneration::open_remote_from_head(&store, &head, limits, dir.path())
                .await
                .unwrap();
            let query = vec![1.; dimensions];
            let (baseline, _) = flat
                .diagnostic_search_with_store(&store, &query, 10)
                .await
                .unwrap();
            let ranking = |result: &borsuk::two_bit_generation::TwoBitSearchResult| {
                result
                    .ranked
                    .candidates
                    .iter()
                    .map(|hit| (hit.ordinal, hit.id, hit.score.to_bits()))
                    .collect::<Vec<_>>()
            };
            for generation in [&local, &remote] {
                let (actual, _) = generation
                    .diagnostic_search_with_store(&store, &query, 10)
                    .await
                    .unwrap();
                assert_eq!(actual.plan, baseline.plan);
                assert_eq!(ranking(&actual), ranking(&baseline));
            }
            let records_path = current.join("plane/records.bin");
            let mut damaged = std::fs::read(&records_path).unwrap();
            damaged[0] ^= 1;
            std::fs::write(&records_path, &damaged).unwrap();
            let failed = dir.path().join("corrupt-refused");
            args[6] = failed.display().to_string();
            assert!(run(&args).is_err());
            assert!(!failed.join("manifest.json").exists());
            assert_eq!(std::fs::read(&records_path).unwrap(), damaged);
            for (name, bytes) in &unchanged {
                if *name != "plane/records.bin" {
                    assert_eq!(std::fs::read(current.join(name)).unwrap(), *bytes, "{name}");
                }
            }
        });
    }
}
