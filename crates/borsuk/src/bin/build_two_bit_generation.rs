//! Assemble approved source/SQ8 snapshots through the public generation builder.
use borsuk::{
    semantic_unit_router::SemanticProfile, two_bit_build::TwoBitGenerationBuilder,
    two_bit_generation::DiscoveryMode, two_bit_source::TwoBitSource,
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
fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
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
    println!("{sha}");
    Ok(())
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
}
