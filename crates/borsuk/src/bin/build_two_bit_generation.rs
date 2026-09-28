//! Assemble approved source/SQ8 snapshots through the public generation builder.
use borsuk::{two_bit_build::TwoBitGenerationBuilder, two_bit_source::TwoBitSource};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
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
    println!("{}", builder.build(Path::new(&args[4]), args[3].parse()?)?);
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_rejects_wrong_hash_oversize_and_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.json");
        let valid = br#"{"raw":"raw","raw_sha256":"a","sq8":"sq8","sq8_sha256":"b","rows":1,"dimensions":2,"generation":1,"base_epoch":0,"low":[0,0],"step":[1,1],"sq8_object_key":"key","sq8_etag":"etag"}"#;
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
