//! Publish one authenticated graph generation to a fresh S3 prefix.

use std::{env, error::Error, fs, path::Path};

use borsuk::resident_graph_store::{publish_graph_generation, read_graph_head};
use object_store::parse_url_opts;
use sha2::{Digest, Sha256};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err(
            "usage: v246_publish_graph_s3 S3_URI ROOT_JSON ARTIFACT_DIR TRUSTED_SHA".into(),
        );
    }
    let root = fs::read(&args[2])?;
    if format!("{:x}", Sha256::digest(&root)) != args[4] {
        return Err("trusted root differs".into());
    }
    let (store, prefix) = parse_url_opts(&Url::parse(&args[1])?, [("aws_region", "eu-central-1")])?;
    if read_graph_head(store.as_ref(), &prefix).await?.is_some() {
        return Err("publication prefix already has a head".into());
    }
    let head =
        publish_graph_generation(store.as_ref(), &prefix, &root, Path::new(&args[3]), None).await?;
    let readback = read_graph_head(store.as_ref(), &prefix)
        .await?
        .ok_or("published head missing")?;
    if head.root_sha256 != args[4] || readback.root_sha256 != args[4] {
        return Err("published root differs".into());
    }
    println!(
        "{}",
        serde_json::json!({"generation":head.generation,"root_sha256":head.root_sha256})
    );
    Ok(())
}
