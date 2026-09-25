//! One real object-store publication and cold/warm authenticated load.

use std::{env, error::Error, fs, path::Path};

use borsuk::resident_graph_store::{
    hydrate_graph_generation, publish_graph_generation, read_graph_head,
};
use object_store::parse_url_opts;
use serde_json::json;
use sha2::{Digest, Sha256};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        return Err(
            "usage: v224_graph_store_roundtrip S3_URI ROOT_JSON ARTIFACT_DIR CACHE_DIR".into(),
        );
    }
    let (store, prefix) = parse_url_opts(&Url::parse(&args[1])?, [("aws_region", "eu-central-1")])?;
    let root_bytes = fs::read(&args[2])?;
    let root_sha256 = format!("{:x}", Sha256::digest(&root_bytes));
    if read_graph_head(store.as_ref(), &prefix).await?.is_some() {
        return Err("publication prefix already has a head".into());
    }
    let published = publish_graph_generation(
        store.as_ref(),
        &prefix,
        &root_bytes,
        Path::new(&args[3]),
        None,
    )
    .await?;
    let head = read_graph_head(store.as_ref(), &prefix)
        .await?
        .ok_or("published head is missing")?;
    if head.root_sha256 != root_sha256 || published.root_sha256 != root_sha256 {
        return Err("head readback differs".into());
    }
    let (cold, cold_stats) = hydrate_graph_generation(
        store.as_ref(),
        &prefix,
        &head,
        Path::new(&args[4]),
        3 * 1024 * 1024 * 1024,
        8,
    )
    .await?;
    let (warm, warm_stats) = hydrate_graph_generation(
        store.as_ref(),
        &prefix,
        &head,
        Path::new(&args[4]),
        3 * 1024 * 1024 * 1024,
        8,
    )
    .await?;
    if cold_stats.object_gets != 5 || warm_stats.object_gets != 0 {
        return Err("cold/warm GET counts differ".into());
    }
    println!(
        "{}",
        json!({
            "schema": "borsuk-v224-graph-store-roundtrip-v1",
            "root_sha256": root_sha256,
            "generation": cold.generation(),
            "rows": cold.rows(),
            "dimensions": cold.dimensions(),
            "warm_generation": warm.generation(),
            "cold_gets": cold_stats.object_gets,
            "cold_bytes": cold_stats.response_bytes,
            "warm_gets": warm_stats.object_gets,
            "warm_bytes": warm_stats.response_bytes,
        })
    );
    Ok(())
}
