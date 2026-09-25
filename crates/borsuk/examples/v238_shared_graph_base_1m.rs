//! Measure two decoded collection overlays sharing one authenticated graph.

use std::{env, error::Error, fs, path::Path, sync::Arc, time::Instant};

use borsuk::{
    resident_graph_collection::{
        hydrate_graph_collection_decoded, hydrate_graph_collection_decoded_reusing_base,
        read_graph_collection_head,
    },
    resident_graph_overlay::ResidentGraphOverlay,
    resident_vector_graph::GraphSearchWorkspace,
};
use object_store::parse_url_opts;
use serde_json::json;
use url::Url;

const ROOT_SHA: &str = "c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf";
const MUTATION_SHA: &str = "6b9ba6391dff9de0867447045504beaa1c22776ccf9502d4065c7a3b76ab4153";

fn memory_bytes(name: &str) -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with(name))
        .ok_or("memory field missing")?;
    Ok(line
        .split_whitespace()
        .nth(1)
        .ok_or("memory value missing")?
        .parse::<u64>()?
        * 1024)
}

fn search(overlay: &ResidentGraphOverlay) -> Result<Vec<u64>, Box<dyn Error>> {
    let view = overlay.base().cosine_view()?;
    let bound = overlay.bind(&view)?;
    Ok(bound
        .search(
            &vec![1.0; 768],
            100,
            4096,
            4096,
            &mut GraphSearchWorkspace::new(overlay.base().rows())?,
        )?
        .0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        return Err("usage: v238_shared_graph_base_1m S3_URI CACHE_DIR RESULT_JSON".into());
    }
    let (store, prefix) = parse_url_opts(&Url::parse(&args[1])?, [("aws_region", "eu-central-1")])?;
    let head = read_graph_collection_head(store.as_ref(), &prefix, 16 * 1024 * 1024)
        .await?
        .ok_or("collection head missing")?;
    if head.revision != 2
        || head.base_root_sha256 != ROOT_SHA
        || head.mutation_sha256 != MUTATION_SHA
    {
        return Err("collection authority differs".into());
    }
    let started = Instant::now();
    let (old, cold) = hydrate_graph_collection_decoded(
        store.as_ref(),
        &prefix,
        &head,
        Path::new(&args[2]),
        3 * 1024 * 1024 * 1024,
        8,
        16 * 1024 * 1024,
        64 * 1024 * 1024,
    )
    .await?;
    let cold_hydrate_ms = started.elapsed().as_secs_f64() * 1000.0;
    if old.base().rows() != 1_000_000 || old.base().dimensions() != 768 {
        return Err("graph geometry differs".into());
    }
    let old_ids = search(&old)?;
    let rss_before = memory_bytes("VmRSS:")?;
    let started = Instant::now();
    let new = hydrate_graph_collection_decoded_reusing_base(
        &head,
        &old,
        16 * 1024 * 1024,
        64 * 1024 * 1024,
    )?;
    let reuse_ms = started.elapsed().as_secs_f64() * 1000.0;
    let rss_after = memory_bytes("VmRSS:")?;
    let base_shared = Arc::ptr_eq(&old.base_arc(), &new.base_arc());
    let ids_equal = old_ids == search(&new)?;
    let peak_rss = memory_bytes("VmHWM:")?;
    if !base_shared || !ids_equal {
        return Err("shared reader differs".into());
    }
    fs::write(
        &args[3],
        serde_json::to_vec(&json!({
            "schema":"borsuk-v238-shared-graph-base-1m-v1",
            "revision":head.revision,"root_sha256":head.base_root_sha256,
            "mutation_sha256":head.mutation_sha256,
            "cold_graph_blob_gets":cold.object_gets,
            "cold_graph_response_bytes":cold.response_bytes,
            "old_overlay_bytes":old.resident_bytes(),
            "new_overlay_bytes":new.resident_bytes(),
            "base_shared":base_shared,"ids_equal":ids_equal,
            "cold_hydrate_ms":cold_hydrate_ms,"reuse_ms":reuse_ms,
            "rss_before_bytes":rss_before,"rss_after_bytes":rss_after,
            "rss_increase_bytes":rss_after.saturating_sub(rss_before),
            "peak_rss_bytes":peak_rss,
        }))?,
    )?;
    Ok(())
}
