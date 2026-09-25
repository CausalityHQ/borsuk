//! Publish a frozen 1M graph, mutate it, then replay pinned S3 revisions.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    sync::Arc,
    time::Instant,
};

use borsuk::{
    resident_graph_collection::{
        ResidentGraphCollectionSlot, apply_graph_mutations, hydrate_graph_collection_decoded,
        publish_graph_collection, read_graph_collection_head,
    },
    resident_graph_overlay::{ResidentGraphOverlay, ResidentMutation},
    resident_graph_store::{publish_graph_generation, read_graph_head},
    resident_vector_graph::GraphSearchWorkspace,
};
use object_store::{ObjectStoreExt, parse_url_opts};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use url::Url;

const ROOT_SHA: &str = "c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf";
const SNAPSHOT_CAP: usize = 16 * 1024 * 1024;
const DELTA_CAP: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
struct Request {
    query_ordinal: usize,
    query: Vec<f32>,
}

fn search_all(
    overlay: &ResidentGraphOverlay,
    requests: &[Request],
) -> Result<Vec<Vec<u64>>, Box<dyn Error>> {
    let view = overlay.base().cosine_view()?;
    let bound = overlay.bind(&view)?;
    let mut workspace = GraphSearchWorkspace::new(overlay.base().rows())?;
    requests
        .iter()
        .map(|request| {
            let (ids, _) = bound.search(&request.query, 100, 4096, 4096, &mut workspace)?;
            if ids.len() != 100 {
                return Err("short result".into());
            }
            Ok(ids)
        })
        .collect()
}

fn write_raw(path: &Path, ids: &[Vec<u64>]) -> Result<(), Box<dyn Error>> {
    let mut output = BufWriter::new(File::create(path)?);
    for (ordinal, returned_ids) in ids.iter().enumerate() {
        serde_json::to_writer(
            &mut output,
            &json!({
                "ordinal":ordinal,"returned_ids":returned_ids,
            }),
        )?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 6 {
        return Err("usage: v236_graph_collection_1m S3_URI ARTIFACT_DIR CACHE_DIR REQUESTS_JSONL OUTPUT_DIR".into());
    }
    let (store, prefix) = parse_url_opts(&Url::parse(&args[1])?, [("aws_region", "eu-central-1")])?;
    let artifacts = Path::new(&args[2]);
    let cache = Path::new(&args[3]);
    let output = Path::new(&args[5]);
    fs::create_dir_all(output)?;
    if read_graph_head(store.as_ref(), &prefix).await?.is_some()
        || read_graph_collection_head(store.as_ref(), &prefix, SNAPSHOT_CAP)
            .await?
            .is_some()
    {
        return Err("publication prefix already has a head".into());
    }
    let root_bytes = fs::read(artifacts.join("generation.json"))?;
    if format!("{:x}", Sha256::digest(&root_bytes)) != ROOT_SHA {
        return Err("trusted graph root differs".into());
    }
    let requests = BufReader::new(File::open(&args[4])?)
        .lines()
        .map(|line| Ok(serde_json::from_str::<Request>(&line?)?))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if requests.len() != 1000
        || requests.iter().enumerate().any(|(i, row)| {
            row.query_ordinal != i
                || row.query.len() != 768
                || row.query.iter().any(|value| !value.is_finite())
        })
    {
        return Err("request panel geometry".into());
    }

    let start = Instant::now();
    let graph_head =
        publish_graph_generation(store.as_ref(), &prefix, &root_bytes, artifacts, None).await?;
    if graph_head.root_sha256 != ROOT_SHA {
        return Err("published graph root differs".into());
    }
    let graph_publish_ms = start.elapsed().as_secs_f64() * 1000.0;
    let initial =
        publish_graph_collection(store.as_ref(), &prefix, ROOT_SHA, &[], SNAPSHOT_CAP, None)
            .await?;
    let pinned = read_graph_collection_head(store.as_ref(), &prefix, SNAPSHOT_CAP)
        .await?
        .ok_or("revision 1 missing")?;
    if initial.revision != 1 || pinned.revision != 1 {
        return Err("revision 1 differs".into());
    }
    let start = Instant::now();
    let (old, cold) = hydrate_graph_collection_decoded(
        store.as_ref(),
        &prefix,
        &pinned,
        cache,
        3 * 1024 * 1024 * 1024,
        8,
        SNAPSHOT_CAP,
        DELTA_CAP,
    )
    .await?;
    let cold_hydrate_ms = start.elapsed().as_secs_f64() * 1000.0;
    if old.base().rows() != 1_000_000 || old.base().dimensions() != 768 {
        return Err("base geometry differs".into());
    }
    let slot = ResidentGraphCollectionSlot::new(1, Arc::clone(&old))?;
    let held = slot.pin();
    let old_ids = search_all(&held.1, &requests)?;
    let updates = (0..1_000_000)
        .step_by(100)
        .map(|ordinal| {
            Ok(ResidentMutation {
                id: old.base().source_id(ordinal)?,
                vector: Some(old.base().vector_f32(ordinal)?),
            })
        })
        .collect::<Result<Vec<_>, borsuk::resident_graph_generation::ResidentGraphGenerationError>>(
        )?;
    let start = Instant::now();
    let changed = apply_graph_mutations(store.as_ref(), &prefix, &updates, SNAPSHOT_CAP, 4).await?;
    let mutation_publish_ms = start.elapsed().as_secs_f64() * 1000.0;
    let readback = read_graph_collection_head(store.as_ref(), &prefix, SNAPSHOT_CAP)
        .await?
        .ok_or("revision 2 missing")?;
    if changed.revision != 2
        || readback.revision != 2
        || readback.mutation_sha256 != changed.mutation_sha256
    {
        return Err("revision 2 differs".into());
    }
    let start = Instant::now();
    let (new, warm) = hydrate_graph_collection_decoded(
        store.as_ref(),
        &prefix,
        &readback,
        cache,
        3 * 1024 * 1024 * 1024,
        8,
        SNAPSHOT_CAP,
        DELTA_CAP,
    )
    .await?;
    let warm_hydrate_ms = start.elapsed().as_secs_f64() * 1000.0;
    let retired = slot.replace(2, new)?;
    let new_ids = search_all(&slot.pin().1, &requests)?;
    if !Arc::ptr_eq(&retired.1, &held.1) || search_all(&held.1, &requests)? != old_ids {
        return Err("held revision 1 changed".into());
    }
    write_raw(&output.join("revision1.raw.jsonl"), &old_ids)?;
    write_raw(&output.join("revision2.raw.jsonl"), &new_ids)?;
    let snapshot = store
        .head(
            &prefix
                .clone()
                .join(format!("mutations/{}.bin", readback.mutation_sha256,)),
        )
        .await?;
    let result = json!({
        "schema":"borsuk-v236-graph-collection-1m-v1",
        "root_sha256":ROOT_SHA,"initial_revision":1,"mutation_revision":2,
        "mutation_sha256":readback.mutation_sha256,"mutation_snapshot_bytes":snapshot.size,
        "mutation_rows":updates.len(),"queries":requests.len(),
        "old_reader_unchanged":true,"cold_graph_blob_gets":cold.object_gets,
        "cold_graph_response_bytes":cold.response_bytes,
        "warm_graph_blob_gets":warm.object_gets,
        "warm_graph_response_bytes":warm.response_bytes,
        "graph_publish_ms":graph_publish_ms,"cold_hydrate_ms":cold_hydrate_ms,
        "mutation_publish_ms":mutation_publish_ms,"warm_hydrate_ms":warm_hydrate_ms,
        "old_overlay_bytes":held.1.resident_bytes(),
        "new_overlay_bytes":slot.pin().1.resident_bytes(),
    });
    fs::write(output.join("collection.json"), serde_json::to_vec(&result)?)?;
    Ok(())
}
