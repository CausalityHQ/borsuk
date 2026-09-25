//! S3 collection head CAS across mutations and a staged base-root switch.

use std::{env, error::Error, fs, path::Path, sync::Arc};

use borsuk::{
    resident_fp16_tier::{ResidentFp16Tier, write_resident_fp16_tier},
    resident_graph_collection::{
        ResidentGraphCollectionError, ResidentGraphCollectionSlot, apply_graph_mutations,
        hydrate_graph_collection, publish_graph_collection, read_graph_collection_head,
    },
    resident_graph_overlay::{ResidentGraphOverlay, ResidentMutation},
    resident_graph_store::{publish_graph_generation, read_graph_head, stage_graph_generation},
    resident_vector_graph::{GraphSearchWorkspace, ResidentVectorGraph},
};
use object_store::{Error as StoreError, parse_url_opts};
use serde_json::json;
use sha2::{Digest, Sha256};
use url::Url;

fn build(
    directory: &Path,
    generation: u64,
    first_id: u64,
    first: [f32; 2],
) -> Result<Vec<u8>, Box<dyn Error>> {
    fs::create_dir_all(directory)?;
    let source = format!("{:x}", Sha256::digest(format!("source-{generation}")));
    let vectors = vec![
        first.to_vec(),
        vec![0.9, 0.1],
        vec![0.0, 1.0],
        vec![-1.0, 0.0],
    ];
    let plane_path = directory.join("plane.bin");
    let plane_sha = write_resident_fp16_tier(
        &plane_path,
        4,
        2,
        generation,
        &source,
        [first_id, 7, 19, 33]
            .into_iter()
            .zip(vectors.iter().cloned()),
    )?;
    let plane = ResidentFp16Tier::open_authenticated(
        &plane_path,
        &plane_sha,
        &source,
        4,
        2,
        generation,
        48,
    )?;
    ResidentVectorGraph::build(vectors, &plane, 4, 4, 8)?
        .write_authenticated(&directory.join("graph.bin"))?;
    fs::write(
        directory.join("map.u32"),
        (0..4_u32).flat_map(u32::to_le_bytes).collect::<Vec<_>>(),
    )?;
    let mut books = vec![0.0_f32; 64 * 256];
    books[31 * 256 + 1] = 1.0;
    books[63 * 256] = 0.1;
    fs::write(
        directory.join("books.bin"),
        books
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect::<Vec<_>>(),
    )?;
    let mut codes = vec![0_u8; 4 * 64];
    codes[31] = 1;
    codes[64 + 31] = 1;
    fs::write(directory.join("codes.bin"), codes)?;
    let artifact = |name: &str| -> Result<serde_json::Value, Box<dyn Error>> {
        let bytes = fs::read(directory.join(name))?;
        Ok(json!({"bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(&bytes))}))
    };
    Ok(serde_json::to_vec(&json!({
        "schema":"borsuk-resident-graph-generation-v1",
        "generation":generation,"source_sha256":source,"rows":4,"dimensions":2,
        "plane":artifact("plane.bin")?,"graph":artifact("graph.bin")?,
        "map":artifact("map.u32")?,"books":artifact("books.bin")?,
        "codes":artifact("codes.bin")?
    }))?)
}

fn search(overlay: &ResidentGraphOverlay) -> Result<Vec<u64>, Box<dyn Error>> {
    let view = overlay.base().cosine_view()?;
    let graph = overlay.bind(&view)?;
    let mut workspace = GraphSearchWorkspace::new(overlay.base().rows())?;
    Ok(graph.search(&[1.0, 0.0], 4, 4, 4, &mut workspace)?.0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        return Err("usage: v228_graph_collection_s3 S3_URI WORK_DIR".into());
    }
    let (store, prefix) = parse_url_opts(&Url::parse(&args[1])?, [("aws_region", "eu-central-1")])?;
    let work = Path::new(&args[2]);
    fs::create_dir_all(work)?;
    if read_graph_head(store.as_ref(), &prefix).await?.is_some()
        || read_graph_collection_head(store.as_ref(), &prefix, 1024)
            .await?
            .is_some()
    {
        return Err("publication prefix already has a head".into());
    }
    let first = build(&work.join("first"), 7, 42, [1.0, 0.0])?;
    let first_head =
        publish_graph_generation(store.as_ref(), &prefix, &first, &work.join("first"), None)
            .await?;
    let initial = publish_graph_collection(
        store.as_ref(),
        &prefix,
        &first_head.root_sha256,
        &[],
        1024,
        None,
    )
    .await?;
    let pinned = read_graph_collection_head(store.as_ref(), &prefix, 1024)
        .await?
        .ok_or("collection head missing")?;
    let (old_overlay, old_stats) = hydrate_graph_collection(
        store.as_ref(),
        &prefix,
        &pinned,
        &work.join("cache"),
        100_000,
        1,
        1024,
        1,
    )
    .await?;
    let slot = ResidentGraphCollectionSlot::new(1, Arc::clone(&old_overlay))?;
    let held = slot.pin();
    let old_ids = search(&held.1)?;
    let changed = apply_graph_mutations(
        store.as_ref(),
        &prefix,
        &[
            ResidentMutation {
                id: 42,
                vector: None,
            },
            ResidentMutation {
                id: 99,
                vector: Some(vec![1.0, 0.0]),
            },
        ],
        1024,
        4,
    )
    .await?;
    let readback = read_graph_collection_head(store.as_ref(), &prefix, 1024)
        .await?
        .ok_or("changed collection head missing")?;
    if readback.revision != 2 || readback.mutation_sha256 != changed.mutation_sha256 {
        return Err("mutation head readback differs".into());
    }
    let (changed_overlay, changed_stats) = hydrate_graph_collection(
        store.as_ref(),
        &prefix,
        &readback,
        &work.join("cache"),
        100_000,
        1,
        1024,
        21,
    )
    .await?;
    let retired = slot.replace(2, changed_overlay)?;
    let mutation_ids = search(&slot.pin().1)?;
    if !Arc::ptr_eq(&retired.1, &held.1)
        || !old_ids.contains(&42)
        || old_ids.contains(&99)
        || mutation_ids.contains(&42)
        || !mutation_ids.contains(&99)
        || search(&held.1)? != old_ids
    {
        return Err("pinned mutation reader differs".into());
    }
    let second = build(&work.join("second"), 8, 99, [1.0, 0.0])?;
    let staged =
        stage_graph_generation(store.as_ref(), &prefix, &second, &work.join("second")).await?;
    if read_graph_head(store.as_ref(), &prefix)
        .await?
        .ok_or("base head missing")?
        .root_sha256
        != first_head.root_sha256
    {
        return Err("staging moved base head".into());
    }
    let switched =
        publish_graph_collection(store.as_ref(), &prefix, &staged, &[], 1024, Some(&readback))
            .await?;
    let latest = read_graph_collection_head(store.as_ref(), &prefix, 1024)
        .await?
        .ok_or("switched head missing")?;
    if switched.revision != 3 || latest.base_root_sha256 != staged {
        return Err("root switch readback differs".into());
    }
    let (new_overlay, new_stats) = hydrate_graph_collection(
        store.as_ref(),
        &prefix,
        &latest,
        &work.join("cache"),
        100_000,
        1,
        1024,
        1,
    )
    .await?;
    slot.replace(3, new_overlay)?;
    let new_ids = search(&slot.pin().1)?;
    let compacted_ids_match = new_ids == mutation_ids;
    if !compacted_ids_match || new_ids.contains(&42) || search(&held.1)? != old_ids {
        return Err("pinned root reader differs".into());
    }
    let stale_error = publish_graph_collection(
        store.as_ref(),
        &prefix,
        &first_head.root_sha256,
        &[],
        1024,
        Some(&pinned),
    )
    .await
    .unwrap_err();
    if !matches!(
        stale_error,
        ResidentGraphCollectionError::Store(StoreError::Precondition { .. })
    ) || read_graph_collection_head(store.as_ref(), &prefix, 1024)
        .await?
        .ok_or("final head missing")?
        .revision
        != 3
    {
        return Err("stale collection CAS moved head".into());
    }
    println!(
        "{}",
        json!({
            "schema":"borsuk-v228-graph-collection-s3-v1",
            "initial_revision":initial.revision,
            "mutation_revision":changed.revision,
            "switched_revision":switched.revision,
            "old_ids":old_ids,"mutation_ids":mutation_ids,"new_ids":new_ids,
            "first_root_sha256":first_head.root_sha256,
            "second_root_sha256":staged,
            "first_blob_gets":old_stats.object_gets,
            "first_response_bytes":old_stats.response_bytes,
            "mutation_blob_gets":changed_stats.object_gets,
            "mutation_response_bytes":changed_stats.response_bytes,
            "second_blob_gets":new_stats.object_gets,
            "second_response_bytes":new_stats.response_bytes,
            "compacted_ids_match":compacted_ids_match,
            "stale_cas_rejected":true,
        })
    );
    Ok(())
}
