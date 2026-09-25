//! A changed graph generation, S3 head update CAS, and pinned-reader switch.

use std::{env, error::Error, fs, path::Path, sync::Arc};

use borsuk::{
    resident_fp16_tier::{ResidentFp16Tier, write_resident_fp16_tier},
    resident_graph_generation::{ResidentGraphGeneration, ResidentGraphSlot},
    resident_graph_store::{
        ResidentGraphStoreError, hydrate_graph_generation, publish_graph_generation,
        read_graph_head,
    },
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

fn search(generation: &ResidentGraphGeneration) -> Result<Vec<u64>, Box<dyn Error>> {
    let view = generation.cosine_view()?;
    let graph = generation.bind(&view)?;
    let mut workspace = GraphSearchWorkspace::new(generation.rows())?;
    Ok(graph.search(&[1.0, 0.0], 4, 4, 4, &mut workspace)?.0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        return Err("usage: v225_graph_switch_s3 S3_URI WORK_DIR".into());
    }
    let (store, prefix) = parse_url_opts(&Url::parse(&args[1])?, [("aws_region", "eu-central-1")])?;
    let work = Path::new(&args[2]);
    fs::create_dir_all(work)?;
    if read_graph_head(store.as_ref(), &prefix).await?.is_some() {
        return Err("publication prefix already has a head".into());
    }
    let first = build(&work.join("first"), 7, 42, [1.0, 0.0])?;
    let first_head =
        publish_graph_generation(store.as_ref(), &prefix, &first, &work.join("first"), None)
            .await?;
    let pinned_head = read_graph_head(store.as_ref(), &prefix)
        .await?
        .ok_or("first head missing")?;
    if first_head.root_sha256 != pinned_head.root_sha256 {
        return Err("first head readback differs".into());
    }
    let (first_graph, first_stats) = hydrate_graph_generation(
        store.as_ref(),
        &prefix,
        &pinned_head,
        &work.join("cache"),
        100_000,
        1,
    )
    .await?;
    let slot = ResidentGraphSlot::new(Arc::new(first_graph));
    let held_reader = slot.pin();
    let old_ids = search(&held_reader)?;

    let second = build(&work.join("second"), 8, 99, [0.0, 1.0])?;
    let second_head = publish_graph_generation(
        store.as_ref(),
        &prefix,
        &second,
        &work.join("second"),
        Some(&pinned_head),
    )
    .await?;
    let readback = read_graph_head(store.as_ref(), &prefix)
        .await?
        .ok_or("second head missing")?;
    if readback.root_sha256 != second_head.root_sha256 || readback.generation != 8 {
        return Err("second head readback differs".into());
    }
    let (next_graph, second_stats) = hydrate_graph_generation(
        store.as_ref(),
        &prefix,
        &readback,
        &work.join("cache"),
        100_000,
        1,
    )
    .await?;
    let retiring = slot.replace(Arc::new(next_graph))?;
    let new_ids = search(&slot.pin())?;
    if !Arc::ptr_eq(&retiring, &held_reader)
        || held_reader.generation() != 7
        || slot.pin().generation() != 8
        || !old_ids.contains(&42)
        || old_ids.contains(&99)
        || !new_ids.contains(&99)
        || new_ids.contains(&42)
        || search(&held_reader)? != old_ids
    {
        return Err("pinned reader or switched result differs".into());
    }

    let stale = build(&work.join("stale"), 9, 77, [1.0, 0.0])?;
    let stale_error = publish_graph_generation(
        store.as_ref(),
        &prefix,
        &stale,
        &work.join("stale"),
        Some(&pinned_head),
    )
    .await
    .unwrap_err();
    if !matches!(
        stale_error,
        ResidentGraphStoreError::Store(StoreError::Precondition { .. })
    ) || read_graph_head(store.as_ref(), &prefix)
        .await?
        .ok_or("final head missing")?
        .root_sha256
        != second_head.root_sha256
    {
        return Err("stale CAS moved head or wrong error".into());
    }
    println!(
        "{}",
        json!({
            "schema":"borsuk-v225-graph-switch-s3-v1",
            "old_generation":held_reader.generation(),
            "new_generation":slot.pin().generation(),
            "old_ids":old_ids,"new_ids":new_ids,
            "first_root_sha256":first_head.root_sha256,
            "second_root_sha256":second_head.root_sha256,
            "first_blob_gets":first_stats.object_gets,
            "second_blob_gets":second_stats.object_gets,
            "stale_cas_rejected":true,
        })
    );
    Ok(())
}
