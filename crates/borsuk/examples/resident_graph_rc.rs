//! Rust v0.1 release candidate: create, publish, and reopen in separate runs.

use std::{env, error::Error, fs, path::Path};

use borsuk::{
    resident_graph_build::build_graph_generation,
    resident_graph_store::{hydrate_graph_generation, publish_graph_generation, read_graph_head},
};
use object_store::{local::LocalFileSystem, path::Path as ObjectPath};

fn rows() -> Vec<(u64, Vec<f32>)> {
    (0..256_u64)
        .map(|row| {
            (
                1000 + row,
                (0..64)
                    .map(|dimension| ((row * 37 + dimension * 13) % 101) as f32 / 50.0 - 1.0)
                    .collect(),
            )
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 3 || !matches!(args[1].as_str(), "create" | "search") {
        return Err("usage: resident_graph_rc {create|search} DIRECTORY".into());
    }
    let root = Path::new(&args[2]);
    let store_root = root.join("store");
    let prefix = ObjectPath::from("index");
    if args[1] == "create" {
        fs::create_dir_all(&store_root)?;
        let directory = root.join("generation");
        let sha = build_graph_generation(&directory, rows(), 1)?;
        let store = LocalFileSystem::new_with_prefix(&store_root)?;
        let published = publish_graph_generation(
            &store,
            &prefix,
            &fs::read(directory.join("root.json"))?,
            &directory,
            None,
        )
        .await?;
        if published.root_sha256 != sha {
            return Err("published root differs".into());
        }
        println!("{}", serde_json::json!({"root_sha256":sha}));
    } else {
        let store = LocalFileSystem::new_with_prefix(&store_root)?;
        let head = read_graph_head(&store, &prefix)
            .await?
            .ok_or("head missing")?;
        let (generation, hydration) = hydrate_graph_generation(
            &store,
            &prefix,
            &head,
            &root.join("cache"),
            32 * 1024 * 1024,
            2,
        )
        .await?;
        let mut searcher = generation.searcher()?;
        let ids = searcher.search(&rows()[42].1, 10)?.0;
        println!(
            "{}",
            serde_json::json!({
                "root_sha256":head.root_sha256,"generation":generation.generation(),
                "ids":ids,"hydrate_gets":hydration.object_gets,
            })
        );
    }
    Ok(())
}
