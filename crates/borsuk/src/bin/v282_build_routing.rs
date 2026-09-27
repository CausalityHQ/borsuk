//! Build authenticated centroid routing artifacts from one sealed SQ8 object.

use std::error::Error;
use std::fs::{self, File};
use std::io::{Read, Seek};
use std::path::Path;

use borsuk::pq64_router_artifact::load_source_router;
use borsuk::unit_centroid_graph::UnitCentroidGraph;
use borsuk::unit_centroid_pages::UnitCentroidPages;
use sha2::{Digest, Sha256};

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err(
            "usage: v282_build_routing SQ8 ROUTER_DIR ROUTER_MANIFEST_SHA OUTPUT_DIR".into(),
        );
    }
    let router = load_source_router(Path::new(&args[2]), &args[3])?;
    let mut sq8 = File::open(&args[1])?;
    let expected_bytes = router
        .router
        .rows()
        .checked_mul(
            router
                .router
                .dimensions()
                .checked_add(12)
                .ok_or("SQ8 width overflow")?,
        )
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or("SQ8 size overflow")?;
    if sq8.metadata()?.len() != expected_bytes {
        return Err("SQ8 geometry differs".into());
    }
    let mut source_hash = Sha256::new();
    let mut chunk = vec![0; 4 * 1024 * 1024];
    loop {
        let count = sq8.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        source_hash.update(&chunk[..count]);
    }
    if format!("{:x}", source_hash.finalize()) != router.sq8_sha256 {
        return Err("SQ8 digest differs from router".into());
    }
    sq8.rewind()?;
    let centroid_blob = UnitCentroidPages::build_from_sq8_reader(
        &mut sq8,
        router.router.rows(),
        router.router.dimensions(),
        32,
        256,
        &router.low,
        &router.step,
    )?;
    let centroids = UnitCentroidPages::decode(&centroid_blob)?;
    let graph_blob = UnitCentroidGraph::build(&centroids, &centroid_blob)?.encode()?;
    let graph_resident = UnitCentroidGraph::preflight_resident_bytes(&graph_blob, &centroids)?;
    let out = Path::new(&args[4]);
    fs::create_dir(out)?;
    fs::write(out.join("centroids.bin"), &centroid_blob)?;
    fs::write(out.join("graph.bin"), &graph_blob)?;
    let build = serde_json::json!({
        "schema":"borsuk-v282-routing-build-v1",
        "rows":router.router.rows(),
        "dimensions":router.router.dimensions(),
        "unit_rows":32,
        "page_rows":256,
        "sq8_sha256":router.sq8_sha256,
        "centroids_sha256":digest(&centroid_blob),
        "graph_sha256":digest(&graph_blob),
        "graph_resident_bytes":graph_resident,
    });
    fs::write(out.join("build.json"), format!("{build}\n"))?;
    println!("{build}");
    Ok(())
}
