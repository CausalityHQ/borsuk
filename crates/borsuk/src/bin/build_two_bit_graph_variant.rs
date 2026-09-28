//! Offline topology-only generation adapter; never a production builder default.
use borsuk::{
    two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace},
    unit_centroid_graph::UnitCentroidGraph,
    unit_centroid_pages::UnitCentroidPages,
};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{self, File},
    io::Read,
    path::Path,
    time::Instant,
};

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn bounded(path: &Path, cap: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take((cap + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > cap {
        return Err("offline metadata cap".into());
    }
    Ok(bytes)
}
fn cpu_ns() -> Result<u64, Box<dyn Error>> {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    Ok(u64::try_from(
        i128::from(t.tv_sec) * 1_000_000_000 + i128::from(t.tv_nsec),
    )?)
}
fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err("usage: build_two_bit_graph_variant ROOT ROOT_SHA NEW_OUTPUT".into());
    }
    let root = Path::new(&args[1]);
    let out = Path::new(&args[3]);
    if out.exists() {
        return Err("output already exists".into());
    }
    let body = bounded(&root.join("manifest.json"), 65536)?;
    if hash(&body) != args[2] {
        return Err("root identity".into());
    }
    let mut manifest: serde_json::Value = serde_json::from_slice(&body)?;
    let rows = manifest["canonical"]["rows"]
        .as_u64()
        .ok_or("row geometry")?;
    let dimensions = manifest["canonical"]["dimensions"]
        .as_u64()
        .ok_or("dimension geometry")?;
    if rows == 0 || rows > 100000 || dimensions == 0 || dimensions > 768 {
        return Err("offline graph preparation limited to100k rows/D768".into());
    }
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: 1_073_741_824,
        max_active_queries: 1,
        max_query_bytes: 84 * 256 * 780,
        max_query_gets: 32,
        max_parallel_gets: 32,
        max_query_scratch_bytes: 400_000 + TwoBitPlanTrace::scratch_bytes(usize::MAX),
        already_pinned_bytes: 0,
    };
    // Authenticate all source metadata before allocating output or building graphs.
    drop(TwoBitGeneration::open(root, &args[2], limits)?);
    let blob = bounded(&root.join("centroids.bin"), 64 * 1024 * 1024)?;
    if hash(&blob)
        != manifest["centroids_sha256"]
            .as_str()
            .ok_or("centroid hash")?
    {
        return Err("centroid identity".into());
    }
    let centroids = UnitCentroidPages::decode(&blob)?;
    let old = bounded(&root.join("graph.bin"), 64 * 1024 * 1024)?;
    if hash(&old) != manifest["graph_sha256"].as_str().ok_or("graph hash")? {
        return Err("graph identity".into());
    }
    let wall = Instant::now();
    let cpu = cpu_ns()?;
    let nearest = UnitCentroidGraph::build(&centroids, &blob)?;
    let nearest_blob = nearest.encode()?;
    let nearest_wall = wall.elapsed().as_nanos();
    let nearest_cpu = cpu_ns()?.checked_sub(cpu).ok_or("CPU clock")?;
    if nearest_blob != old {
        return Err("nearest reconstruction differs from authenticated control".into());
    }
    let nearest_degrees = nearest.diagnostic_node_degrees();
    drop(nearest);
    let wall = Instant::now();
    let cpu = cpu_ns()?;
    let diverse = UnitCentroidGraph::build_diverse(&centroids, &blob)?;
    let diverse_blob = diverse.encode()?;
    let diverse_wall = wall.elapsed().as_nanos();
    let diverse_cpu = cpu_ns()?.checked_sub(cpu).ok_or("CPU clock")?;
    if diverse_blob == old {
        return Err("no topology intervention: graphs identical".into());
    }
    let diverse_degrees = diverse.diagnostic_node_degrees();
    if nearest_blob[..80] != diverse_blob[..80]
        || nearest_degrees
            .iter()
            .map(Vec::len)
            .ne(diverse_degrees.iter().map(Vec::len))
    {
        return Err("graph headers/levels changed".into());
    }
    drop(diverse);
    let resident = UnitCentroidGraph::preflight_resident_bytes(&diverse_blob, &centroids)?;
    let stats = serde_json::json!({"schema":"borsuk-topology-only-build-v1","rows":rows,"dimensions":dimensions,
        "centroids_sha256":hash(&blob),"control_root_sha256":args[2],
        "control":{"graph_sha256":hash(&nearest_blob),"graph_bytes":nearest_blob.len(),
            "graph_resident_bytes":manifest["graph_resident_bytes"],"node_layer_degrees":nearest_degrees,
            "build_wall_ns":nearest_wall,"build_process_cpu_ns":nearest_cpu},
        "candidate":{"graph_sha256":hash(&diverse_blob),"graph_bytes":diverse_blob.len(),
            "graph_resident_bytes":resident,"node_layer_degrees":diverse_degrees,
            "build_wall_ns":diverse_wall,"build_process_cpu_ns":diverse_cpu}});
    manifest["graph_sha256"] = hash(&diverse_blob).into();
    manifest["graph_resident_bytes"] = resident.into();
    let new_body = serde_json::to_vec(&manifest)?;
    let parent = out
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let staged = tempfile::tempdir_in(parent)?;
    fs::create_dir(staged.path().join("plane"))?;
    for name in [
        "page_manifest.json",
        "page_digests.bin",
        "centroids.bin",
        "plane/manifest.json",
        "plane/mean.bin",
        "plane/records.bin",
    ] {
        fs::write(
            staged.path().join(name),
            bounded(&root.join(name), 64 * 1024 * 1024)?,
        )?;
    }
    fs::write(staged.path().join("graph.bin"), &diverse_blob)?;
    fs::write(staged.path().join("build.json"), format!("{stats}\n"))?;
    fs::write(staged.path().join("manifest.json"), &new_body)?;
    let root_sha = hash(&new_body);
    drop(TwoBitGeneration::open(staged.path(), &root_sha, limits)?);
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staged.path(),
        rustix::fs::CWD,
        out,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    println!(
        "{}",
        serde_json::json!({"root_sha256":root_sha,"build":stats})
    );
    Ok(())
}
