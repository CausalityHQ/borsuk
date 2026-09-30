//! Offline publication adapter; preserves the fitted router and calls the shared validator.
use borsuk::{
    semantic_unit_router::{RouterArtifacts, SourceIdentity},
    two_bit_build::{SemanticRouterImport, repackage_semantic_router},
    two_bit_generation::TwoBitGenerationLimits,
};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::Read,
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const ROOT_CAP: usize = 65536;
const MEMORY_CAP: u64 = 128 * 1024 * 1024;

fn regular(path: &Path, cap: usize) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > cap as u64 {
        return Err(format!("regular input/byte cap: {}", path.display()).into());
    }
    Ok(file)
}

fn bounded(path: &Path, cap: usize) -> Result<Vec<u8>> {
    let mut file = regular(path, cap)?;
    let mut bytes = vec![0; usize::try_from(file.metadata()?.len())?];
    file.read_exact(&mut bytes)?;
    if file.read(&mut [0])? != 0 {
        return Err("input length changed".into());
    }
    Ok(bytes)
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 8 {
        return Err("usage: repackage_semantic_generation GRAPH ROOT_SHA SQ8 PROOF_DIR ORIGINAL_ROOT_SHA ROUTER_SHA NEW_OUTPUT".into());
    }
    let current = Path::new(&args[1]);
    let proof = Path::new(&args[4]);
    let output = Path::new(&args[7]);
    if output.symlink_metadata().is_ok() {
        return Err("output already exists".into());
    }
    let original_root = bounded(&proof.join("original-root.json"), ROOT_CAP)?;
    let original_plane = bounded(&proof.join("original-plane.json"), ROOT_CAP)?;
    if hash(&original_root) != args[5] {
        return Err("original root SHA256".into());
    }
    let root: serde_json::Value = serde_json::from_slice(&original_root)?;
    let rows = root["canonical"]["rows"].as_u64().ok_or("root rows")?;
    let dimensions = root["canonical"]["dimensions"]
        .as_u64()
        .ok_or("root dimensions")?;
    if root["schema"] != "borsuk-two-bit-generation-v4"
        || !(1..=100_000).contains(&rows)
        || !(1..=768).contains(&dimensions)
    {
        return Err("original schema/geometry cap".into());
    }
    let (rows, dimensions) = (rows as usize, dimensions as usize);
    let units = rows.div_ceil(32);
    let artifacts = RouterArtifacts {
        manifest: bounded(&proof.join("router/manifest.json"), 1024 * 1024)?,
        membership: bounded(&proof.join("router/membership.bin"), units * 4)?,
        leaves: bounded(
            &proof.join("router/leaves.bin"),
            units * (4 + dimensions * 2),
        )?,
    };
    if hash(&artifacts.manifest) != args[6] {
        return Err("router root SHA256".into());
    }
    let centroid_sha = root["centroids_sha256"]
        .as_str()
        .ok_or("centroid SHA256")?
        .to_owned();
    let import = SemanticRouterImport {
        artifacts: &artifacts,
        input: SourceIdentity {
            schema: "borsuk-two-bit-generation-v4",
            root_sha256: &args[5],
            centroids_sha256: &centroid_sha,
            rows,
            dimensions,
        },
        original_root: &original_root,
        original_plane: &original_plane,
    };
    // The shared helper opens immutable caller snapshots. Reject special files
    // before entering its ordinary file readers, including FIFO without a writer.
    for (name, cap) in [
        ("manifest.json", ROOT_CAP),
        ("plane/manifest.json", ROOT_CAP),
        ("plane/mean.bin", dimensions * 4),
        // D <= 768, including codec block padding; shared validation checks exact size.
        ("plane/records.bin", rows * 200),
        ("plane/page_digests.bin", units * 32),
        ("page_manifest.json", ROOT_CAP),
        ("page_digests.bin", rows.div_ceil(256) * 32),
        ("canonical.bin", rows * (dimensions * 4 + 8)),
        ("centroids.bin", 32 + units * dimensions * 2),
        ("graph.bin", 8 * 1024 * 1024),
        ("diverse_graph.bin", 8 * 1024 * 1024),
    ] {
        regular(&current.join(name), cap)?;
    }
    regular(Path::new(&args[3]), rows * (dimensions + 12))?;
    // Charge the two raw proof bodies plus a conservative bound for the decoded
    // original root. The helper separately admits the retained router vectors.
    let pinned = (original_root.capacity() * 8 + original_plane.capacity()) as u64;
    drop(root);
    let sha = repackage_semantic_router(
        current,
        &args[2],
        Path::new(&args[3]),
        &import,
        output,
        TwoBitGenerationLimits {
            max_memory_bytes: MEMORY_CAP,
            already_pinned_bytes: pinned,
            max_active_queries: 1,
            max_query_bytes: 2 * 1024 * 1024,
            max_query_gets: 8,
            max_parallel_gets: 1,
            max_source_bytes: 2 * 1024 * 1024,
            max_source_gets: 8,
            max_parallel_source_gets: 1,
            max_query_scratch_bytes: 8 * 1024 * 1024,
        },
    )?;
    println!(
        "{}",
        serde_json::json!({
            "root_sha256": sha,
            "root_bytes": output.join("manifest.json").metadata()?.len(),
            "rows": rows, "dimensions": dimensions,
            "original_root_sha256": args[5], "router_sha256": args[6],
            "modeled_memory_limit_bytes": MEMORY_CAP,
            "caller_pinned_bytes": pinned,
        })
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regular_bounded_inputs_only() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("input");
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(bounded(&path, 3).unwrap(), b"abc");
        assert!(bounded(&path, 2).is_err());
        assert!(bounded(temp.path(), ROOT_CAP).is_err());
        std::fs::write(&path, []).unwrap();
        assert!(bounded(&path, ROOT_CAP).is_err());
        let fifo = temp.path().join("fifo");
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || send.send(bounded(&fifo, ROOT_CAP).is_err()));
        assert!(
            receive
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
        );
        worker.join().unwrap().unwrap();
    }
}
