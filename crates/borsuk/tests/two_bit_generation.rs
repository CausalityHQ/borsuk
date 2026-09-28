use borsuk::{
    two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits},
    two_bit_source::TwoBitSource,
    unit_centroid_graph::UnitCentroidGraph,
    unit_centroid_pages::UnitCentroidPages,
};
use sha2::{Digest, Sha256};
use std::{fs, io::Cursor};
fn hash(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}

#[tokio::test]
async fn pinned_generation_reloads_plans_without_pq_and_rejects_corruption_or_budget() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let raw = (0..512)
        .flat_map(|_| [0.5_f32, 0.25].into_iter().flat_map(f32::to_le_bytes))
        .collect::<Vec<_>>();
    let sq8 = (0..512_i64)
        .flat_map(|id| {
            let mut b = id.to_le_bytes().to_vec();
            b.extend_from_slice(&0.3125_f32.to_le_bytes());
            b.extend_from_slice(&[128, 64]);
            b
        })
        .collect::<Vec<_>>();
    let raw_path = root.join("raw");
    let sq8_path = root.join("sq8");
    fs::write(&raw_path, &raw).unwrap();
    fs::write(&sq8_path, &sq8).unwrap();
    let sq8_sha = hash(&sq8);
    TwoBitSource {
        raw: &raw_path,
        raw_sha256: &hash(&raw),
        sq8: &sq8_path,
        sq8_sha256: &sq8_sha,
        rows: 512,
        dimensions: 2,
    }
    .build(&root.join("plane"), 1_000_000)
    .unwrap();
    let centroid = UnitCentroidPages::build_from_sq8_reader(
        &mut Cursor::new(&sq8),
        512,
        2,
        32,
        256,
        &[0.; 2],
        &[1. / 255.; 2],
    )
    .unwrap();
    let centers = UnitCentroidPages::decode(&centroid).unwrap();
    let graph = UnitCentroidGraph::build(&centers, &centroid)
        .unwrap()
        .encode()
        .unwrap();
    let graph_resident = UnitCentroidGraph::preflight_resident_bytes(&graph, &centers).unwrap();
    let sidecar = sq8
        .chunks(256 * 14)
        .flat_map(|b| Sha256::digest(b).to_vec())
        .collect::<Vec<_>>();
    let page_manifest = serde_json::to_vec(
        &serde_json::json!({"schema":"borsuk-v115-sq8-page-authority-v2",
        "generation":1,"rows":512,"dimensions":2,"page_rows":256,"object_sha256":sq8_sha,
        "page_digest_sha256":hash(&sidecar)}),
    )
    .unwrap();
    for (name, b) in [
        ("centroids.bin", &centroid),
        ("graph.bin", &graph),
        ("page_digests.bin", &sidecar),
        ("page_manifest.json", &page_manifest),
    ] {
        fs::write(root.join(name), b).unwrap();
    }
    let manifest=serde_json::to_vec(&serde_json::json!({"schema":"borsuk-two-bit-generation-v1",
        "generation":1,"plane_manifest_sha256":hash(&fs::read(root.join("plane/manifest.json")).unwrap()),
        "page_manifest_sha256":hash(&page_manifest),"centroids_sha256":hash(&centroid),
        "graph_sha256":hash(&graph),"graph_resident_bytes":graph_resident,
        "sq8_object_sha256":sq8_sha,"sq8_object_key":format!("tenant/g1/objects/{sq8_sha}"),
        "sq8_etag":"etag-1","low":[0.,0.],"step":[1./255.,1./255.]})).unwrap();
    fs::write(root.join("manifest.json"), &manifest).unwrap();
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: 4_000_000,
        max_active_queries: 2,
        max_query_bytes: 16_384,
        max_query_gets: 2,
        max_parallel_gets: 2,
        max_query_scratch_bytes: 8192,
        already_pinned_bytes: 0,
    };
    let generation = TwoBitGeneration::open(root, &hash(&manifest), limits).unwrap();
    let first = generation.plan(&[0.5, 0.25]).await.unwrap();
    assert_eq!(first.planned_bytes, 7168);
    assert_eq!(first.ranges.len(), 1);
    let reloaded = TwoBitGeneration::open(root, &hash(&manifest), limits).unwrap();
    assert_eq!(
        first.ranges,
        reloaded.plan(&[0.5, 0.25]).await.unwrap().ranges
    );
    assert!(!root.join("router").exists());
    assert!(TwoBitGeneration::open(root, &"0".repeat(64), limits).is_err());
    assert!(TwoBitGeneration::open(
        root,
        &hash(&manifest),
        TwoBitGenerationLimits {
            max_memory_bytes: 1,
            ..limits
        }
    )
    .is_err());
    for field in ["generation", "sq8_object_sha256"] {
        let mut wrong: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        if field == "generation" {
            wrong[field] = 2.into();
        } else {
            wrong[field] = "0".repeat(64).into();
            wrong["sq8_object_key"] = format!("tenant/g1/objects/{}", "0".repeat(64)).into();
        }
        let body = serde_json::to_vec(&wrong).unwrap();
        fs::write(root.join("manifest.json"), &body).unwrap();
        assert!(TwoBitGeneration::open(root, &hash(&body), limits).is_err());
    }
    fs::write(root.join("manifest.json"), &manifest).unwrap();
    for name in [
        "centroids.bin",
        "graph.bin",
        "page_digests.bin",
        "plane/records.bin",
    ] {
        let original = fs::read(root.join(name)).unwrap();
        let mut bad = original.clone();
        bad[0] ^= 1;
        fs::write(root.join(name), bad).unwrap();
        assert!(TwoBitGeneration::open(root, &hash(&manifest), limits).is_err());
        fs::write(root.join(name), original).unwrap();
    }
}
