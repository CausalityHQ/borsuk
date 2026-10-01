use borsuk::two_bit_build::TwoBitGenerationBuilder;
use borsuk::two_bit_store::{publish_two_bit_generation, read_two_bit_head};
use borsuk::{
    two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits},
    two_bit_source::TwoBitSource,
    unit_centroid_graph::UnitCentroidGraph,
    unit_centroid_pages::UnitCentroidPages,
};
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};
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
    let diverse_graph = UnitCentroidGraph::build_diverse(&centers, &centroid)
        .unwrap()
        .encode()
        .unwrap();
    let diverse_graph_resident =
        UnitCentroidGraph::preflight_resident_bytes(&diverse_graph, &centers).unwrap();
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
        ("diverse_graph.bin", &diverse_graph),
        ("page_digests.bin", &sidecar),
        ("page_manifest.json", &page_manifest),
    ] {
        fs::write(root.join(name), b).unwrap();
    }
    let canonical = (0..512_i64)
        .flat_map(|id| {
            let mut b = id.to_le_bytes().to_vec();
            let norm = (0.5_f64 * 0.5 + 0.25_f64 * 0.25).sqrt();
            for v in [0.5_f64, 0.25] {
                b.extend_from_slice(&((v / norm) as f32).to_le_bytes());
            }
            b
        })
        .collect::<Vec<_>>();
    fs::write(root.join("canonical.bin"), &canonical).unwrap();
    let canonical_descriptor = serde_json::json!({"rows":512,"dimensions":2,
        "bytes":canonical.len(),"sha256":hash(&canonical),"object_key":format!("tenant/g1/objects/{}",hash(&canonical))});
    let manifest=serde_json::to_vec(&serde_json::json!({"schema":"borsuk-two-bit-generation-v8",
        "generation":1,"base_epoch":0,"plane_manifest_sha256":hash(&fs::read(root.join("plane/manifest.json")).unwrap()),
        "page_manifest_sha256":hash(&page_manifest),"discovery":{"mode":"graph","centroids_sha256":hash(&centroid),
        "graph_sha256":hash(&graph),"graph_resident_bytes":graph_resident,
        "diverse_graph_sha256":hash(&diverse_graph),"diverse_graph_resident_bytes":diverse_graph_resident},
        "sq8_object_sha256":sq8_sha,"sq8_object_key":format!("tenant/g1/objects/{sq8_sha}"),
        "sq8_etag":"etag-1","low":[0.,0.],"step":[1_f32/255.,1_f32/255.],"canonical":canonical_descriptor})).unwrap();
    fs::write(root.join("manifest.json"), &manifest).unwrap();
    let generated = tempfile::tempdir().unwrap();
    let generated_root = generated.path().join("generation");
    let builder = TwoBitGenerationBuilder {
        base_epoch: 0,
        source: TwoBitSource {
            raw: &raw_path,
            raw_sha256: &hash(&raw),
            sq8: &sq8_path,
            sq8_sha256: &sq8_sha,
            rows: 512,
            dimensions: 2,
        },
        generation: 1,
        low: &[0.; 2],
        step: &[1. / 255.; 2],
        sq8_object_key: &format!("tenant/g1/objects/{sq8_sha}"),
        sq8_etag: "etag-1",
    };
    assert!(builder.build(&generated_root, 1).is_err());
    assert!(!generated_root.exists());
    let built_sha = builder.build(&generated_root, 2_000_000).unwrap();
    assert_eq!(built_sha, hash(&manifest));
    for name in [
        "manifest.json",
        "canonical.bin",
        "page_manifest.json",
        "page_digests.bin",
        "centroids.bin",
        "graph.bin",
        "diverse_graph.bin",
        "plane/manifest.json",
        "plane/mean.bin",
        "plane/records.bin",
        "plane/page_digests.bin",
    ] {
        assert_eq!(
            fs::read(generated_root.join(name)).unwrap(),
            fs::read(root.join(name)).unwrap(),
            "{name}"
        );
    }
    assert!(builder.build(&generated_root, 2_000_000).is_err());
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: 4_000_000,
        max_active_queries: 2,
        max_query_bytes: 16_384,
        max_query_gets: 2,
        max_parallel_gets: 2,
        max_source_bytes: 64 * 1024 * 1024,
        max_source_gets: 128,
        max_parallel_source_gets: 16,
        max_query_scratch_bytes: 8192,
        already_pinned_bytes: 0,
    };
    let mut legacy: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    legacy["schema"] = "borsuk-two-bit-generation-v1".into();
    let legacy_bytes = serde_json::to_vec(&legacy).unwrap();
    fs::write(root.join("manifest.json"), &legacy_bytes).unwrap();
    assert!(TwoBitGeneration::open(root, &hash(&legacy_bytes), limits).is_err());
    fs::write(root.join("manifest.json"), &manifest).unwrap();
    for case in 0..3 {
        let mut malformed: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        match case {
            0 => malformed["canonical"]["rows"] = 513.into(),
            1 => {
                malformed["canonical"]["dimensions"] = 3.into();
                malformed["canonical"]["bytes"] = 10240.into();
            }
            _ => {
                malformed["canonical"]["sha256"] = "x".into();
                malformed["canonical"]["object_key"] = "tenant/g1/objects/x".into();
            }
        }
        let malformed_bytes = serde_json::to_vec(&malformed).unwrap();
        fs::write(root.join("manifest.json"), &malformed_bytes).unwrap();
        assert!(TwoBitGeneration::open(root, &hash(&malformed_bytes), limits).is_err());
    }
    fs::write(root.join("manifest.json"), &manifest).unwrap();
    let generation = TwoBitGeneration::open(root, &hash(&manifest), limits).unwrap();
    assert!(generation.remote_open_stats().is_none());
    let first = generation.plan(&[0.5, 0.25]).await.unwrap();
    let (diagnostic, trace) = generation.diagnostic_plan(&[0.5, 0.25]).await.unwrap();
    assert_eq!(diagnostic, first);
    assert_eq!(trace.ranked_candidate_pages, vec![0, 1]);
    let evaluated = trace
        .nomination_evaluated_units
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(evaluated.len(), trace.nomination_evaluated_units.len());
    assert_eq!(evaluated, (0..generation.rows().div_ceil(32)).collect());
    assert!(
        trace
            .ranked_candidate_pages
            .contains(&trace.discoveries[0].seed_page)
    );
    assert_eq!(trace.primary_page, trace.ranked_candidate_pages[0]);
    assert!(!trace.discoveries[0].seed_work_exhausted && !trace.discoveries[0].walk_work_exhausted);
    assert!(diagnostic.selected_pages.contains(&trace.primary_page));
    for (units, cap) in [
        (&trace.discoveries[0].seed_evaluated_units, 128),
        (&trace.discoveries[0].walk_evaluated_units, 1272),
    ] {
        assert!(!units.is_empty() && units.len() <= cap);
        assert!(units.iter().all(|&unit| unit < 16));
        assert_eq!(
            units.iter().collect::<std::collections::HashSet<_>>().len(),
            units.len()
        );
    }

    // Exactly admits this D2 codec (one packed byte), leaving no trace budget.
    let tight = TwoBitGeneration::open(
        root,
        &hash(&manifest),
        TwoBitGenerationLimits {
            max_query_scratch_bytes: (256 + 2) * std::mem::size_of::<f64>(),
            ..limits
        },
    )
    .unwrap();
    assert_eq!(tight.plan(&[0.5, 0.25]).await.unwrap(), first);
    assert!(matches!(
        tight.diagnostic_plan(&[0.5, 0.25]).await,
        Err(borsuk::two_bit_generation::TwoBitGenerationError::Plane(
            borsuk::two_bit_source::SourceBuildError::Codec(
                borsuk::rotated_two_bit::TwoBitError::MemoryBudget
            )
        ))
    ));
    let under_trace = TwoBitGeneration::open(
        root,
        &hash(&manifest),
        TwoBitGenerationLimits {
            max_query_scratch_bytes: borsuk::two_bit_generation::TwoBitPlanTrace::scratch_bytes(
                512,
            ) - 1,
            ..limits
        },
    )
    .unwrap();
    assert!(matches!(
        under_trace.diagnostic_plan(&[0.5, 0.25]).await,
        Err(borsuk::two_bit_generation::TwoBitGenerationError::Invalid(
            "diagnostic scratch"
        ))
    ));

    let requests = root.join("paired-requests.jsonl");
    let request_bytes = (0..64)
        .map(|ordinal| {
            format!(
                "{}\n",
                serde_json::json!({"query_ordinal":ordinal,"query":[0.5,0.25]})
            )
        })
        .collect::<String>();
    fs::write(&requests, &request_bytes).unwrap();
    let paired_output = root.join("paired-plans.jsonl");
    let command = std::process::Command::new(env!("CARGO_BIN_EXE_two_bit_plan_demo"))
        .args([
            root.to_str().unwrap(),
            &hash(&manifest),
            requests.to_str().unwrap(),
            &hash(request_bytes.as_bytes()),
            paired_output.to_str().unwrap(),
            "0",
            "64",
            "--trace",
            "--paired",
            root.to_str().unwrap(),
            &hash(&manifest),
        ])
        .output()
        .unwrap();
    assert!(
        command.status.success(),
        "paired replay missing: {}",
        String::from_utf8_lossy(&command.stderr)
    );
    let records = fs::read_to_string(&paired_output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 128);
    for (ordinal, pair) in records.chunks_exact(2).enumerate() {
        let arms = if ordinal % 2 == 0 {
            ["control", "candidate"]
        } else {
            ["candidate", "control"]
        };
        for (record, arm) in pair.iter().zip(arms) {
            assert_eq!(record["query_ordinal"], ordinal);
            assert_eq!(record["arm"], arm);
            assert!(record["route_wall_ns"].as_u64().is_some());
            assert!(record["route_process_cpu_ns"].as_u64().is_some());
        }
        for key in [
            "ranges",
            "planned_bytes",
            "ranked_candidate_pages",
            "selected_pages",
            "discoveries",
            "primary_page",
        ] {
            assert_eq!(pair[0][key], pair[1][key], "{key}");
        }
    }

    let ordinary_output = root.join("ordinary-plans.jsonl");
    let ordinary = std::process::Command::new(env!("CARGO_BIN_EXE_two_bit_plan_demo"))
        .args([
            root.to_str().unwrap(),
            &hash(&manifest),
            requests.to_str().unwrap(),
            &hash(request_bytes.as_bytes()),
            ordinary_output.to_str().unwrap(),
            "0",
            "64",
        ])
        .output()
        .unwrap();
    assert!(ordinary.status.success());
    let ordinary_records = fs::read_to_string(&ordinary_output).unwrap();
    assert_eq!(ordinary_records.lines().count(), 64);
    for (ordinal, line) in ordinary_records.lines().enumerate() {
        let record: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(record.as_object().unwrap().len(), 3);
        for key in ["query_ordinal", "ranges", "planned_bytes"] {
            assert_eq!(record[key], records[ordinal * 2][key]);
        }
    }
    let forbidden = root.join("forbidden-validation.jsonl");
    let rejected = std::process::Command::new(env!("CARGO_BIN_EXE_two_bit_plan_demo"))
        .args([
            root.to_str().unwrap(),
            &hash(&manifest),
            requests.to_str().unwrap(),
            &hash(request_bytes.as_bytes()),
            forbidden.to_str().unwrap(),
            "256",
            "64",
            "--trace",
            "--paired",
            root.to_str().unwrap(),
            &hash(&manifest),
        ])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("paired replay restricted"));
    assert!(!forbidden.exists());

    for query in [[5e29, 2.5e29], [5e-31, 2.5e-31]] {
        assert_eq!(first.ranges, generation.plan(&query).await.unwrap().ranges);
    }
    assert!(generation.plan(&[0., 0.]).await.is_err());
    assert!(generation.plan(&[f32::NAN, 1.]).await.is_err());
    assert!(generation.plan(&[1.]).await.is_err());
    assert_eq!(first.planned_bytes, 7168);
    assert_eq!(first.ranges.len(), 1);
    let reloaded = TwoBitGeneration::open(root, &hash(&manifest), limits).unwrap();
    assert_eq!(
        first.ranges,
        reloaded.plan(&[0.5, 0.25]).await.unwrap().ranges
    );
    assert!(!root.join("router").exists());
    let publication = tempfile::tempdir().unwrap();
    for name in [
        "manifest.json",
        "canonical.bin",
        "page_manifest.json",
        "page_digests.bin",
        "centroids.bin",
        "graph.bin",
        "diverse_graph.bin",
        "plane/manifest.json",
        "plane/mean.bin",
        "plane/records.bin",
        "plane/page_digests.bin",
    ] {
        let target = publication.path().join(name);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(root.join(name), target).unwrap();
    }
    let published_store = InMemory::new();
    let index_prefix = ObjectPath::from("tenant/index");
    let source_key = ObjectPath::from(format!("tenant/g1/objects/{sq8_sha}"));
    let source_put = published_store
        .put(&source_key, PutPayload::from(sq8.clone()))
        .await
        .unwrap();
    let mut prepared: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    prepared["sq8_etag"] = source_put.e_tag.unwrap().into();
    let prepared_bytes = serde_json::to_vec(&prepared).unwrap();
    fs::write(publication.path().join("manifest.json"), &prepared_bytes).unwrap();
    assert!(
        read_two_bit_head(&published_store, &index_prefix)
            .await
            .unwrap()
            .is_none()
    );
    let publish_limits = TwoBitGenerationLimits {
        max_memory_bytes: 32_000_000,
        ..limits
    };
    let pinned = publish_two_bit_generation(
        &published_store,
        &index_prefix,
        publication.path(),
        &hash(&prepared_bytes),
        publish_limits,
        None,
    )
    .await
    .unwrap();
    assert_eq!(pinned.generation(), 1);
    assert_eq!(
        pinned.metadata_prefix().to_string(),
        format!("tenant/index/generations/{}", pinned.root_sha256())
    );
    assert!(
        published_store
            .head(&ObjectPath::from(format!(
                "{}/plane/records.bin",
                pinned.metadata_prefix()
            )))
            .await
            .is_ok()
    );

    let head = read_two_bit_head(&published_store, &index_prefix)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pinned.root_sha256(), head.root_sha256());
    let live = TwoBitGeneration::open_remote(
        &published_store,
        &head.metadata_prefix(),
        head.root_sha256(),
        limits,
        publication.path(),
    )
    .await
    .unwrap();
    assert_eq!(
        first.ranges,
        live.plan_with_store(&published_store, &[0.5, 0.25])
            .await
            .unwrap()
            .0
            .ranges
    );
    let seal_limits = borsuk::two_bit_mutations::TwoBitMutationLimits {
        max_snapshot_bytes: 1024,
        max_memory_bytes: 32768,
    };
    for generation_id in [2, 3] {
        let mut page: serde_json::Value = serde_json::from_slice(&page_manifest).unwrap();
        page["generation"] = generation_id.into();
        let page_bytes = serde_json::to_vec(&page).unwrap();
        fs::write(publication.path().join("page_manifest.json"), &page_bytes).unwrap();
        prepared["generation"] = generation_id.into();
        prepared["page_manifest_sha256"] = hash(&page_bytes).into();
        let mut bytes = serde_json::to_vec(&prepared).unwrap();
        fs::write(publication.path().join("manifest.json"), &bytes).unwrap();
        if generation_id == 2 {
            assert!(
                publish_two_bit_generation(
                    &published_store,
                    &index_prefix,
                    publication.path(),
                    &hash(&bytes),
                    publish_limits,
                    Some(&pinned),
                )
                .await
                .is_err()
            );
            assert_eq!(
                read_two_bit_head(&published_store, &index_prefix)
                    .await
                    .unwrap()
                    .unwrap()
                    .generation(),
                1
            );
            let empty = borsuk::two_bit_mutations::seal_two_bit_mutations(
                &published_store,
                &pinned,
                None,
                seal_limits,
            )
            .await
            .unwrap();
            assert!(empty.is_sealed());
            assert!(empty.rows().is_empty());
            prepared["base_epoch"] = read_two_bit_head(&published_store, &index_prefix)
                .await
                .unwrap()
                .unwrap()
                .control_epoch()
                .into();
            bytes = serde_json::to_vec(&prepared).unwrap();
            fs::write(publication.path().join("manifest.json"), &bytes).unwrap();
            assert!(
                borsuk::two_bit_mutations::apply_two_bit_mutations(
                    &published_store,
                    &pinned,
                    2,
                    None,
                    &[borsuk::two_bit_mutations::TwoBitMutation {
                        id: 7,
                        vector: None
                    }],
                    seal_limits,
                )
                .await
                .is_err()
            );
        }
        let result = publish_two_bit_generation(
            &published_store,
            &index_prefix,
            publication.path(),
            &hash(&bytes),
            publish_limits,
            Some(&pinned),
        )
        .await;
        if generation_id == 2 {
            assert_eq!(result.unwrap().generation(), 2);
        } else {
            assert!(result.is_err());
        }
    }
    assert_eq!(
        read_two_bit_head(&published_store, &index_prefix)
            .await
            .unwrap()
            .unwrap()
            .generation(),
        2
    );
    let old_pinned = TwoBitGeneration::open_remote(
        &published_store,
        &pinned.metadata_prefix(),
        pinned.root_sha256(),
        limits,
        publication.path(),
    )
    .await
    .unwrap();
    assert_eq!(
        first.ranges,
        old_pinned
            .plan_with_store(&published_store, &[0.5, 0.25])
            .await
            .unwrap()
            .0
            .ranges
    );
    assert_eq!(
        published_store
            .head(&source_key)
            .await
            .unwrap()
            .e_tag
            .as_deref(),
        prepared["sq8_etag"].as_str()
    );
    assert!(
        publish_two_bit_generation(
            &published_store,
            &ObjectPath::from("another/index"),
            publication.path(),
            &hash(&fs::read(publication.path().join("manifest.json")).unwrap()),
            publish_limits,
            Some(&pinned)
        )
        .await
        .is_err()
    );
    let current = read_two_bit_head(&published_store, &index_prefix)
        .await
        .unwrap()
        .unwrap();
    let records_path = publication.path().join("plane/records.bin");
    let mut bad = fs::read(&records_path).unwrap();
    bad[0] ^= 1;
    fs::write(&records_path, bad).unwrap();
    assert!(
        publish_two_bit_generation(
            &published_store,
            &index_prefix,
            publication.path(),
            &hash(&fs::read(publication.path().join("manifest.json")).unwrap()),
            publish_limits,
            Some(&current)
        )
        .await
        .is_err()
    );
    assert_eq!(
        read_two_bit_head(&published_store, &index_prefix)
            .await
            .unwrap()
            .unwrap()
            .root_sha256(),
        current.root_sha256()
    );

    // Only metadata exists remotely: an SQ8 payload GET would fail this open.
    let store = InMemory::new();
    let prefix = ObjectPath::from("tenant/g1/metadata");
    for name in [
        "manifest.json",
        "canonical.bin",
        "page_manifest.json",
        "page_digests.bin",
        "centroids.bin",
        "graph.bin",
        "diverse_graph.bin",
        "plane/manifest.json",
        "plane/mean.bin",
        "plane/records.bin",
        "plane/page_digests.bin",
    ] {
        store
            .put(
                &ObjectPath::from(format!("{prefix}/{name}")),
                PutPayload::from(fs::read(root.join(name)).unwrap()),
            )
            .await
            .unwrap();
    }
    let scratch = tempfile::tempdir().unwrap();
    let remote =
        TwoBitGeneration::open_remote(&store, &prefix, &hash(&manifest), limits, scratch.path())
            .await
            .unwrap();
    let stats = remote.remote_open_stats().unwrap();
    assert_eq!(stats.metadata.len(), 9);
    assert!(
        stats
            .metadata
            .iter()
            .all(|object| object.name != "plane/records.bin")
    );
    for object in &stats.metadata {
        assert_eq!(
            object.bytes,
            fs::metadata(root.join(&object.name)).unwrap().len()
        );
        assert!(object.chunks > 0);
        assert!(object.write_wall_ns <= object.stream_wall_ns);
    }
    assert!(
        stats
            .metadata
            .iter()
            .map(|entry| entry.get_wall_ns + entry.stream_wall_ns)
            .sum::<u128>()
            <= stats.staging_wall_ns
    );
    assert!(remote.plan(&[0.5, 0.25]).await.is_err());
    assert_eq!(stats.source_head_requests, 1);
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
    assert!(
        TwoBitGeneration::open_remote(&store, &prefix, &"0".repeat(64), limits, scratch.path())
            .await
            .is_err()
    );
    assert!(
        TwoBitGeneration::open_remote(
            &store,
            &prefix,
            &hash(&manifest),
            TwoBitGenerationLimits {
                max_memory_bytes: 1,
                ..limits
            },
            scratch.path()
        )
        .await
        .is_err()
    );
    store
        .put(
            &ObjectPath::from(format!("{prefix}/plane/records.bin")),
            PutPayload::from(vec![0; 4608]),
        )
        .await
        .unwrap();
    let corrupted_source =
        TwoBitGeneration::open_remote(&store, &prefix, &hash(&manifest), limits, scratch.path())
            .await
            .unwrap();
    assert!(
        corrupted_source
            .plan_with_store(&store, &[0.5, 0.25])
            .await
            .is_err()
    );
    store
        .delete(&ObjectPath::from(format!("{prefix}/plane/records.bin")))
        .await
        .unwrap();
    assert!(
        TwoBitGeneration::open_remote(&store, &prefix, &hash(&manifest), limits, scratch.path())
            .await
            .is_err()
    );
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);

    assert!(TwoBitGeneration::open(root, &"0".repeat(64), limits).is_err());
    assert!(
        TwoBitGeneration::open(
            root,
            &hash(&manifest),
            TwoBitGenerationLimits {
                max_memory_bytes: 1,
                ..limits
            }
        )
        .is_err()
    );
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
        "diverse_graph.bin",
        "page_digests.bin",
        "plane/records.bin",
        "plane/page_digests.bin",
    ] {
        let original = fs::read(root.join(name)).unwrap();
        let mut bad = original.clone();
        bad[0] ^= 1;
        fs::write(root.join(name), bad).unwrap();
        assert!(TwoBitGeneration::open(root, &hash(&manifest), limits).is_err());
        fs::write(root.join(name), original).unwrap();
    }
    let tiny = tempfile::tempdir().unwrap();
    let tiny_raw = tiny.path().join("raw");
    let tiny_sq8 = tiny.path().join("sq8");
    fs::write(&tiny_raw, &raw[..8]).unwrap();
    fs::write(&tiny_sq8, &sq8[..14]).unwrap();
    let tiny_sha = hash(&sq8[..14]);
    let tiny_root = tiny.path().join("generation");
    let tiny_builder = TwoBitGenerationBuilder {
        base_epoch: 0,
        source: TwoBitSource {
            raw: &tiny_raw,
            raw_sha256: &hash(&raw[..8]),
            sq8: &tiny_sq8,
            sq8_sha256: &tiny_sha,
            rows: 1,
            dimensions: 2,
        },
        generation: 1,
        low: &[0.; 2],
        step: &[1. / 255.; 2],
        sq8_object_key: &format!("tenant/tiny/objects/{tiny_sha}"),
        sq8_etag: "tiny-etag",
    };
    let tiny_root_sha = tiny_builder.build(&tiny_root, 2_000_000).unwrap();
    let tiny_generation = TwoBitGeneration::open(&tiny_root, &tiny_root_sha, limits).unwrap();
    let tiny_plan = tiny_generation.plan(&[0.5, 0.25]).await.unwrap();
    assert_eq!(tiny_plan.planned_bytes, 14);
    assert_eq!(tiny_plan.ranges, vec![0..14]);

    let mut zero_norm = sq8[..14].to_vec();
    zero_norm[8..12].copy_from_slice(&0_f32.to_le_bytes());
    fs::write(&tiny_sq8, &zero_norm).unwrap();
    let zero_sha = hash(&zero_norm);
    let bad_output = tiny.path().join("zero-norm-generation");
    let bad_builder = TwoBitGenerationBuilder {
        base_epoch: 0,
        source: TwoBitSource {
            sq8_sha256: &zero_sha,
            ..tiny_builder.source
        },
        ..tiny_builder
    };
    assert!(bad_builder.build(&bad_output, 2_000_000).is_err());
    assert!(!bad_output.join("manifest.json").exists());
}

#[test]
fn graph_variant_adapter_preserves_components_and_rejects_untrusted_roots() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("control");
    let raw_path = temp.path().join("raw.f32");
    let sq8_path = temp.path().join("sq8.bin");
    let mut raw = Vec::new();
    let mut sq8 = Vec::new();
    for id in 0..8224_i64 {
        let unit = id as usize / 32;
        let code = (0..8)
            .map(|d| ((unit * 37 + d * 71 + unit * d * 13) % 251 + 1) as u8)
            .collect::<Vec<_>>();
        let values = code.iter().map(|&c| c as f32 / 255.0).collect::<Vec<_>>();
        raw.extend(values.iter().flat_map(|v| v.to_le_bytes()));
        sq8.extend(id.to_le_bytes());
        sq8.extend(values.iter().map(|&v| v * v).sum::<f32>().to_le_bytes());
        sq8.extend(code);
    }
    fs::write(&raw_path, &raw).unwrap();
    fs::write(&sq8_path, &sq8).unwrap();
    let sha = TwoBitGenerationBuilder {
        base_epoch: 0,
        source: TwoBitSource {
            raw: &raw_path,
            raw_sha256: &hash(&raw),
            sq8: &sq8_path,
            sq8_sha256: &hash(&sq8),
            rows: 8224,
            dimensions: 8,
        },
        generation: 1,
        low: &[0.; 8],
        step: &[1.0 / 255.0; 8],
        sq8_object_key: &format!("fixture/objects/{}", hash(&sq8)),
        sq8_etag: "fixture-etag",
    }
    .build(&root, 64_000_000)
    .unwrap();
    let names = [
        "manifest.json",
        "page_manifest.json",
        "page_digests.bin",
        "centroids.bin",
        "graph.bin",
        "diverse_graph.bin",
        "plane/manifest.json",
        "plane/mean.bin",
        "plane/records.bin",
        "plane/page_digests.bin",
    ];
    let before = names.map(|name| fs::read(root.join(name)).unwrap());
    let candidate = temp.path().join("candidate");
    let invoke = |trusted: &str, output: &std::path::Path| {
        std::process::Command::new(env!("CARGO_BIN_EXE_build_two_bit_graph_variant"))
            .args([root.to_str().unwrap(), trusted, output.to_str().unwrap()])
            .output()
            .unwrap()
    };
    let result = invoke(&sha, &candidate);
    assert!(
        result.status.success(),
        "graph adapter missing: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["build"]["rows"], 8224);
    let control_degrees = report["build"]["control"]["node_layer_degrees"]
        .as_array()
        .unwrap();
    let candidate_degrees = report["build"]["candidate"]["node_layer_degrees"]
        .as_array()
        .unwrap();
    assert_eq!(control_degrees.len(), 257);
    assert_eq!(candidate_degrees.len(), 257);
    for (control, candidate) in control_degrees.iter().zip(candidate_degrees) {
        assert_eq!(
            control.as_array().unwrap().len(),
            candidate.as_array().unwrap().len()
        );
        for layers in [control, candidate] {
            let layers = layers.as_array().unwrap();
            assert!(!layers.is_empty() && layers.len() <= 17);
            for (index, degree) in layers.iter().enumerate() {
                assert!(
                    degree.as_u64().unwrap() <= if index + 1 == layers.len() { 32 } else { 16 }
                );
            }
        }
    }
    let control_manifest: serde_json::Value = serde_json::from_slice(&before[0]).unwrap();
    let candidate_bytes = fs::read(candidate.join("manifest.json")).unwrap();
    let mut candidate_manifest: serde_json::Value =
        serde_json::from_slice(&candidate_bytes).unwrap();
    assert_ne!(
        candidate_manifest["discovery"]["graph_sha256"],
        control_manifest["discovery"]["graph_sha256"]
    );
    for field in ["graph_sha256", "graph_resident_bytes"] {
        candidate_manifest["discovery"][field] = control_manifest["discovery"][field].clone();
    }
    assert_eq!(candidate_manifest, control_manifest);
    for (name, bytes) in names.into_iter().zip(&before) {
        assert_eq!(fs::read(root.join(name)).unwrap(), *bytes);
        if name != "manifest.json" && name != "graph.bin" {
            assert_eq!(fs::read(candidate.join(name)).unwrap(), *bytes);
        }
    }
    TwoBitGeneration::open(
        &candidate,
        &hash(&candidate_bytes),
        TwoBitGenerationLimits {
            max_memory_bytes: 64_000_000,
            max_active_queries: 1,
            max_query_bytes: 1_048_576,
            max_query_gets: 32,
            max_parallel_gets: 32,
            max_source_bytes: 64 * 1024 * 1024,
            max_source_gets: 128,
            max_parallel_source_gets: 16,
            max_query_scratch_bytes: 16_384,
            already_pinned_bytes: 0,
        },
    )
    .unwrap();
    assert!(!invoke(&sha, &candidate).status.success());
    let wrong = temp.path().join("wrong-root");
    assert!(!invoke(&"0".repeat(64), &wrong).status.success());
    assert!(!wrong.exists());
    fs::write(root.join("centroids.bin"), b"corrupt").unwrap();
    let corrupt = temp.path().join("corrupt-root");
    assert!(!invoke(&sha, &corrupt).status.success());
    assert!(!corrupt.exists());
}
