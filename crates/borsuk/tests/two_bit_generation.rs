use borsuk::two_bit_build::TwoBitGenerationBuilder;
use borsuk::two_bit_store::{publish_two_bit_generation, read_two_bit_head};
use borsuk::{
    two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits},
    two_bit_source::TwoBitSource,
    unit_centroid_graph::UnitCentroidGraph,
    unit_centroid_pages::UnitCentroidPages,
};
use futures_util::StreamExt;
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};
use sha2::{Digest, Sha256};
use std::{fs, io::Cursor};
fn hash(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}

// Removing the approved-root check, full-body authentication, create-only head,
// or the SQ8-only rewrite must break these real retained-publication falsifiers.
const RETAINED_METADATA: [&str; 10] = [
    "manifest.json",
    "page_manifest.json",
    "page_digests.bin",
    "plane/manifest.json",
    "plane/mean.bin",
    "plane/records.bin",
    "plane/page_digests.bin",
    "router/root.bin",
    "router/membership.bin",
    "router/leaves.bin",
];

// ObjectPath::join appends one segment; fixed roster names include directories.
fn retained_metadata_location(prefix: &ObjectPath, name: &str) -> ObjectPath {
    name.split('/')
        .fold(prefix.clone(), |path, segment| path.join(segment))
}

// The real local Create either succeeds before a lost response or is raced by
// an identical competing Create immediately before the publisher's attempt.
#[derive(Debug)]
struct LostHeadResponse {
    inner: std::sync::Arc<dyn object_store::ObjectStore>,
    head: ObjectPath,
    competing_head: bool,
}

impl std::fmt::Display for LostHeadResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "LostHeadResponse({})", self.inner)
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl object_store::ObjectStore for LostHeadResponse {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let lose_response =
            *path == self.head && matches!(&options.mode, object_store::PutMode::Create);
        if lose_response && self.competing_head {
            self.inner
                .put_opts(
                    path,
                    payload.clone(),
                    object_store::PutOptions {
                        mode: object_store::PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
        }
        let result = self.inner.put_opts(path, payload, options).await?;
        if lose_response {
            return Err(object_store::Error::Generic {
                store: "LostHeadResponse",
                source: std::io::Error::other("committed head response lost").into(),
            });
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(path, options).await
    }
    async fn get_ranges(
        &self,
        path: &ObjectPath,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.inner.get_ranges(path, ranges).await
    }
    fn delete_stream(
        &self,
        paths: futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: object_store::RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

struct RetainedFixture {
    temp: tempfile::TempDir,
    original: std::sync::Arc<dyn object_store::ObjectStore>,
    copied: std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: ObjectPath,
    root_sha: String,
    old_sq8_key: ObjectPath,
    new_sq8_key: ObjectPath,
    original_etag: String,
    limits: TwoBitGenerationLimits,
}

impl RetainedFixture {
    async fn new() -> Self {
        use borsuk::semantic_unit_router::SemanticProfile;
        use object_store::{chunked::ChunkedStore, local::LocalFileSystem};
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        for directory in ["original", "copied", "scratch"] {
            fs::create_dir(temp.path().join(directory)).unwrap();
        }
        let original_files =
            Arc::new(LocalFileSystem::new_with_prefix(temp.path().join("original")).unwrap());
        let copied_files =
            Arc::new(LocalFileSystem::new_with_prefix(temp.path().join("copied")).unwrap());
        let original: Arc<dyn object_store::ObjectStore> =
            Arc::new(ChunkedStore::new(original_files.clone(), 8192));
        let copied: Arc<dyn object_store::ObjectStore> =
            Arc::new(ChunkedStore::new(copied_files.clone(), 8192));
        let dimensions = 1024;
        let rows = 64;
        let low = vec![-0.0; dimensions];
        let step = vec![1.0 / 255.0; dimensions];
        let mut raw = Vec::new();
        let mut sq8 = Vec::new();
        for row in 0..rows {
            let codes = (0..dimensions)
                .map(|d| ((row * 37 + d * 19) % 251 + 1) as u8)
                .collect::<Vec<_>>();
            let values = codes.iter().map(|&v| v as f32 / 255.0).collect::<Vec<_>>();
            raw.extend(values.iter().flat_map(|v| v.to_le_bytes()));
            sq8.extend((1000 + row as i64 * 17).to_le_bytes());
            sq8.extend(values.iter().map(|v| v * v).sum::<f32>().to_le_bytes());
            sq8.extend(codes);
        }
        let raw_path = temp.path().join("raw.f32");
        let sq8_path = temp.path().join("sq8.bin");
        fs::write(&raw_path, &raw).unwrap();
        fs::write(&sq8_path, &sq8).unwrap();
        let sq8_sha = hash(&sq8);
        let old_sq8_key = ObjectPath::from(format!("frozen/objects/{sq8_sha}"));
        let new_sq8_key = ObjectPath::from(format!("rebound/objects/{sq8_sha}"));
        let original_etag = original
            .put(&old_sq8_key, PutPayload::from(sq8))
            .await
            .unwrap()
            .e_tag
            .unwrap();
        let root_sha = TwoBitGenerationBuilder {
            base_epoch: 0,
            source: TwoBitSource {
                raw: &raw_path,
                raw_sha256: &hash(&raw),
                sq8: &sq8_path,
                sq8_sha256: &sq8_sha,
                rows,
                dimensions,
            },
            generation: 7,
            low: &low,
            step: &step,
            sq8_object_key: old_sq8_key.as_ref(),
            sq8_etag: &original_etag,
        }
        .build_with_semantic_profile(
            Some(&(0..rows as u64).collect::<Vec<_>>()),
            SemanticProfile::Native100k,
            &temp.path().join("build"),
            64_000_000,
        )
        .unwrap();
        let limits = TwoBitGenerationLimits {
            max_memory_bytes: 64_000_000,
            max_active_queries: 1,
            max_query_bytes: 1_048_576,
            max_query_gets: 32,
            max_parallel_gets: 1,
            max_source_bytes: 1_048_576,
            max_source_gets: 128,
            max_parallel_source_gets: 1,
            max_query_scratch_bytes: 400_000,
            // Caller-owned fixture buffers and comparison pins are explicit.
            already_pinned_bytes: 2_000_000,
        };
        let prefix = ObjectPath::from("retained/index");
        let head = publish_two_bit_generation(
            original.as_ref(),
            &prefix,
            &temp.path().join("build"),
            &root_sha,
            limits,
            None,
        )
        .await
        .unwrap();
        let metadata = head.metadata_prefix();
        assert!(
            original
                .head(&metadata.clone().join("centroids.bin"))
                .await
                .is_err()
        );
        let root: serde_json::Value =
            serde_json::from_slice(&fs::read(temp.path().join("build/manifest.json")).unwrap())
                .unwrap();
        let mut roster = RETAINED_METADATA
            .iter()
            .map(|name| retained_metadata_location(&metadata, name))
            .collect::<Vec<_>>();
        roster.extend([
            prefix.clone().join("head.json"),
            old_sq8_key.clone(),
            ObjectPath::from(root["canonical"]["object_key"].as_str().unwrap()),
        ]);
        // Copy exactly the published roster, never the builder's centroids.
        for location in roster {
            let source = original_files.path_to_filesystem(&location).unwrap();
            let destination = copied_files.path_to_filesystem(&location).unwrap();
            fs::create_dir_all(destination.parent().unwrap()).unwrap();
            fs::copy(&source, &destination).unwrap_or_else(|error| {
                panic!("copy {location} from {source:?} to {destination:?}: {error}")
            });
        }
        let new_sq8_path = copied_files.path_to_filesystem(&new_sq8_key).unwrap();
        fs::create_dir_all(new_sq8_path.parent().unwrap()).unwrap();
        fs::copy(
            copied_files.path_to_filesystem(&old_sq8_key).unwrap(),
            new_sq8_path,
        )
        .unwrap();
        assert!(
            !temp
                .path()
                .join("copied")
                .join(metadata.as_ref())
                .join("centroids.bin")
                .exists()
        );
        assert_ne!(
            copied.head(&old_sq8_key).await.unwrap().e_tag.as_deref(),
            Some(original_etag.as_str())
        );
        Self {
            temp,
            original,
            copied,
            prefix,
            root_sha,
            old_sq8_key,
            new_sq8_key,
            original_etag,
            limits,
        }
    }

    fn scratch(&self) -> std::path::PathBuf {
        self.temp.path().join("scratch")
    }

    async fn head(&self) -> borsuk::two_bit_store::TwoBitHead {
        read_two_bit_head(self.copied.as_ref(), &self.prefix)
            .await
            .unwrap()
            .unwrap()
    }

    fn approval<'a>(&'a self, etag: &'a str) -> borsuk::two_bit_store::RetainedTwoBitApproval<'a> {
        borsuk::two_bit_store::RetainedTwoBitApproval {
            root_sha256: &self.root_sha,
            generation: 7,
            control_epoch: 1,
            sq8_object_key: self.new_sq8_key.as_ref(),
            sq8_etag: etag,
        }
    }
}

#[tokio::test]
async fn retained_semantic_republication_preserves_payload_and_score_bits() {
    use borsuk::two_bit_store::republish_retained_two_bit_generation;
    let fixture = RetainedFixture::new().await;
    let scratch = fixture.scratch();
    let original_head = read_two_bit_head(fixture.original.as_ref(), &fixture.prefix)
        .await
        .unwrap()
        .unwrap();
    let query = (0..1024)
        .map(|d| 0.25 + (d % 13) as f32 / 7.0)
        .collect::<Vec<_>>();
    let original = TwoBitGeneration::open_remote_from_head(
        fixture.original.as_ref(),
        &original_head,
        fixture.limits,
        &scratch,
    )
    .await
    .unwrap();
    let bits = |result: borsuk::two_bit_generation::TwoBitSearchResult| {
        result
            .ranked
            .candidates
            .into_iter()
            .map(|hit| (hit.id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    };
    let expected = bits(
        original
            .search_with_store(fixture.original.as_ref(), &query, 10, None)
            .await
            .unwrap(),
    );
    assert_eq!(expected.len(), 10);
    let copied_head = fixture.head().await;
    let old = TwoBitGeneration::open_remote_from_head(
        fixture.copied.as_ref(),
        &copied_head,
        fixture.limits,
        &scratch,
    )
    .await
    .unwrap();
    assert!(
        old.search_with_store(fixture.copied.as_ref(), &query, 10, None)
            .await
            .is_err()
    );
    drop(old);
    drop(original);
    let destination = ObjectPath::from("rebound/index");
    assert!(
        republish_retained_two_bit_generation(
            fixture.copied.as_ref(),
            &copied_head,
            fixture.approval(&fixture.original_etag),
            &destination,
            fixture.limits,
            &scratch,
            4_000_000
        )
        .await
        .is_err()
    );
    assert!(
        read_two_bit_head(fixture.copied.as_ref(), &destination)
            .await
            .unwrap()
            .is_none()
    );
    let etag = fixture
        .copied
        .head(&fixture.new_sq8_key)
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let rebound = republish_retained_two_bit_generation(
        fixture.copied.as_ref(),
        &copied_head,
        fixture.approval(&etag),
        &destination,
        fixture.limits,
        &scratch,
        4_000_000,
    )
    .await
    .unwrap();
    assert_eq!(rebound.generation(), 7);
    assert_eq!(rebound.control_epoch(), 1);
    assert_ne!(rebound.root_sha256(), fixture.root_sha);
    assert_eq!(fixture.head().await.root_sha256(), fixture.root_sha);
    assert_eq!(
        fs::read(
            fixture
                .temp
                .path()
                .join("copied")
                .join(fixture.prefix.as_ref())
                .join("head.json")
        )
        .unwrap(),
        fs::read(
            fixture
                .temp
                .path()
                .join("original")
                .join(fixture.prefix.as_ref())
                .join("head.json")
        )
        .unwrap(),
    );
    let before = fixture
        .copied
        .get(&copied_head.metadata_prefix().join("manifest.json"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let after = fixture
        .copied
        .get(&rebound.metadata_prefix().join("manifest.json"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let before_root: serde_json::Value = serde_json::from_slice(&before).unwrap();
    let mut after_root: serde_json::Value = serde_json::from_slice(&after).unwrap();
    assert_eq!(after_root["sq8_object_key"], fixture.new_sq8_key.as_ref());
    assert_eq!(after_root["sq8_etag"], etag);
    for field in ["low", "step"] {
        let values = |root: &serde_json::Value| {
            root[field]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| (v.as_f64().unwrap() as f32).to_bits())
                .collect::<Vec<_>>()
        };
        assert_eq!(values(&after_root), values(&before_root));
    }
    after_root["sq8_object_key"] = before_root["sq8_object_key"].clone();
    after_root["sq8_etag"] = before_root["sq8_etag"].clone();
    assert_eq!(after_root, before_root);
    for name in RETAINED_METADATA
        .into_iter()
        .filter(|name| *name != "manifest.json")
    {
        let old = fixture
            .copied
            .get(&retained_metadata_location(
                &copied_head.metadata_prefix(),
                name,
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let new = fixture
            .copied
            .get(&retained_metadata_location(
                &rebound.metadata_prefix(),
                name,
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(hash(&old), hash(&new), "{name}");
        assert_eq!(old, new, "{name}");
    }
    let canonical = ObjectPath::from(before_root["canonical"]["object_key"].as_str().unwrap());
    for location in [&canonical, &fixture.old_sq8_key] {
        let old = fixture
            .original
            .get(location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let retained = fixture
            .copied
            .get(location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(old, retained);
    }
    let new_sq8 = fixture
        .copied
        .get(&fixture.new_sq8_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        hash(&new_sq8),
        before_root["sq8_object_sha256"].as_str().unwrap()
    );
    let open = TwoBitGeneration::open_remote_from_head(
        fixture.copied.as_ref(),
        &rebound,
        fixture.limits,
        &scratch,
    )
    .await
    .unwrap();
    assert_eq!(
        bits(
            open.search_with_store(fixture.copied.as_ref(), &query, 10, None)
                .await
                .unwrap()
        ),
        expected
    );
    drop(open);
    // Copying can also preserve the SQ8 key while changing only its local ETag.
    let same_key_etag = fixture
        .copied
        .head(&fixture.old_sq8_key)
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let mut same_key_approval = fixture.approval(&same_key_etag);
    same_key_approval.sq8_object_key = fixture.old_sq8_key.as_ref();
    let same_key = republish_retained_two_bit_generation(
        fixture.copied.as_ref(),
        &copied_head,
        same_key_approval,
        &ObjectPath::from("etag-only/index"),
        fixture.limits,
        &scratch,
        4_000_000,
    )
    .await
    .unwrap();
    let same_key_generation = TwoBitGeneration::open_remote_from_head(
        fixture.copied.as_ref(),
        &same_key,
        fixture.limits,
        &scratch,
    )
    .await
    .unwrap();
    assert_eq!(
        bits(
            same_key_generation
                .search_with_store(fixture.copied.as_ref(), &query, 10, None)
                .await
                .unwrap()
        ),
        expected
    );
    drop(same_key_generation);
    for (prefix, competing_head) in [("lost-response/index", false), ("raced-head/index", true)] {
        let lost_prefix = ObjectPath::from(prefix);
        let lost = LostHeadResponse {
            inner: fixture.copied.clone(),
            head: lost_prefix.clone().join("head.json"),
            competing_head,
        };
        let result = republish_retained_two_bit_generation(
            &lost,
            &copied_head,
            fixture.approval(&etag),
            &lost_prefix,
            fixture.limits,
            &scratch,
            4_000_000,
        )
        .await;
        if competing_head {
            assert!(matches!(
                result,
                Err(borsuk::two_bit_store::TwoBitStoreError::Store(
                    object_store::Error::AlreadyExists { .. }
                ))
            ));
        } else {
            assert!(matches!(
                result,
                Err(borsuk::two_bit_store::TwoBitStoreError::Store(
                    object_store::Error::Generic {
                        store: "LostHeadResponse",
                        ..
                    }
                ))
            ));
        }
        let committed = read_two_bit_head(fixture.copied.as_ref(), &lost_prefix)
            .await
            .unwrap()
            .unwrap();
        let committed_generation = TwoBitGeneration::open_remote_from_head(
            fixture.copied.as_ref(),
            &committed,
            fixture.limits,
            &scratch,
        )
        .await
        .unwrap();
        assert_eq!(
            bits(
                committed_generation
                    .search_with_store(fixture.copied.as_ref(), &query, 10, None)
                    .await
                    .unwrap()
            ),
            expected
        );
        drop(committed_generation);
        // An identical already committed head still refuses; no implicit recovery.
        assert!(
            republish_retained_two_bit_generation(
                fixture.copied.as_ref(),
                &copied_head,
                fixture.approval(&etag),
                &lost_prefix,
                fixture.limits,
                &scratch,
                4_000_000
            )
            .await
            .is_err()
        );
    }
    // The new publication must survive loss of construction and old control/root.
    fs::remove_dir_all(fixture.temp.path().join("build")).unwrap();
    fs::remove_dir_all(fixture.temp.path().join("original")).unwrap();
    fs::remove_file(fixture.temp.path().join("raw.f32")).unwrap();
    fs::remove_file(fixture.temp.path().join("sq8.bin")).unwrap();
    for name in RETAINED_METADATA {
        fixture
            .copied
            .delete(&retained_metadata_location(
                &copied_head.metadata_prefix(),
                name,
            ))
            .await
            .unwrap();
    }
    fixture
        .copied
        .delete(&fixture.prefix.clone().join("head.json"))
        .await
        .unwrap();
    let reopened = read_two_bit_head(fixture.copied.as_ref(), &destination)
        .await
        .unwrap()
        .unwrap();
    let generation = TwoBitGeneration::open_remote_from_head(
        fixture.copied.as_ref(),
        &reopened,
        fixture.limits,
        &scratch,
    )
    .await
    .unwrap();
    assert_eq!(
        bits(
            generation
                .search_with_store(fixture.copied.as_ref(), &query, 10, None)
                .await
                .unwrap()
        ),
        expected
    );
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    for prefix in ["lost-response/index", "raced-head/index"] {
        let committed = read_two_bit_head(fixture.copied.as_ref(), &ObjectPath::from(prefix))
            .await
            .unwrap()
            .unwrap();
        let committed_generation = TwoBitGeneration::open_remote_from_head(
            fixture.copied.as_ref(),
            &committed,
            fixture.limits,
            &scratch,
        )
        .await
        .unwrap();
        assert_eq!(
            bits(
                committed_generation
                    .search_with_store(fixture.copied.as_ref(), &query, 10, None)
                    .await
                    .unwrap()
            ),
            expected
        );
    }
}

#[tokio::test]
async fn retained_semantic_republication_refuses_unapproved_changed_or_unbounded_inputs() {
    use borsuk::two_bit_store::republish_retained_two_bit_generation;
    let fixture = RetainedFixture::new().await;
    let scratch = fixture.scratch();
    let destination = ObjectPath::from("refused/index");
    let etag = fixture
        .copied
        .head(&fixture.new_sq8_key)
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let wrong_sha = "0".repeat(64);
    let nested = fixture.prefix.clone().join("nested");
    assert!(matches!(
        republish_retained_two_bit_generation(
            fixture.copied.as_ref(),
            &fixture.head().await,
            fixture.approval(&etag),
            &nested,
            fixture.limits,
            &scratch,
            4_000_000,
        )
        .await,
        Err(borsuk::two_bit_store::TwoBitStoreError::Invalid(
            "retained approval/destination"
        ))
    ));
    assert!(
        read_two_bit_head(fixture.copied.as_ref(), &nested)
            .await
            .unwrap()
            .is_none()
    );
    for fault in [
        "sha",
        "generation",
        "epoch",
        "etag",
        "weak",
        "wildcard",
        "key",
        "memory",
        "pins",
        "scratch",
        "zero-scratch",
    ] {
        let mut approval = fixture.approval(&etag);
        let mut limits = fixture.limits;
        let mut scratch_cap = 4_000_000;
        match fault {
            "sha" => approval.root_sha256 = &wrong_sha,
            "generation" => approval.generation += 1,
            "epoch" => approval.control_epoch += 1,
            "etag" => approval.sq8_etag = "wrong-etag",
            "weak" => approval.sq8_etag = "W/weak",
            "wildcard" => approval.sq8_etag = "*",
            "key" => approval.sq8_object_key = "bad/objects/wrong-sha",
            "memory" => limits.max_memory_bytes = 1,
            "pins" => limits.already_pinned_bytes = limits.max_memory_bytes,
            "scratch" => scratch_cap = 65536,
            _ => scratch_cap = 0,
        }
        assert!(
            republish_retained_two_bit_generation(
                fixture.copied.as_ref(),
                &fixture.head().await,
                approval,
                &destination,
                limits,
                &scratch,
                scratch_cap
            )
            .await
            .is_err(),
            "{fault}"
        );
        assert!(
            read_two_bit_head(fixture.copied.as_ref(), &destination)
                .await
                .unwrap()
                .is_none(),
            "{fault}"
        );
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    }
    let retained = fixture.head().await;
    let long_prefix = ObjectPath::from("x".repeat(60_000));
    let long_prefix_result = republish_retained_two_bit_generation(
        fixture.copied.as_ref(),
        &retained,
        fixture.approval(&etag),
        &long_prefix,
        TwoBitGenerationLimits {
            max_memory_bytes: 12_000_000,
            ..fixture.limits
        },
        &scratch,
        4_000_000,
    )
    .await;
    assert!(matches!(
        long_prefix_result,
        Err(borsuk::two_bit_store::TwoBitStoreError::Invalid(
            "publication memory"
        ))
    ));
    let root: serde_json::Value = serde_json::from_slice(
        &fixture
            .copied
            .get(&retained.metadata_prefix().join("manifest.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    let mut objects = RETAINED_METADATA
        .iter()
        .map(|name| retained_metadata_location(&retained.metadata_prefix(), name))
        .collect::<Vec<_>>();
    objects.extend([
        fixture.old_sq8_key.clone(),
        fixture.new_sq8_key.clone(),
        ObjectPath::from(root["canonical"]["object_key"].as_str().unwrap()),
    ]);
    for location in objects {
        let original = fixture
            .copied
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        for fault in ["sha", "truncated", "grown"] {
            let mut bad = original.to_vec();
            match fault {
                "sha" => bad[0] ^= 1,
                "truncated" => {
                    bad.pop();
                }
                _ => bad.push(0),
            }
            fixture
                .copied
                .put(&location, PutPayload::from(bad))
                .await
                .unwrap();
            // Read head before corrupting the root; all other cases use current authority.
            let current = fixture
                .copied
                .head(&fixture.new_sq8_key)
                .await
                .unwrap()
                .e_tag
                .unwrap();
            assert!(
                republish_retained_two_bit_generation(
                    fixture.copied.as_ref(),
                    &retained,
                    fixture.approval(&current),
                    &destination,
                    fixture.limits,
                    &scratch,
                    4_000_000
                )
                .await
                .is_err(),
                "{location}: {fault}"
            );
            assert!(
                read_two_bit_head(fixture.copied.as_ref(), &destination)
                    .await
                    .unwrap()
                    .is_none()
            );
            fixture
                .copied
                .put(&location, PutPayload::from(original.clone()))
                .await
                .unwrap();
            assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
        }
    }
    let head_key = fixture.prefix.clone().join("head.json");
    let head_bytes = fixture
        .copied
        .get(&head_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    for fault in ["unknown-page-tree", "nested-page-scalar"] {
        let original_page = fixture
            .copied
            .get(&retained.metadata_prefix().join("page_manifest.json"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut page: serde_json::Value = serde_json::from_slice(&original_page).unwrap();
        page[if fault == "unknown-page-tree" {
            "unknown"
        } else {
            "rows"
        }] = serde_json::json!({"nested":[{"more":[0]}]});
        let page = serde_json::to_vec(&page).unwrap();
        let mut changed_root = root.clone();
        changed_root["page_manifest_sha256"] = hash(&page).into();
        let changed_root = serde_json::to_vec(&changed_root).unwrap();
        let approved_sha = hash(&changed_root);
        let changed_prefix = fixture
            .prefix
            .clone()
            .join("generations")
            .join(approved_sha.as_str());
        for name in RETAINED_METADATA {
            let body = match name {
                "manifest.json" => PutPayload::from(changed_root.clone()),
                "page_manifest.json" => PutPayload::from(page.clone()),
                _ => PutPayload::from(
                    fixture
                        .copied
                        .get(&retained_metadata_location(
                            &retained.metadata_prefix(),
                            name,
                        ))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                ),
            };
            fixture
                .copied
                .put(&retained_metadata_location(&changed_prefix, name), body)
                .await
                .unwrap();
        }
        let mut control: serde_json::Value = serde_json::from_slice(&head_bytes).unwrap();
        control["root_sha256"] = approved_sha.clone().into();
        fixture
            .copied
            .put(
                &head_key,
                PutPayload::from(serde_json::to_vec(&control).unwrap()),
            )
            .await
            .unwrap();
        let current_head = fixture.head().await;
        let current_etag = fixture
            .copied
            .head(&fixture.new_sq8_key)
            .await
            .unwrap()
            .e_tag
            .unwrap();
        let mut approval = fixture.approval(&current_etag);
        approval.root_sha256 = &approved_sha;
        let refused = republish_retained_two_bit_generation(
            fixture.copied.as_ref(),
            &current_head,
            approval,
            &destination,
            fixture.limits,
            &scratch,
            4_000_000,
        )
        .await;
        assert!(
            matches!(
                refused,
                Err(borsuk::two_bit_store::TwoBitStoreError::Invalid(
                    "retained page schema"
                ))
            ),
            "{fault}"
        );
        assert!(
            read_two_bit_head(fixture.copied.as_ref(), &destination)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
        fixture
            .copied
            .put(&head_key, PutPayload::from(head_bytes.clone()))
            .await
            .unwrap();
    }
    for fault in ["mutation", "fence", "epoch"] {
        let mut retained = fixture.head().await;
        let mut control: serde_json::Value = serde_json::from_slice(&head_bytes).unwrap();
        match fault {
            "mutation" => {
                control["mutation"] =
                    serde_json::json!({"revision":1,"sha256":wrong_sha,"sealed":true})
            }
            "fence" => control["fence"] = "1".repeat(32).into(),
            _ => control["epoch"] = 2.into(),
        }
        fixture
            .copied
            .put(
                &head_key,
                PutPayload::from(serde_json::to_vec(&control).unwrap()),
            )
            .await
            .unwrap();
        if fault == "mutation" {
            // A fresh token must reach the mutation predicate, not the ETag guard.
            retained = fixture.head().await;
        }
        let refused = republish_retained_two_bit_generation(
            fixture.copied.as_ref(),
            &retained,
            fixture.approval(
                &fixture
                    .copied
                    .head(&fixture.new_sq8_key)
                    .await
                    .unwrap()
                    .e_tag
                    .unwrap(),
            ),
            &destination,
            fixture.limits,
            &scratch,
            4_000_000,
        )
        .await;
        assert!(
            matches!(
                refused,
                Err(borsuk::two_bit_store::TwoBitStoreError::Invalid(
                    "retained control changed"
                ))
            ),
            "{fault}"
        );
        assert!(
            read_two_bit_head(fixture.copied.as_ref(), &destination)
                .await
                .unwrap()
                .is_none()
        );
        fixture
            .copied
            .put(&head_key, PutPayload::from(head_bytes.clone()))
            .await
            .unwrap();
    }
    // A freshly authenticated copied head is insufficient without original approval.
    for fault in ["unapproved-root", "base-epoch"] {
        let mut changed = root.clone();
        if fault == "base-epoch" {
            changed["base_epoch"] = 1.into();
        } else {
            changed["low"][0] = 0.125.into();
        }
        let body = serde_json::to_vec(&changed).unwrap();
        let changed_sha = hash(&body);
        let changed_prefix = fixture
            .prefix
            .clone()
            .join("generations")
            .join(changed_sha.as_str());
        fixture
            .copied
            .put(
                &changed_prefix.join("manifest.json"),
                PutPayload::from(body),
            )
            .await
            .unwrap();
        let mut control: serde_json::Value = serde_json::from_slice(&head_bytes).unwrap();
        control["root_sha256"] = changed_sha.clone().into();
        fixture
            .copied
            .put(
                &head_key,
                PutPayload::from(serde_json::to_vec(&control).unwrap()),
            )
            .await
            .unwrap();
        let retained = fixture.head().await;
        let current = fixture
            .copied
            .head(&fixture.new_sq8_key)
            .await
            .unwrap()
            .e_tag
            .unwrap();
        let mut approval = fixture.approval(&current);
        if fault == "base-epoch" {
            approval.root_sha256 = &changed_sha;
        }
        let refused = republish_retained_two_bit_generation(
            fixture.copied.as_ref(),
            &retained,
            approval,
            &destination,
            fixture.limits,
            &scratch,
            4_000_000,
        )
        .await;
        if fault == "base-epoch" {
            assert!(matches!(
                refused,
                Err(borsuk::two_bit_store::TwoBitStoreError::Invalid(
                    "retained initial semantic generation"
                ))
            ));
        } else {
            assert!(refused.is_err(), "{fault}");
        }
        assert!(
            read_two_bit_head(fixture.copied.as_ref(), &destination)
                .await
                .unwrap()
                .is_none()
        );
        fixture
            .copied
            .put(&head_key, PutPayload::from(head_bytes.clone()))
            .await
            .unwrap();
    }
    fixture
        .copied
        .put(
            &destination.clone().join("occupied"),
            PutPayload::from("occupied"),
        )
        .await
        .unwrap();
    let retained = fixture.head().await;
    let current = fixture
        .copied
        .head(&fixture.new_sq8_key)
        .await
        .unwrap()
        .e_tag
        .unwrap();
    assert!(
        republish_retained_two_bit_generation(
            fixture.copied.as_ref(),
            &retained,
            fixture.approval(&current),
            &destination,
            fixture.limits,
            &scratch,
            4_000_000
        )
        .await
        .is_err()
    );
    assert!(
        read_two_bit_head(fixture.copied.as_ref(), &destination)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .copied
            .get(&destination.clone().join("occupied"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"occupied"
    );
    fixture
        .copied
        .delete(&destination.clone().join("occupied"))
        .await
        .unwrap();
    // Race real local control after the first destination metadata object appears.
    let marker = fixture
        .temp
        .path()
        .join("copied")
        .join(destination.as_ref());
    // The occupied-object check created an empty local directory. Remove it so
    // this race waits for actual publication staging, rather than old scratch.
    if marker.exists() {
        fs::remove_dir_all(&marker).unwrap();
    }
    let mut raced_control: serde_json::Value = serde_json::from_slice(&head_bytes).unwrap();
    raced_control["epoch"] = 2.into();
    let raced_control = serde_json::to_vec(&raced_control).unwrap();
    let control_path = fixture.temp.path().join("copied").join(head_key.as_ref());
    let race = async {
        loop {
            if marker.exists() {
                fs::write(&control_path, &raced_control).unwrap();
                break;
            }
            tokio::task::yield_now().await;
        }
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            republish_retained_two_bit_generation(
                fixture.copied.as_ref(),
                &retained,
                fixture.approval(&current),
                &destination,
                fixture.limits,
                &scratch,
                4_000_000
            ),
            race
        )
    })
    .await
    .unwrap();
    assert!(result.is_err());
    assert!(
        read_two_bit_head(fixture.copied.as_ref(), &destination)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    assert!(
        fixture
            .copied
            .list(Some(&destination))
            .next()
            .await
            .is_none()
    );
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
