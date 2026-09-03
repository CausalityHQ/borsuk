//! Immutable PQ4 shard construction and bounded exact-row search.

#![forbid(unsafe_code)]

mod builder;
mod core;
mod format;
mod index;
mod sharded_index;
mod shards;

#[cfg(test)]
pub(crate) use format::{Pq4ArtifactIdentity, Pq4Manifest, canonical_manifest_bytes};
#[cfg(test)]
pub(crate) use snapshot::{Pq4Snapshot, Pq4SnapshotWriteRequest, write_snapshot};

pub use builder::{Pq4BuildConfig, Pq4BuildReport, Pq4Builder};
#[cfg(test)]
pub(crate) use index::search_with_exact_rerank_observer_for_test;
pub use index::{Pq4Index, Pq4Match, Pq4OpenOptions};
#[cfg(test)]
pub(crate) use sharded_index::search_with_shard_observer_for_test;
pub use sharded_index::{Pq4ShardedIndex, Pq4ShardedOpenOptions};
pub use shards::merge_pq4_shard_matches;

mod snapshot;

/// Errors returned by PQ4 construction and search contracts.
#[derive(Debug, thiserror::Error)]
pub enum BorsukError {
    /// An input, artifact, or metric violates the concrete PQ4 contract.
    #[error("{0}")]
    InvalidMetricInput(String),
    /// A required local storage or SIMD capability is unavailable.
    #[error("{0}")]
    InvalidStorage(String),
}

/// Result type for PQ4 operations.
pub type Result<T> = std::result::Result<T, BorsukError>;

#[cfg(all(test, not(target_arch = "aarch64")))]
pub(crate) use core::rank_candidates;
#[cfg(test)]
pub(crate) use core::{
    Pq4Codebook, encode_blocks, fit_codebook, projected_resident_bytes,
    rank_candidates_parallel_scalar_for_test, rank_candidates_scalar, score_rows_scalar,
    select_ranked_rows_with_histogram_for_test,
};

#[cfg(test)]
mod tests {
    #[cfg(not(target_arch = "aarch64"))]
    use super::rank_candidates;
    use super::{
        Pq4ArtifactIdentity, Pq4BuildConfig, Pq4Builder, Pq4Codebook, Pq4Index, Pq4Manifest,
        Pq4Match, Pq4OpenOptions, Pq4ShardedIndex, Pq4ShardedOpenOptions, Pq4Snapshot,
        Pq4SnapshotWriteRequest, canonical_manifest_bytes, encode_blocks, fit_codebook,
        merge_pq4_shard_matches, projected_resident_bytes,
        rank_candidates_parallel_scalar_for_test, rank_candidates_scalar, score_rows_scalar,
        search_with_exact_rerank_observer_for_test, search_with_shard_observer_for_test,
        select_ranked_rows_with_histogram_for_test, write_snapshot,
    };
    use std::{
        collections::BTreeSet,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow_array::{
        Array, ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{
        arrow::ArrowWriter,
        basic::Compression,
        file::properties::{WriterProperties, WriterVersion},
    };

    fn snapshot_identity(
        role: &str,
        file_name: &str,
        digest_byte: char,
        encoded_bytes: u64,
        row_count: u64,
        schema: &str,
    ) -> Pq4ArtifactIdentity {
        Pq4ArtifactIdentity {
            role: role.to_owned(),
            file_name: file_name.to_owned(),
            sha256: digest_byte.to_string().repeat(64),
            encoded_bytes,
            row_count,
            schema: schema.to_owned(),
        }
    }

    fn rows(count: usize) -> Vec<[f32; 96]> {
        (0..count)
            .map(|row| {
                std::array::from_fn(|dimension| {
                    let value = ((row * 37 + dimension * 19) % 257) as f32;
                    (value - 128.0) / 129.0
                })
            })
            .collect()
    }

    fn write_pq4_input(path: &std::path::Path, vectors: &[[f32; 96]], ids: &[Vec<u8>]) {
        let values = vectors.iter().flatten().copied().collect::<Vec<_>>();
        let vector_array = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
            Arc::new(Float32Array::from(values)),
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Binary, false),
            Field::new("vector", vector_array.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BinaryArray::from_iter_values(ids.iter().map(Vec::as_slice))) as ArrayRef,
                Arc::new(vector_array),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            schema,
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn write_sharded_fixture(root: &std::path::Path, shard_count: u32) -> Vec<(u32, PathBuf)> {
        let vectors = rows(4_096);
        (0..shard_count)
            .map(|shard_ordinal| {
                let input = root.join(format!("input-{shard_ordinal}.parquet"));
                let output = root.join(format!("snapshot-{shard_ordinal}"));
                let ids = (0..vectors.len())
                    .map(|source_ordinal| {
                        (u64::from(shard_ordinal) * 4_096 + source_ordinal as u64)
                            .to_le_bytes()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                write_pq4_input(&input, &vectors, &ids);
                Pq4Builder::build_parquet(
                    &input,
                    &output,
                    &Pq4BuildConfig {
                        generation: format!("pq4-100m-shard-{shard_ordinal:04}"),
                        source_uri: format!("s3://frozen/partition-{shard_ordinal:04}.parquet"),
                        batch_rows: 1_024,
                        worker_count: 2,
                    },
                )
                .unwrap();
                (shard_ordinal, output)
            })
            .collect()
    }

    fn sharded_options() -> Pq4ShardedOpenOptions {
        Pq4ShardedOpenOptions {
            memory_budget_bytes: 64 * 1024 * 1024,
            fanout_threads: 3,
            shard_query_threads: 2,
            admission_timeout_ms: 1_000,
        }
    }

    #[test]
    fn v26_pq4_100m_sharded_open_requires_contiguous_authority_and_aggregate_memory() {
        // Break caught: the deployment opens a gapped/duplicate shard set or admits each shard
        // against the full process budget independently and oversubscribes aggregate memory.
        let directory = tempfile::tempdir().unwrap();
        let shards = write_sharded_fixture(directory.path(), 3);
        assert!(Pq4ShardedIndex::open(&shards, sharded_options()).is_ok());

        let gapped = vec![shards[0].clone(), (2, shards[1].1.clone())];
        assert!(Pq4ShardedIndex::open(&gapped, sharded_options()).is_err());
        let duplicate = vec![shards[0].clone(), (0, shards[1].1.clone())];
        assert!(Pq4ShardedIndex::open(&duplicate, sharded_options()).is_err());
        let mut insufficient = sharded_options();
        insufficient.memory_budget_bytes = 1;
        assert!(Pq4ShardedIndex::open(&shards, insufficient).is_err());
    }

    #[test]
    fn v26_pq4_100m_sharded_search_merges_exact_local_results_deterministically() {
        // Break caught: shard completion order changes ties or local IDs are rewritten from their
        // global little-endian source authority during fan-out/merge.
        let directory = tempfile::tempdir().unwrap();
        let shards = write_sharded_fixture(directory.path(), 3);
        let index = Pq4ShardedIndex::open(&shards, sharded_options()).unwrap();
        let query = rows(4_096)[2_047];

        let first = index.search(&query, 10).unwrap();
        let second = index.search(&query, 10).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 10);
        assert!(first.windows(2).all(|pair| {
            pair[0]
                .squared_distance
                .total_cmp(&pair[1].squared_distance)
                .then_with(|| pair[0].shard_ordinal.cmp(&pair[1].shard_ordinal))
                .then_with(|| pair[0].source_ordinal.cmp(&pair[1].source_ordinal))
                .is_le()
        }));
        for item in &first {
            let global = u64::from_le_bytes(item.id.clone().try_into().unwrap());
            assert_eq!(
                global,
                u64::from(item.shard_ordinal) * 4_096 + item.source_ordinal
            );
        }
    }

    #[test]
    fn v26_pq4_100m_sharded_search_executes_every_shard_concurrently() {
        // Break caught: fan-out is accidentally replaced with a serial iterator, making 100M
        // latency approximately ten times the already measured 10M latency.
        let directory = tempfile::tempdir().unwrap();
        let shards = write_sharded_fixture(directory.path(), 3);
        let index = Pq4ShardedIndex::open(&shards, sharded_options()).unwrap();
        let active = AtomicUsize::new(0);
        let maximum = AtomicUsize::new(0);
        let query = rows(4_096)[2_047];

        let matches = search_with_shard_observer_for_test(&index, &query, 10, |_| {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            maximum.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(50));
            active.fetch_sub(1, Ordering::SeqCst);
        })
        .unwrap();

        assert_eq!(matches.len(), 10);
        assert_eq!(maximum.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn v26_pq4_100m_sharded_search_rejects_partial_results() {
        // Break caught: one unreadable shard is discarded and the coordinator publishes a top-k
        // from only the surviving corpus partitions.
        let directory = tempfile::tempdir().unwrap();
        let shards = write_sharded_fixture(directory.path(), 3);
        let index = Pq4ShardedIndex::open(&shards, sharded_options()).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(shards[1].1.join("vectors.arrow"))
            .unwrap();

        assert!(index.search(&rows(4_096)[2_047], 10).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_core_projection_and_training_are_deterministic() {
        assert_eq!(
            projected_resident_bytes(100_000_000).unwrap(),
            2_336_975_744
        );
        assert!(projected_resident_bytes(0).is_err());

        let rows = rows(64);
        let first = fit_codebook(&rows).unwrap();
        let second = fit_codebook(&rows).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.centroids.len(), 32);
        assert!(
            first
                .centroids
                .iter()
                .flatten()
                .all(|value| value.is_finite())
        );

        let mut invalid = rows.clone();
        invalid[3][17] = f32::NAN;
        assert!(fit_codebook(&invalid).is_err());
        assert!(fit_codebook(&[[0.0; 96]; 16]).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_core_blocks_preserve_nibbles_order_and_padding() {
        let codes = (0..35)
            .map(|row| std::array::from_fn(|subspace| ((row + subspace) % 16) as u8))
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        assert_eq!(blocks.len(), 2);
        for (row, code) in codes.iter().enumerate() {
            for (subspace, expected) in code.iter().enumerate() {
                let packed = blocks[row / 32][subspace * 16 + row % 32 / 2];
                let actual = if row % 2 == 0 {
                    packed & 15
                } else {
                    packed >> 4
                };
                assert_eq!(actual, *expected, "row {row}, subspace {subspace}");
            }
        }
        for row in 35..64 {
            for subspace in 0..32 {
                let packed = blocks[1][subspace * 16 + row % 32 / 2];
                let actual = if row % 2 == 0 {
                    packed & 15
                } else {
                    packed >> 4
                };
                assert_eq!(actual, 0, "padding row {row}, subspace {subspace}");
            }
        }

        let mut invalid = codes;
        invalid[4][9] = 16;
        assert!(encode_blocks(&invalid).is_err());
        assert!(encode_blocks(&[]).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_core_histogram_ranking_matches_literal_full_sort() {
        let rows = rows(640);
        let codebook: Pq4Codebook = fit_codebook(&rows).unwrap();
        let codes = rows
            .iter()
            .map(|row| codebook.encode(row).unwrap())
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        let query = rows[319];

        let actual = rank_candidates_scalar(&codebook, &blocks, rows.len(), &query, 512).unwrap();
        assert_eq!(actual.len(), 512);
        assert!(actual.windows(2).all(|pair| {
            (pair[0].score, pair[0].source_ordinal) <= (pair[1].score, pair[1].source_ordinal)
        }));

        let scores = score_rows_scalar(&codebook, &blocks, rows.len(), &query).unwrap();
        let mut expected = scores
            .into_iter()
            .enumerate()
            .map(|(source_ordinal, score)| (score, source_ordinal as u64))
            .collect::<Vec<_>>();
        expected.sort_unstable();
        expected.truncate(512);
        assert_eq!(
            actual
                .iter()
                .map(|row| (row.score, row.source_ordinal))
                .collect::<Vec<_>>(),
            expected
        );
        assert!(actual.iter().any(|row| row.source_ordinal == 319));
        assert!(rank_candidates_scalar(&codebook, &blocks, rows.len(), &query, 513).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_core_parallel_histogram_is_reused_for_selection() {
        // Break caught: the parallel scan discards its histogram and serially rebuilds it across
        // every corpus score before selecting the bounded candidate set.
        let scores = vec![7_u16, 2, 2, 5];
        let mut histogram = Box::new([0_u32; 8_192]);
        histogram[2] = 2;
        histogram[5] = 1;
        histogram[7] = 1;
        let ranked =
            select_ranked_rows_with_histogram_for_test(scores.clone(), histogram, 2).unwrap();
        assert_eq!(
            ranked
                .iter()
                .map(|row| (row.score, row.source_ordinal))
                .collect::<Vec<_>>(),
            vec![(2, 1), (2, 2)]
        );

        let mut incomplete = Box::new([0_u32; 8_192]);
        incomplete[2] = 2;
        assert!(select_ranked_rows_with_histogram_for_test(scores, incomplete, 2).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_core_parallel_chunks_match_the_scalar_control() {
        let rows = rows(4_097);
        let codebook = fit_codebook(&rows).unwrap();
        let codes = rows
            .iter()
            .map(|row| codebook.encode(row).unwrap())
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        let query = rows[4_096];

        let scalar = rank_candidates_scalar(&codebook, &blocks, rows.len(), &query, 2_048).unwrap();
        let parallel =
            rank_candidates_parallel_scalar_for_test(&codebook, &blocks, rows.len(), &query, 2_048)
                .unwrap();
        assert_eq!(parallel, scalar);
    }

    #[test]
    fn v26_release_contract_pq4_core_3072_depth_matches_the_scalar_control() {
        // Break caught: the evidence-qualified 3,072-row depth is rejected by a stale
        // diagnostic allowlist or changes deterministic score/source ordering.
        let rows = rows(4_097);
        let codebook = fit_codebook(&rows).unwrap();
        let codes = rows
            .iter()
            .map(|row| codebook.encode(row).unwrap())
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        let query = rows[2_113];

        let scalar = rank_candidates_scalar(&codebook, &blocks, rows.len(), &query, 3_072).unwrap();
        let parallel =
            rank_candidates_parallel_scalar_for_test(&codebook, &blocks, rows.len(), &query, 3_072)
                .unwrap();
        assert_eq!(parallel, scalar);
        assert_eq!(parallel.len(), 3_072);
    }

    #[test]
    fn v26_release_contract_pq4_snapshot_manifest_rejects_identity_and_layout_drift() {
        // Break caught: an incomplete or ambiguous manifest admits renamed, mutated, padded, or
        // differently packed cross-language arrays before their bytes are authenticated.
        let baseline = Pq4Manifest {
            schema: "borsuk-pq4-snapshot-v1".to_owned(),
            generation: "deep-image-96-pq4-generation-0001".to_owned(),
            source_uri: "s3://frozen/deep-image-96.parquet".to_owned(),
            source_sha256: "a".repeat(64),
            source_encoded_bytes: 38_400_000_000,
            row_count: 4_097,
            dimension: 96,
            subquantizer_count: 32,
            subspace_dimensions: 3,
            centroid_count: 16,
            lloyd_iterations: 4,
            block_rows: 32,
            block_count: 129,
            padding_rows: 31,
            code_bytes_per_row: 16,
            byte_order: "subquantizer-major".to_owned(),
            nibble_order: "even-low-odd-high".to_owned(),
            source_order: "ascending-source-ordinal".to_owned(),
            candidate_depth: 3_072,
            codebook: snapshot_identity(
                "codebook-arrow",
                "codebook.arrow",
                'b',
                8_192,
                1,
                "centroids:non-nullable-fixed-list-f32[1536]",
            ),
            codes: snapshot_identity(
                "codes-arrow",
                "codes.arrow",
                'c',
                24_576,
                129,
                "block_ordinal:u64,packed_codes:non-nullable-fixed-binary[512]",
            ),
            vectors: snapshot_identity(
                "vectors-arrow",
                "vectors.arrow",
                'd',
                400_000,
                4_097,
                "vector:non-nullable-fixed-list-f32[96]",
            ),
            ids: snapshot_identity(
                "ids-arrow",
                "ids.arrow",
                'e',
                20_000,
                4_097,
                "id:non-nullable-binary",
            ),
        };

        let bytes = canonical_manifest_bytes(&baseline).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
        assert!(
            !bytes
                .iter()
                .any(|byte| byte.is_ascii_whitespace() && *byte != b'\n')
        );
        assert!(bytes.starts_with(b"{\"block_count\":129,\"block_rows\":32,"));
        let nested_prefix = b"\"codebook\":{\"encoded_bytes\":8192,\"file_name\"";
        assert!(
            bytes
                .windows(nested_prefix.len())
                .any(|window| window == nested_prefix)
        );

        type Mutation = Box<dyn Fn(&mut Pq4Manifest)>;
        let mutations: Vec<Mutation> = vec![
            Box::new(|value| value.schema = "borsuk-pq4-snapshot-v2".to_owned()),
            Box::new(|value| value.generation.clear()),
            Box::new(|value| value.source_sha256.replace_range(0..1, "A")),
            Box::new(|value| value.row_count = 4_096),
            Box::new(|value| value.block_count = 128),
            Box::new(|value| value.padding_rows = 30),
            Box::new(|value| value.candidate_depth = 2_048),
            Box::new(|value| value.nibble_order = "odd-low-even-high".to_owned()),
            Box::new(|value| value.codebook.role = "vectors-arrow".to_owned()),
            Box::new(|value| value.codes.file_name = "renamed.arrow".to_owned()),
            Box::new(|value| value.vectors.sha256.replace_range(0..1, "G")),
            Box::new(|value| value.ids.encoded_bytes = 0),
            Box::new(|value| value.ids.row_count = 4_096),
            Box::new(|value| value.ids.file_name = value.vectors.file_name.clone()),
        ];
        for mutate in mutations {
            let mut drifted = baseline.clone();
            mutate(&mut drifted);
            assert!(canonical_manifest_bytes(&drifted).is_err());
        }
    }

    #[test]
    fn v26_release_contract_pq4_snapshot_round_trip_authenticates_arrow_and_source_order() {
        // Break caught: a snapshot writer/open pair loses source order or admits mutated,
        // missing, or unregistered files before Arrow bytes are authenticated.
        let parent = tempfile::tempdir().unwrap();
        let directory = parent.path().join("snapshot");
        let vectors = rows(3_073);
        let codebook = fit_codebook(&vectors).unwrap();
        let codes = vectors
            .iter()
            .map(|vector| codebook.encode(vector).unwrap())
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        let source_sha256 = "a".repeat(64);
        let ids = (0..3_073)
            .map(|ordinal| vec![0, 255, ordinal as u8, (ordinal * 7) as u8])
            .collect::<Vec<_>>();
        let request = Pq4SnapshotWriteRequest {
            directory: &directory,
            generation: "deep-image-96-pq4-generation-0001",
            source_uri: "s3://frozen/deep-image-96.parquet",
            source_sha256: &source_sha256,
            source_encoded_bytes: 38_400_000_000,
            codebook: &codebook,
            blocks: &blocks,
            vectors: &vectors,
            ids: &ids,
        };
        let manifest = write_snapshot(&request).unwrap();
        assert_eq!(manifest.row_count, 3_073);
        assert_eq!(manifest.block_count, 97);
        assert_eq!(manifest.padding_rows, 31);
        assert_eq!(
            std::fs::read(directory.join("manifest.json")).unwrap(),
            canonical_manifest_bytes(&manifest).unwrap()
        );

        let snapshot = Pq4Snapshot::open(&directory).unwrap();
        assert_eq!(snapshot.row_count(), 3_073);
        assert_eq!(snapshot.blocks(), blocks.as_slice());
        assert_eq!(snapshot.codebook(), &codebook);
        assert_eq!(snapshot.read_vector(3_072).unwrap(), vectors[3_072]);
        assert_eq!(snapshot.read_id(3_072).unwrap(), ids[3_072]);
        assert!(snapshot.read_vector(3_073).is_err());
        assert!(snapshot.read_id(3_073).is_err());

        std::fs::write(directory.join("vectors.arrow"), b"mutated").unwrap();
        assert!(Pq4Snapshot::open(&directory).is_err());

        let second = parent.path().join("snapshot-extra");
        let mut second_request = request;
        second_request.directory = &second;
        write_snapshot(&second_request).unwrap();
        std::fs::write(second.join("unregistered"), b"x").unwrap();
        assert!(Pq4Snapshot::open(&second).is_err());
        std::fs::remove_file(second.join("unregistered")).unwrap();
        std::fs::remove_file(second.join("ids.arrow")).unwrap();
        assert!(Pq4Snapshot::open(&second).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_builder_is_bounded_parallel_and_byte_deterministic() {
        // Break caught: worker scheduling changes shard bytes/source order, construction retains
        // the corpus instead of bounded batches, or malformed input leaves an openable output.
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input.parquet");
        let vectors = rows(4_097);
        let ids = (0..vectors.len())
            .map(|ordinal| format!("opaque-{ordinal:08}").into_bytes())
            .collect::<Vec<_>>();
        write_pq4_input(&input, &vectors, &ids);

        let build = |worker_count, output: &std::path::Path| {
            Pq4Builder::build_parquet(
                &input,
                output,
                &Pq4BuildConfig {
                    worker_count,
                    batch_rows: 512,
                    generation: "deep-image-96-pq4-generation-0001".to_owned(),
                    source_uri: "s3://frozen/deep-image-96.parquet".to_owned(),
                },
            )
            .unwrap()
        };
        let first_dir = temp.path().join("one-worker");
        let second_dir = temp.path().join("four-workers");
        let first = build(1, &first_dir);
        let second = build(4, &second_dir);
        assert_eq!(first.row_count, 4_097);
        assert_eq!(first.sample_rows, 4_097);
        assert!(first.maximum_buffered_rows <= 9_216);
        assert_eq!(second.worker_count, 4);
        assert_eq!(first.manifest, second.manifest);
        for file_name in [
            "manifest.json",
            "codebook.arrow",
            "codes.arrow",
            "vectors.arrow",
            "ids.arrow",
        ] {
            assert_eq!(
                std::fs::read(first_dir.join(file_name)).unwrap(),
                std::fs::read(second_dir.join(file_name)).unwrap(),
                "{file_name} differs across worker counts"
            );
        }
        let snapshot = Pq4Snapshot::open(&second_dir).unwrap();
        let norm = vectors[4_096]
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt();
        let expected_vector = vectors[4_096].map(|value| value / norm);
        assert_eq!(snapshot.read_vector(4_096).unwrap(), expected_vector);
        assert_eq!(snapshot.read_id(4_096).unwrap(), ids[4_096]);

        let invalid_input = temp.path().join("invalid.parquet");
        let mut invalid_vectors = vectors;
        invalid_vectors[2_048][17] = f32::NAN;
        write_pq4_input(&invalid_input, &invalid_vectors, &ids);
        let rejected = temp.path().join("rejected");
        assert!(
            Pq4Builder::build_parquet(
                &invalid_input,
                &rejected,
                &Pq4BuildConfig {
                    worker_count: 2,
                    batch_rows: 512,
                    generation: "deep-image-96-pq4-generation-0001".to_owned(),
                    source_uri: "s3://frozen/deep-image-96.parquet".to_owned(),
                },
            )
            .is_err()
        );
        assert!(!rejected.exists());
    }

    #[test]
    fn v26_release_contract_pq4_builder_normalizes_every_persisted_vector() {
        // Break caught: ordinary finite Parquet vectors are encoded and reranked at arbitrary
        // input magnitudes even though the evidence-qualified PQ4 geometry is unit normalized.
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("scaled.parquet");
        let mut vectors = rows(3_072);
        for (ordinal, vector) in vectors.iter_mut().enumerate() {
            let scale = (ordinal % 7 + 2) as f32;
            for value in vector {
                *value *= scale;
            }
        }
        let ids = (0..vectors.len())
            .map(|ordinal| ordinal.to_le_bytes().to_vec())
            .collect::<Vec<_>>();
        write_pq4_input(&input, &vectors, &ids);
        let output = temp.path().join("normalized");
        Pq4Builder::build_parquet(
            &input,
            &output,
            &Pq4BuildConfig {
                worker_count: 2,
                batch_rows: 512,
                generation: "normalization-contract".to_owned(),
                source_uri: "s3://frozen/scaled.parquet".to_owned(),
            },
        )
        .unwrap();
        let snapshot = Pq4Snapshot::open(&output).unwrap();
        let actual = snapshot.read_vector(1_537).unwrap();
        let norm = vectors[1_537]
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt();
        let expected = vectors[1_537].map(|value| value / norm);
        assert!(
            actual
                .iter()
                .zip(expected)
                .all(|(left, right)| (left - right).abs() <= 1.0e-6)
        );
        assert!((actual.iter().map(|value| value * value).sum::<f32>() - 1.0).abs() <= 1.0e-6);
    }

    #[test]
    fn v26_release_contract_pq4_search_returns_exact_rows_with_bounded_admission() {
        // Break caught: serving returns quantized/page candidates instead of exact rows, loses
        // deterministic tie order, skips query normalization, or oversubscribes query scratch.
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input.parquet");
        let vectors = rows(4_097);
        let ids = (0..vectors.len())
            .map(|ordinal| format!("id-{ordinal:08}").into_bytes())
            .collect::<Vec<_>>();
        write_pq4_input(&input, &vectors, &ids);
        let output = temp.path().join("snapshot");
        Pq4Builder::build_parquet(
            &input,
            &output,
            &Pq4BuildConfig {
                worker_count: 2,
                batch_rows: 512,
                generation: "search-contract".to_owned(),
                source_uri: "s3://frozen/search.parquet".to_owned(),
            },
        )
        .unwrap();
        let options = Pq4OpenOptions {
            shard_ordinal: 7,
            memory_budget_bytes: 64 * 1024 * 1024,
            query_threads: 2,
            admission_timeout_ms: 1_000,
        };
        let index = Arc::new(Pq4Index::open(&output, options.clone()).unwrap());
        let query = vectors[2_113].map(|value| value * 9.0);
        let matches = index.search(&query, 10).unwrap();
        let rerank_workers = Mutex::new(BTreeSet::new());
        let observed = search_with_exact_rerank_observer_for_test(&index, &query, 10, || {
            rerank_workers
                .lock()
                .unwrap()
                .insert(std::thread::current().name().unwrap().to_owned());
            std::thread::sleep(Duration::from_micros(50));
        })
        .unwrap();
        assert_eq!(observed, matches);
        assert!(
            rerank_workers.into_inner().unwrap().len() >= 2,
            "exact rerank silently became serial"
        );
        let expected_ordinals = (0..10).map(|index| 57 + index * 257).collect::<Vec<_>>();
        assert_eq!(
            matches
                .iter()
                .map(|item| item.source_ordinal)
                .collect::<Vec<_>>(),
            expected_ordinals
        );
        assert!(matches.iter().all(|item| item.squared_distance <= 1.0e-12));
        assert_eq!(matches[3].id, ids[expected_ordinals[3] as usize]);
        assert!(matches.iter().all(|item| item.shard_ordinal == 7));

        let handles = (0..4)
            .map(|_| {
                let index = index.clone();
                std::thread::spawn(move || index.search(&query, 10).unwrap())
            })
            .collect::<Vec<_>>();
        for handle in handles {
            assert_eq!(handle.join().unwrap(), matches);
        }
        let too_small = Pq4OpenOptions {
            memory_budget_bytes: 1_024,
            ..options
        };
        assert!(Pq4Index::open(&output, too_small).is_err());
    }

    #[test]
    fn v26_release_contract_pq4_shards_merge_is_bounded_and_permutation_invariant() {
        // Break caught: global merging loses shard identity, depends on arrival order, admits a
        // duplicate shard, or fails to retain the true top-k from exact local top-k lists.
        let item = |shard_ordinal, source_ordinal, squared_distance| Pq4Match {
            id: format!("{shard_ordinal}-{source_ordinal}").into_bytes(),
            squared_distance,
            source_ordinal,
            shard_ordinal,
        };
        let shard_two = vec![item(2, 0, 0.25), item(2, 1, 0.5), item(2, 2, 0.75)];
        let shard_five = vec![item(5, 0, 0.25), item(5, 1, 0.4), item(5, 2, 0.9)];
        let expected = vec![
            item(2, 0, 0.25),
            item(5, 0, 0.25),
            item(5, 1, 0.4),
            item(2, 1, 0.5),
        ];
        assert_eq!(
            merge_pq4_shard_matches(vec![shard_two.clone(), shard_five.clone()], 4).unwrap(),
            expected
        );
        assert_eq!(
            merge_pq4_shard_matches(vec![shard_five.clone(), shard_two.clone()], 4).unwrap(),
            expected
        );
        assert!(merge_pq4_shard_matches(vec![], 4).is_err());
        assert!(merge_pq4_shard_matches(vec![shard_two.clone()], 0).is_err());
        assert!(merge_pq4_shard_matches(vec![shard_two.clone(), shard_two.clone()], 4).is_err());
        let mut unsorted = shard_two;
        unsorted.swap(0, 1);
        assert!(merge_pq4_shard_matches(vec![unsorted], 2).is_err());
    }

    #[cfg(not(target_arch = "aarch64"))]
    #[test]
    fn v26_release_contract_pq4_core_production_scan_rejects_unqualified_backend() {
        let rows = rows(512);
        let codebook = fit_codebook(&rows).unwrap();
        let codes = rows
            .iter()
            .map(|row| codebook.encode(row).unwrap())
            .collect::<Vec<_>>();
        let blocks = encode_blocks(&codes).unwrap();
        let error = rank_candidates(&codebook, &blocks, rows.len(), &rows[0], 512).unwrap_err();
        assert!(error.to_string().contains("AArch64 NEON"));
    }
}
